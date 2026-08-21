// Command agentflow submits workflow manifests and runs engine nodes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/justinndidit/agentflow/internal/blob"
	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/database"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/justinndidit/agentflow/internal/telemetry"
	"github.com/rs/zerolog"
)

const usage = `agentflow — distributed execution engine for AI workers

Usage:
  agentflow submit [flags]   submit a workflow manifest
  agentflow engine [flags]   run an engine node until interrupted

Run "agentflow <command> -h" for the flags of a command.
`

func main() {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	command, args := os.Args[1], os.Args[2:]

	var err error
	switch command {
	case "submit":
		err = runSubmit(&logger, args)
	case "engine":
		err = runEngine(&logger, args)
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", command, usage)
		os.Exit(2)
	}

	if err != nil {
		logger.Error().Err(err).Str("command", command).Msg("command failed")
		os.Exit(1)
	}
}

// runSubmit parses a manifest and persists it as a runnable graph, then exits.
// It does not run anything: an engine node picks the work up.
func runSubmit(logger *zerolog.Logger, args []string) error {
	flags := flag.NewFlagSet("submit", flag.ExitOnError)
	manifestFile := flags.String("manifest", "example-workflow.yml", "workflow manifest location")
	seedDevData := flags.Bool("seed", false, "insert the development agents the example manifest refers to")
	if err := flags.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_, db, err := open(ctx, logger, *seedDevData)
	if err != nil {
		return err
	}
	defer closeDB(ctx, db, logger)

	txManager := repositories.NewTxManager(db.Pool, logger)
	processor := engine.NewManifestProcessor(logger, txManager)

	workflow, err := processor.SubmitManifest(ctx, *manifestFile)
	if err != nil {
		return fmt.Errorf("submit manifest: %w", err)
	}

	fmt.Printf("workflow %s submitted (%d tasks)\n", workflow.ID, workflow.TaskCount)
	return nil
}

// runEngine runs a node until interrupted.
func runEngine(logger *zerolog.Logger, args []string) error {
	flags := flag.NewFlagSet("engine", flag.ExitOnError)
	seedDevData := flags.Bool("seed", false, "insert the development agents the example manifest refers to")
	echoDelay := flags.Duration("echo-delay", time.Second,
		"how long the echo runtime takes per task; ignored by the docker runtime")
	if err := flags.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, db, err := open(ctx, logger, *seedDevData)
	if err != nil {
		return err
	}
	defer closeDB(ctx, db, logger)

	rt, closeRuntime, err := buildRuntime(cfg, logger, *echoDelay)
	if err != nil {
		return err
	}
	defer closeRuntime()

	logger.Info().Str("runtime", rt.Name()).Msg("engine runtime selected")

	// Before anything claims work, so the first attempt is traced like any
	// other. A collector that is down must never stop a node: Init reports only
	// configuration errors, and export failures are logged thereafter.
	shutdownTelemetry, err := telemetry.Init(ctx, telemetryConfig(cfg), logger)
	if err != nil {
		return err
	}
	defer flushTelemetry(ctx, shutdownTelemetry, logger)

	blobs, err := buildBlobStore(ctx, cfg, logger)
	if err != nil {
		return err
	}

	node := engine.NewNode(cfg, db.Pool, rt, blobs, logger)
	if err := node.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func telemetryConfig(cfg *config.Config) telemetry.Config {
	if cfg.Telemetry == nil {
		return telemetry.Config{}
	}
	return telemetry.Config{
		Enabled:     cfg.Telemetry.Enabled,
		Endpoint:    cfg.Telemetry.Endpoint,
		ServiceName: cfg.Telemetry.ServiceName,
		Insecure:    cfg.Telemetry.Insecure,
		SampleRatio: cfg.Telemetry.SampleRatio,
	}
}

// flushTelemetry drains buffered spans and metrics on the way out.
//
// Its own context: shutdown arrives with the caller's already cancelled, and a
// node that stops cleanly should not drop the telemetry describing what it did
// last — which is exactly the part anyone investigating will want.
func flushTelemetry(ctx context.Context, shutdown telemetry.Shutdown, logger *zerolog.Logger) {
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	if err := shutdown(flushCtx); err != nil {
		logger.Error().Err(err).Msg("error flushing telemetry")
	}
}

// buildBlobStore connects to artifact storage, or returns the no-op store when
// none is configured.
func buildBlobStore(ctx context.Context, cfg *config.Config, logger *zerolog.Logger) (blob.Store, error) {
	if cfg.Blob == nil || !cfg.Blob.Enabled {
		logger.Info().Msg("blob storage disabled; task output must fit inline")
		return blob.Disabled{}, nil
	}

	store, err := blob.NewS3Store(ctx, blob.S3Config{
		Endpoint:  cfg.Blob.Endpoint,
		AccessKey: cfg.Blob.AccessKey,
		SecretKey: cfg.Blob.SecretKey,
		Bucket:    cfg.Blob.Bucket,
		Region:    cfg.Blob.Region,
		UseSSL:    cfg.Blob.UseSSL,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("connect to blob storage: %w", err)
	}
	return store, nil
}

// buildRuntime constructs the configured executor, returning a cleanup for the
// ones that hold a connection.
func buildRuntime(
	cfg *config.Config,
	logger *zerolog.Logger,
	echoDelay time.Duration,
) (runtime.Runtime, func(), error) {
	switch cfg.Engine.Runtime {
	case "docker":
		docker, err := runtime.NewDocker(logger, runtime.WithLimits(runtime.Limits{
			MemoryBytes: cfg.Engine.TaskMemoryMB * 1024 * 1024,
			NanoCPUs:    int64(cfg.Engine.TaskCPUs * 1_000_000_000),
			PidsLimit:   runtime.DefaultLimits.PidsLimit,
			NetworkMode: cfg.Engine.TaskNetwork,
			StderrLimit: runtime.DefaultLimits.StderrLimit,
		}))
		if err != nil {
			return nil, nil, fmt.Errorf("start docker runtime: %w", err)
		}
		return docker, func() {
			if err := docker.Close(); err != nil {
				logger.Error().Err(err).Msg("error closing docker client")
			}
		}, nil

	case "echo":
		// Runs no container: the input comes back as the output. This is what
		// the scheduling tests exercise, and what a fresh checkout uses,
		// because the seeded agents point at placeholder images.
		return runtime.NewEcho(echoDelay), func() {}, nil

	default:
		return nil, nil, fmt.Errorf("unknown runtime %q", cfg.Engine.Runtime)
	}
}

// open loads configuration, connects, migrates, and optionally seeds.
func open(ctx context.Context, logger *zerolog.Logger, seed bool) (*config.Config, *database.PostgresDatabase, error) {
	cfg, err := config.LoadConfig(logger)
	if err != nil {
		return nil, nil, fmt.Errorf("load config: %w", err)
	}

	db := database.NewPostgresDatabase(cfg.Database, logger)
	if err := db.Open(ctx); err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}

	migrator, err := database.NewMigrator(cfg.Migrations, db.Pool, logger)
	if err != nil {
		closeDB(ctx, db, logger)
		return nil, nil, fmt.Errorf("create migrator: %w", err)
	}
	if err := migrator.Migrate(ctx); err != nil {
		closeDB(ctx, db, logger)
		return nil, nil, fmt.Errorf("migrate: %w", err)
	}

	// Must come after Migrate and before any manifest is submitted:
	// tasks.agent_name is a foreign key to agents(name), so submitting against
	// an unseeded database fails the whole bulk insert.
	if seed {
		if err := database.SeedDevAgents(ctx, db.Pool, logger); err != nil {
			closeDB(ctx, db, logger)
			return nil, nil, fmt.Errorf("seed development agents: %w", err)
		}
	}

	return cfg, db, nil
}

func closeDB(ctx context.Context, db *database.PostgresDatabase, logger *zerolog.Logger) {
	// Its own context: shutdown usually arrives with the caller's already
	// cancelled, which would abort the close.
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	if err := db.Close(closeCtx); err != nil {
		logger.Error().Err(err).Str("func", "closeDB").Msg("error closing database")
	}
}
