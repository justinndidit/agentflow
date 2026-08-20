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

	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/database"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
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
		"how long the placeholder echo runtime takes per task; the Docker runtime is not implemented yet")
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

	// Every task is executed by the echo runtime, which runs no container. The
	// scheduling loops are proven against a fake worker before containers are
	// introduced, so a scheduling bug and a container bug cannot be confused.
	node := engine.NewNode(cfg, db.Pool, runtime.NewEcho(*echoDelay), logger)

	if err := node.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
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
