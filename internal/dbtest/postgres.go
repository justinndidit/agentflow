//go:build integration

// Package dbtest starts a throwaway Postgres for integration tests.
//
// The repository layer is almost entirely Postgres semantics — positional COPY,
// INTERVAL encoding, NOT NULL versus column defaults, foreign keys, enum casts —
// so a fake Repository would only assert that the code calls the methods it was
// written to call. These tests run against a real server instead.
package dbtest

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/persistence/database"
)

// The container image and database names mirror docker-compose.dev.yml so the
// tests exercise the same setup a developer runs. In particular the init script
// creates a database (appDatabase) that is NOT the container's POSTGRES_DB
// (containerDatabase) — the app connects to the former, and replicating that
// here is what keeps the split honest.
const (
	image             = "postgres:16-alpine"
	containerDatabase = "agentflow_db"
	appDatabase       = "agentflow"
	user              = "postgres"
	password          = "password"
)

// One container is shared by every test in a package run. Starting a server per
// test would dominate the runtime; isolation comes from Reset instead.
var (
	once      sync.Once
	shared    *instance
	startErr  error
	repoRootV string
	rootOnce  sync.Once
)

type instance struct {
	container *postgres.PostgresContainer
	dsn       string
}

// Pool returns a connection pool to a migrated, empty database.
//
// The schema is migrated once for the process; every table is truncated before
// the test body runs, so tests see a clean database without paying for a new
// container each time.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	inst := start(t)
	pool, err := pgxpool.New(context.Background(), inst.dsn)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)

	Reset(t, pool)
	return pool
}

// DSN is the connection string for the shared test database.
func DSN(t *testing.T) string {
	t.Helper()
	return start(t).dsn
}

// HostPort is the test database's address, for subprocesses that build their
// own configuration from the environment.
func HostPort(t *testing.T) (string, int) {
	t.Helper()
	return hostPort(t, start(t))
}

// Config returns an application config pointed at the test database, built
// through the real loader so tests exercise the same code path main does.
func Config(t *testing.T) *config.Config {
	t.Helper()

	inst := start(t)
	host, port := hostPort(t, inst)

	logger := zerolog.Nop()
	cfg, err := config.LoadConfigWithOptions(&logger, config.Options{
		// No env file: the values below are the whole configuration, so a
		// developer's .env cannot leak into a test run.
		EnvFile: filepath.Join(t.TempDir(), "absent.env"),
		Environ: func() []string {
			return []string{
				"AGENTFLOW__DATABASE__HOST=" + host,
				fmt.Sprintf("AGENTFLOW__DATABASE__PORT=%d", port),
				"AGENTFLOW__DATABASE__USER=" + user,
				"AGENTFLOW__DATABASE__PASSWORD=" + password,
				"AGENTFLOW__DATABASE__NAME=" + appDatabase,
				"AGENTFLOW__DATABASE__SSL_MODE=disable",
				"AGENTFLOW__MIGRATIONS__PATH=file://" + filepath.Join(repoRoot(t), "migrations"),
			}
		},
	})
	if err != nil {
		t.Fatalf("failed to build test config: %v", err)
	}
	return cfg
}

// Reset truncates every application table. RESTART IDENTITY and CASCADE are both
// set so a test does not have to know the foreign key order, and TRUNCATE rather
// than DELETE so this stays constant-time as tests accumulate rows.
func Reset(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx := context.Background()
	_, err := pool.Exec(ctx,
		`TRUNCATE task_results, tasks, workflows, agents, engines RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("failed to reset test database: %v", err)
	}
}

// SeedAgents inserts the agents a manifest refers to. tasks.agent_name is a
// foreign key to agents(name), so any test that writes tasks has to call this
// first with the agent names its manifest uses.
func SeedAgents(t *testing.T, pool *pgxpool.Pool, names ...string) {
	t.Helper()

	ctx := context.Background()
	for _, name := range names {
		_, err := pool.Exec(ctx,
			`INSERT INTO agents (id, name, agent_image, created_at, updated_at)
			 VALUES (gen_random_uuid(), $1, $2, now(), now())
			 ON CONFLICT (name) DO NOTHING`,
			name, name+":test")
		if err != nil {
			t.Fatalf("failed to seed agent %s: %v", name, err)
		}
	}
}

func start(t *testing.T) *instance {
	t.Helper()

	once.Do(func() {
		shared, startErr = launch(t)
	})
	if startErr != nil {
		t.Fatalf("failed to start test database: %v", startErr)
	}
	return shared
}

func launch(t *testing.T) (*instance, error) {
	// Not t's context: the container outlives the test that happened to start
	// it and is torn down by Ryuk when the run ends.
	ctx := context.Background()

	container, err := postgres.Run(ctx, image,
		postgres.WithDatabase(containerDatabase),
		postgres.WithUsername(user),
		postgres.WithPassword(password),
		// The same script docker-compose.dev.yml mounts, so a change that breaks
		// a fresh developer database breaks the tests too.
		postgres.WithInitScripts(filepath.Join(repoRoot(t), "scripts", "init_db.sql")),
		testcontainers.WithWaitStrategy(
			// Postgres restarts once during init, so the log line alone is not
			// enough — the readiness probe has to see it settle.
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("run postgres container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("container host: %w", err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return nil, fmt.Errorf("container port: %w", err)
	}

	// Connect to the database the init script created, not the container's
	// POSTGRES_DB — that is the one carrying the extensions.
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, password, host, port.Port(), appDatabase)

	inst := &instance{container: container, dsn: dsn}
	if err := migrate(ctx, inst); err != nil {
		return nil, err
	}
	return inst, nil
}

// migrate applies the schema once, through the real Migrator so the migration
// path itself is covered rather than bypassed with a raw schema dump.
func migrate(ctx context.Context, inst *instance) error {
	pool, err := pgxpool.New(ctx, inst.dsn)
	if err != nil {
		return fmt.Errorf("connect for migration: %w", err)
	}
	defer pool.Close()

	logger := zerolog.Nop()
	migrator, err := database.NewMigrator(
		&config.Migrations{MigrationsPath: "file://" + migrationsDir()},
		pool, &logger)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	if err := migrator.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate test database: %w", err)
	}
	return nil
}

func hostPort(t *testing.T, inst *instance) (string, int) {
	t.Helper()

	ctx := context.Background()
	host, err := inst.container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to read container host: %v", err)
	}
	port, err := inst.container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("failed to read container port: %v", err)
	}
	number, err := strconv.Atoi(port.Port())
	if err != nil {
		t.Fatalf("failed to parse container port %q: %v", port.Port(), err)
	}
	return host, number
}

// RepoRoot is the module root, for tests that need to reach repository files —
// building the binary, or pointing a subprocess at the migrations directory.
func RepoRoot() string { return root() }

func migrationsDir() string {
	return filepath.Join(root(), "migrations")
}

// repoRoot is the module root. Tests run with their package directory as the
// working directory, so paths to repo files have to be resolved rather than
// written relative.
func repoRoot(t *testing.T) string {
	t.Helper()
	return root()
}

func root() string {
	rootOnce.Do(func() {
		// This file is at <root>/internal/dbtest/postgres.go.
		_, file, _, ok := runtime.Caller(0)
		if !ok {
			panic("dbtest: cannot determine repository root")
		}
		repoRootV = filepath.Dir(filepath.Dir(filepath.Dir(file)))
	})
	return repoRootV
}
