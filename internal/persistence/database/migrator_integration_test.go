//go:build integration

package database_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/database"
)

func nopLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

// Every node runs migrations on boot, so the second and subsequent runs are the
// common case. ErrNoChange has to be absorbed or a healthy node refuses to
// start.
func TestMigrate_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	cfg := dbtest.Config(t)

	for attempt := 1; attempt <= 3; attempt++ {
		migrator, err := database.NewMigrator(cfg.Migrations, pool, nopLogger())
		if err != nil {
			t.Fatalf("attempt %d: NewMigrator failed: %v", attempt, err)
		}
		if err := migrator.Migrate(ctx); err != nil {
			t.Fatalf("attempt %d: Migrate failed on an already-migrated database: %v", attempt, err)
		}
	}
}

// Migrate checks out a dedicated connection for golang-migrate's advisory lock
// and has to hand it back. If it does not, the pool has one fewer connection
// after every boot and eventually blocks forever.
func TestMigrate_ReleasesItsConnection(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	cfg := dbtest.Config(t)

	for range 3 {
		migrator, err := database.NewMigrator(cfg.Migrations, pool, nopLogger())
		if err != nil {
			t.Fatalf("NewMigrator failed: %v", err)
		}
		if err := migrator.Migrate(ctx); err != nil {
			t.Fatalf("Migrate failed: %v", err)
		}
	}

	// A query on the same pool proves connections are still available rather
	// than all held by abandoned migration drivers.
	var one int
	if err := pool.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("pool is unusable after repeated migrations: %v", err)
	}

	if stat := pool.Stat(); stat.AcquiredConns() != 0 {
		t.Errorf("AcquiredConns = %d after migrating, want 0", stat.AcquiredConns())
	}
}

func TestNewMigrator_NilConfig(t *testing.T) {
	pool := dbtest.Pool(t)

	if _, err := database.NewMigrator(nil, pool, nopLogger()); err == nil {
		t.Fatal("expected NewMigrator to reject a nil config")
	}
}

// The migration creates the enum types, tables and indexes the repository layer
// assumes exist. Asserting on the objects rather than a version number catches a
// migration that ran but produced the wrong schema.
func TestMigrate_CreatesTheExpectedSchema(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	for _, table := range []string{"agents", "engines", "workflows", "tasks", "task_results"} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("failed to check for table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %s is missing", table)
		}
	}

	for _, enum := range []string{"task_status", "engine_status", "workflow_status"} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = $1)`, enum).Scan(&exists)
		if err != nil {
			t.Fatalf("failed to check for enum %s: %v", enum, err)
		}
		if !exists {
			t.Errorf("enum type %s is missing", enum)
		}
	}

	// idx_tasks_ready is the partial index the dispatcher's claim query will
	// depend on; without it the claim degrades to a sequential scan under load.
	for _, index := range []string{
		"idx_tasks_ready", "idx_tasks_depends_on", "idx_tasks_leases",
		"idx_tasks_scheduling", "idx_tasks_agent_name",
		"idx_workflows_name_namespace", "idx_engines_liveness",
	} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM pg_indexes
				WHERE schemaname = 'public' AND indexname = $1
			)`, index).Scan(&exists)
		if err != nil {
			t.Fatalf("failed to check for index %s: %v", index, err)
		}
		if !exists {
			t.Errorf("index %s is missing", index)
		}
	}
}

// The init script creates the extensions and the update_updated_at function on
// the application database. The migration does not create them, so a fresh
// developer database depends on the script having run.
func TestInitScript_CreatesExtensionsAndFunction(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	for _, extension := range []string{"uuid-ossp", "pg_trgm"} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)`, extension).Scan(&exists)
		if err != nil {
			t.Fatalf("failed to check for extension %s: %v", extension, err)
		}
		if !exists {
			t.Errorf("extension %s is missing", extension)
		}
	}

	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_updated_at')`).Scan(&exists)
	if err != nil {
		t.Fatalf("failed to check for update_updated_at: %v", err)
	}
	if !exists {
		t.Error("function update_updated_at is missing")
	}
}

// SeedDevAgents is documented as safe to repeat, which is what makes -seed
// harmless on an already-seeded database.
func TestSeedDevAgents_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	for attempt := 1; attempt <= 3; attempt++ {
		if err := database.SeedDevAgents(ctx, pool, nopLogger()); err != nil {
			t.Fatalf("attempt %d: SeedDevAgents failed: %v", attempt, err)
		}
	}

	var first int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM agents`).Scan(&first); err != nil {
		t.Fatalf("failed to count agents: %v", err)
	}
	if first == 0 {
		t.Fatal("expected the seed to have inserted agents")
	}

	if err := database.SeedDevAgents(ctx, pool, nopLogger()); err != nil {
		t.Fatalf("SeedDevAgents failed on a fourth run: %v", err)
	}

	var second int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM agents`).Scan(&second); err != nil {
		t.Fatalf("failed to count agents: %v", err)
	}
	if second != first {
		t.Errorf("agents = %d after re-seeding, want it to stay at %d", second, first)
	}
}
