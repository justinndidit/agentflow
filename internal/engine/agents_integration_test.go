//go:build integration

package engine_test

import (
	"context"
	"testing"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

func TestCachedAgents_ResolvesAndCaches(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	agents := engine.NewCachedAgents(stores.AgentStore)

	first, err := agents.Lookup(ctx, "research-agent")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if first.Image == "" {
		t.Fatal("Lookup returned an empty image")
	}

	// Re-point the agent behind the cache's back. The old value surviving is
	// the documented trade: an image changing underneath a running workflow
	// would make its tasks non-reproducible.
	_, err = pool.Exec(ctx, `UPDATE agents SET agent_image = 'changed:latest' WHERE name = $1`,
		"research-agent")
	if err != nil {
		t.Fatalf("failed to re-point the agent: %v", err)
	}

	second, err := agents.Lookup(ctx, "research-agent")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if second.Image != first.Image {
		t.Errorf("Lookup = %q then %q; the lookup is not cached", first.Image, second.Image)
	}

	// A fresh cache does see the change, which is what a node restart gives.
	fresh := engine.NewCachedAgents(stores.AgentStore)
	updated, err := fresh.Lookup(ctx, "research-agent")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if updated.Image != "changed:latest" {
		t.Errorf("a fresh cache returned %q, want the updated image", updated.Image)
	}
}

func TestCachedAgents_UnknownAgent(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	agents := engine.NewCachedAgents(stores.AgentStore)

	if _, err := agents.Lookup(ctx, "never-registered"); err == nil {
		t.Fatal("expected an unregistered agent to fail")
	}
}

// An agent registered with no image cannot be run, and saying so is better than
// handing the runtime an empty reference.
func TestCachedAgents_EmptyImage(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	_, err := pool.Exec(ctx,
		`INSERT INTO agents (id, name, agent_image) VALUES (gen_random_uuid(), 'blank', '')`)
	if err != nil {
		t.Fatalf("failed to insert the agent: %v", err)
	}

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	agents := engine.NewCachedAgents(stores.AgentStore)

	if _, err := agents.Lookup(ctx, "blank"); err == nil {
		t.Fatal("expected an agent with no image to fail")
	}
}
