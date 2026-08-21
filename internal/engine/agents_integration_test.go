//go:build integration

package engine_test

import (
	"context"
	"testing"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

func TestCachedAgentImages_ResolvesAndCaches(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	images := engine.NewCachedAgentImages(stores.AgentStore)

	first, err := images.ImageFor(ctx, "research-agent")
	if err != nil {
		t.Fatalf("ImageFor failed: %v", err)
	}
	if first == "" {
		t.Fatal("ImageFor returned an empty image")
	}

	// Re-point the agent behind the cache's back. The old value surviving is
	// the documented trade: an image changing underneath a running workflow
	// would make its tasks non-reproducible.
	_, err = pool.Exec(ctx, `UPDATE agents SET agent_image = 'changed:latest' WHERE name = $1`,
		"research-agent")
	if err != nil {
		t.Fatalf("failed to re-point the agent: %v", err)
	}

	second, err := images.ImageFor(ctx, "research-agent")
	if err != nil {
		t.Fatalf("ImageFor failed: %v", err)
	}
	if second != first {
		t.Errorf("ImageFor = %q then %q; the lookup is not cached", first, second)
	}

	// A fresh cache does see the change, which is what a node restart gives.
	fresh := engine.NewCachedAgentImages(stores.AgentStore)
	updated, err := fresh.ImageFor(ctx, "research-agent")
	if err != nil {
		t.Fatalf("ImageFor failed: %v", err)
	}
	if updated != "changed:latest" {
		t.Errorf("a fresh cache returned %q, want the updated image", updated)
	}
}

func TestCachedAgentImages_UnknownAgent(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	images := engine.NewCachedAgentImages(stores.AgentStore)

	if _, err := images.ImageFor(ctx, "never-registered"); err == nil {
		t.Fatal("expected an unregistered agent to fail")
	}
}

// An agent registered with no image cannot be run, and saying so is better than
// handing the runtime an empty reference.
func TestCachedAgentImages_EmptyImage(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	_, err := pool.Exec(ctx,
		`INSERT INTO agents (id, name, agent_image) VALUES (gen_random_uuid(), 'blank', '')`)
	if err != nil {
		t.Fatalf("failed to insert the agent: %v", err)
	}

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	images := engine.NewCachedAgentImages(stores.AgentStore)

	if _, err := images.ImageFor(ctx, "blank"); err == nil {
		t.Fatal("expected an agent with no image to fail")
	}
}
