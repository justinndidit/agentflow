//go:build integration

package repositories_test

import (
	"context"
	"errors"
	"testing"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

func TestAgentStore_GetByName(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	agent, err := stores(pool).AgentStore.GetByName(ctx, "research-agent")
	if err != nil {
		t.Fatalf("GetByName failed: %v", err)
	}
	if agent.Name != "research-agent" {
		t.Errorf("Name = %q, want research-agent", agent.Name)
	}
	if agent.AgentImage == "" {
		t.Error("AgentImage is empty; the runtime has nothing to run")
	}
}

// tasks.agent_name is a foreign key, so a task reaching dispatch always has a
// row here. A miss means the agent was deleted between submit and dispatch,
// which is worth reporting rather than defaulting to something.
func TestAgentStore_GetByName_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	_, err := stores(pool).AgentStore.GetByName(ctx, "never-registered")
	if err == nil {
		t.Fatal("expected an error for an unregistered agent")
	}
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

func TestAgentStore_ListAgents(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "beta-agent", "alpha-agent")

	agents, err := stores(pool).AgentStore.ListAgents(ctx)
	if err != nil {
		t.Fatalf("ListAgents failed: %v", err)
	}
	if len(agents) != 2 {
		t.Fatalf("listed %d agents, want 2", len(agents))
	}
	// Sorted, so output is stable for anything that displays it.
	if agents[0].Name != "alpha-agent" || agents[1].Name != "beta-agent" {
		t.Errorf("agents = %s, %s; want them sorted by name",
			agents[0].Name, agents[1].Name)
	}
}

func TestAgentStore_ListAgents_Empty(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	agents, err := stores(pool).AgentStore.ListAgents(ctx)
	if err != nil {
		t.Fatalf("ListAgents failed: %v", err)
	}
	if len(agents) != 0 {
		t.Errorf("listed %d agents, want none", len(agents))
	}
}
