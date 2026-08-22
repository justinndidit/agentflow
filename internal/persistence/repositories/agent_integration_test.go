//go:build integration

package repositories_test

import (
	"context"
	"errors"
	"testing"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
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

func TestAgentStore_UpsertRegistersAndRepoints(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).AgentStore

	row := models.NewAgentRow("research-agent", "agentflow/research:v1", nil)
	created, err := store.UpsertAgent(ctx, &row)
	if err != nil {
		t.Fatalf("UpsertAgent failed: %v", err)
	}
	if created.AgentImage != "agentflow/research:v1" {
		t.Errorf("AgentImage = %q", created.AgentImage)
	}
	// Null for an agent that runs its image as built.
	if len(created.AgentCommand) != 0 {
		t.Errorf("AgentCommand = %v, want empty", created.AgentCommand)
	}

	// Re-pointing is an upsert, not a delete and re-insert: the latter would
	// break the foreign key from every task naming the agent, including tasks
	// in workflows still running.
	updated := models.NewAgentRow("research-agent", "agentflow/research:v2",
		[]string{"python", "worker.py", "--role=research"})
	saved, err := store.UpsertAgent(ctx, &updated)
	if err != nil {
		t.Fatalf("re-pointing failed: %v", err)
	}
	if saved.AgentImage != "agentflow/research:v2" {
		t.Errorf("AgentImage = %q, want the new image", saved.AgentImage)
	}
	if len(saved.AgentCommand) != 3 || saved.AgentCommand[0] != "python" {
		t.Errorf("AgentCommand = %v, want the command stored", saved.AgentCommand)
	}
	// The identity survives, which is what keeps the foreign key intact.
	if saved.ID != created.ID {
		t.Errorf("re-pointing changed the agent's id from %s to %s", created.ID, saved.ID)
	}

	if count := countRows(t, pool, "agents"); count != 1 {
		t.Errorf("agents = %d, want the upsert to have replaced rather than added", count)
	}
}

func TestAgentStore_DeleteAgent(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "doomed-agent")
	store := stores(pool).AgentStore

	if err := store.DeleteAgent(ctx, "doomed-agent"); err != nil {
		t.Fatalf("DeleteAgent failed: %v", err)
	}
	if _, err := store.GetByName(ctx, "doomed-agent"); !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("the agent survived deletion: %v", err)
	}
}

func TestAgentStore_DeleteAgent_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	err := stores(pool).AgentStore.DeleteAgent(ctx, "never-registered")
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

// Removing an agent out from under an unfinished workflow would leave tasks
// that can never be dispatched, so the foreign key refuses it.
func TestAgentStore_DeleteAgent_RefusedWhileTasksReferenceIt(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")

	err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{
		newTaskRow(workflow.ID, "in-flight", "research-agent"),
	})
	if err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	if err := stores(pool).AgentStore.DeleteAgent(ctx, "research-agent"); err == nil {
		t.Fatal("expected the foreign key to refuse the delete")
	}
}
