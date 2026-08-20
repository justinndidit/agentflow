//go:build integration

package repositories_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

func nopLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

// stores builds the repository layer over the pool directly rather than inside a
// transaction, which is the path a caller outside WithTransaction takes.
func stores(pool *pgxpool.Pool) *repositories.Stores {
	return repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
}

func newWorkflowRow() *models.WorkflowRow { return dbtest.NewWorkflowRow() }

func newTaskRow(workflowID uuid.UUID, taskKey, agentName string) *models.TaskRow {
	return dbtest.NewTaskRow(workflowID, taskKey, agentName)
}

// seedWorkflow inserts a workflow and the agents its tasks will name, returning
// the persisted workflow.
func seedWorkflow(t *testing.T, pool *pgxpool.Pool, agents ...string) *models.WorkflowRow {
	t.Helper()

	dbtest.SeedAgents(t, pool, agents...)

	workflow, err := stores(pool).WorkflowStore.CreateWorkflow(context.Background(), newWorkflowRow())
	if err != nil {
		t.Fatalf("failed to seed workflow: %v", err)
	}
	return workflow
}

// countRows is used to assert that a failed write left nothing behind.
func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()

	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&count); err != nil {
		t.Fatalf("failed to count %s: %v", table, err)
	}
	return count
}
