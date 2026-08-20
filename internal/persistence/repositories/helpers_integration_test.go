//go:build integration

package repositories_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
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

// newWorkflowRow returns a workflow that satisfies every NOT NULL column, so a
// test can override the one field it cares about.
func newWorkflowRow() *models.WorkflowRow {
	return &models.WorkflowRow{
		BaseModel:         models.NewBaseModel(),
		WorkflowName:      "test-workflow",
		WorkflowNameSpace: "default",
		Manifest:          []byte("name: test-workflow\n"),
		Version:           1,
		Status:            "pending",
		TaskCount:         1,
		MaxParallelism:    4,
		MaxTokensPerRun:   1000,
		DefaultTimeout:    2 * time.Minute,
	}
}

// newTaskRow returns a task attached to workflowID. agentName must already
// exist in agents — tasks.agent_name is a foreign key.
func newTaskRow(workflowID uuid.UUID, taskKey, agentName string) *models.TaskRow {
	timeout := pgtype.Interval{Microseconds: 300 * 1_000_000, Valid: true}

	return &models.TaskRow{
		BaseModel:     models.NewBaseModel(),
		WorkflowID:    workflowID,
		TaskKey:       taskKey,
		AgentName:     agentName,
		Status:        "pending",
		DependsOn:     []string{},
		RemainingDeps: 0,
		InputTemplate: []byte(`{"role":"engineer"}`),
		Priority:      4,
		MaxRetries:    3,
		Timeout:       &timeout,
	}
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
