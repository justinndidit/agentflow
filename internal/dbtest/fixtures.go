//go:build integration

package dbtest

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/state"
)

// Row builders shared by every integration test. They live here rather than in
// one package's test files so the repository tests and the engine tests agree
// on what a valid row looks like — a fixture that drifts between packages hides
// exactly the constraint violations these tests exist to catch.

// NewWorkflowRow returns a workflow satisfying every NOT NULL column, so a test
// can override only the field it cares about.
func NewWorkflowRow() *models.WorkflowRow {
	return &models.WorkflowRow{
		BaseModel:         models.NewBaseModel(),
		WorkflowName:      "test-workflow",
		WorkflowNameSpace: "default",
		Manifest:          []byte("name: test-workflow\n"),
		Version:           1,
		Status:            string(state.PendingWorkflowStatus),
		TaskCount:         1,
		MaxParallelism:    4,
		MaxTokensPerRun:   1000,
		DefaultTimeout:    2 * time.Minute,
	}
}

// NewTaskRow returns a task attached to workflowID that is immediately
// claimable: pending, no outstanding dependencies, and due now.
//
// agentName must already exist in agents — tasks.agent_name is a foreign key.
func NewTaskRow(workflowID uuid.UUID, taskKey, agentName string) *models.TaskRow {
	timeout := pgtype.Interval{Microseconds: 300 * 1_000_000, Valid: true}

	return &models.TaskRow{
		BaseModel:     models.NewBaseModel(),
		WorkflowID:    workflowID,
		TaskKey:       taskKey,
		AgentName:     agentName,
		Status:        string(state.PendingTaskStatus),
		DependsOn:     []string{},
		RemainingDeps: 0,
		InputTemplate: []byte(`{"role":"engineer"}`),
		Priority:      int8(state.HighTaskPriority),
		MaxRetries:    3,
		Timeout:       &timeout,
	}
}
