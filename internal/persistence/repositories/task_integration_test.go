//go:build integration

package repositories_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

// BulkInsertTask writes through COPY, which matches taskInsertFields to values
// positionally. A reordering there shifts every later value one column across
// and only fails loudly when two adjacent types disagree — so every field gets a
// distinct, identifiable value and is checked individually on the way back.
func TestBulkInsertTask_RoundTripsEveryField(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	store := stores(pool).TaskStore

	notBefore := time.Now().UTC().Add(90 * time.Minute).Truncate(time.Microsecond)
	timeout := pgtype.Interval{Microseconds: 137 * 1_000_000, Valid: true}

	want := &models.TaskRow{
		BaseModel:     models.NewBaseModel(),
		WorkflowID:    workflow.ID,
		TaskKey:       "collect-market-data",
		AgentName:     "research-agent",
		Status:        "pending",
		DependsOn:     []string{"alpha", "bravo"},
		RemainingDeps: 2,
		InputTemplate: []byte(`{"roles": ["backend engineer"]}`),
		Priority:      4,
		NotBefore:     &notBefore,
		Attempt:       1,
		MaxRetries:    3,
		Timeout:       &timeout,
	}

	if err := store.BulkInsertTask(ctx, []*models.TaskRow{want}); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	got, err := store.GetTaskByID(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetTaskByID failed: %v", err)
	}

	if got.ID != want.ID {
		t.Errorf("ID = %s, want %s", got.ID, want.ID)
	}
	if got.WorkflowID != want.WorkflowID {
		t.Errorf("WorkflowID = %s, want %s", got.WorkflowID, want.WorkflowID)
	}
	if got.TaskKey != want.TaskKey {
		t.Errorf("TaskKey = %q, want %q", got.TaskKey, want.TaskKey)
	}
	if got.AgentName != want.AgentName {
		t.Errorf("AgentName = %q, want %q", got.AgentName, want.AgentName)
	}
	if got.Status != want.Status {
		t.Errorf("Status = %q, want %q", got.Status, want.Status)
	}
	if len(got.DependsOn) != 2 || got.DependsOn[0] != "alpha" || got.DependsOn[1] != "bravo" {
		t.Errorf("DependsOn = %v, want [alpha bravo]", got.DependsOn)
	}
	if got.RemainingDeps != want.RemainingDeps {
		t.Errorf("RemainingDeps = %d, want %d", got.RemainingDeps, want.RemainingDeps)
	}
	if string(got.InputTemplate) == "" {
		t.Error("InputTemplate came back empty")
	}
	if got.Priority != want.Priority {
		t.Errorf("Priority = %d, want %d", got.Priority, want.Priority)
	}
	if got.NotBefore == nil || !got.NotBefore.Equal(notBefore) {
		t.Errorf("NotBefore = %v, want %v", got.NotBefore, notBefore)
	}
	if got.Attempt != want.Attempt {
		t.Errorf("Attempt = %d, want %d", got.Attempt, want.Attempt)
	}
	if got.MaxRetries != want.MaxRetries {
		t.Errorf("MaxRetries = %d, want %d", got.MaxRetries, want.MaxRetries)
	}
	if got.Timeout == nil || got.Timeout.Microseconds != timeout.Microseconds {
		t.Errorf("Timeout = %v, want %v", got.Timeout, timeout)
	}

	// Columns the insert deliberately leaves out must take their schema defaults
	// rather than arriving as garbage from a shifted COPY.
	if got.EngineID != nil {
		t.Errorf("EngineID = %v, want nil", got.EngineID)
	}
	if got.LeaseEpoch != 0 {
		t.Errorf("LeaseEpoch = %d, want 0", got.LeaseEpoch)
	}
	if got.LeaseExpiry != nil {
		t.Errorf("LeaseExpiry = %v, want nil", got.LeaseExpiry)
	}
	if got.StartedAt != nil || got.FinishedAt != nil || got.ErrorMessage != nil {
		t.Errorf("expected started_at, finished_at and error_message to be null, got %v %v %v",
			got.StartedAt, got.FinishedAt, got.ErrorMessage)
	}
}

// depends_on and not_before are NOT NULL, and COPY has no expression layer to
// COALESCE in — listing a column and supplying nil writes NULL rather than
// falling back to the column default. Both substitutions happen in Go.
func TestBulkInsertTask_SubstitutesNotNullZeroValues(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	store := stores(pool).TaskStore

	task := newTaskRow(workflow.ID, "no-nils", "research-agent")
	task.DependsOn = nil
	task.NotBefore = nil

	if err := store.BulkInsertTask(ctx, []*models.TaskRow{task}); err != nil {
		t.Fatalf("BulkInsertTask failed with nil depends_on and not_before: %v", err)
	}

	got, err := store.GetTaskByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTaskByID failed: %v", err)
	}

	if got.DependsOn == nil || len(got.DependsOn) != 0 {
		t.Errorf("DependsOn = %v, want an empty array", got.DependsOn)
	}
	// not_before falls back to the row's created_at, so a task with no explicit
	// schedule is immediately eligible rather than never eligible.
	if got.NotBefore == nil {
		t.Fatal("NotBefore is null, want it defaulted to created_at")
	}
	// TIMESTAMPTZ holds microseconds, so the nanosecond tail of a Go timestamp
	// is truncated on the way in. Compare at the precision the column actually
	// stores rather than at the precision time.Now produces.
	if wantNotBefore := task.CreatedAt.Truncate(time.Microsecond); !got.NotBefore.Equal(wantNotBefore) {
		t.Errorf("NotBefore = %v, want created_at %v", got.NotBefore.UTC(), wantNotBefore)
	}
}

func TestBulkInsertTask_InsertsAll(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	store := stores(pool).TaskStore

	tasks := []*models.TaskRow{
		newTaskRow(workflow.ID, "a", "research-agent"),
		newTaskRow(workflow.ID, "b", "research-agent"),
		newTaskRow(workflow.ID, "c", "research-agent"),
	}
	if err := store.BulkInsertTask(ctx, tasks); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	listed, err := store.ListTasksByWorkflow(ctx, workflow.ID)
	if err != nil {
		t.Fatalf("ListTasksByWorkflow failed: %v", err)
	}
	if len(listed) != 3 {
		t.Errorf("listed %d tasks, want 3", len(listed))
	}
}

func TestBulkInsertTask_EmptySliceIsANoop(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	if err := stores(pool).TaskStore.BulkInsertTask(ctx, nil); err != nil {
		t.Fatalf("BulkInsertTask(nil) failed: %v", err)
	}
	if count := countRows(t, pool, "tasks"); count != 0 {
		t.Errorf("tasks = %d, want 0", count)
	}
}

// tasks.agent_name references agents(name), so a manifest naming an agent that
// was never registered has to be rejected rather than stored unrunnable.
func TestBulkInsertTask_RejectsUnknownAgent(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")

	task := newTaskRow(workflow.ID, "orphan", "never-registered-agent")
	err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{task})
	if err == nil {
		t.Fatal("expected a foreign key violation for an unregistered agent")
	}
	if count := countRows(t, pool, "tasks"); count != 0 {
		t.Errorf("tasks = %d after a rejected insert, want 0", count)
	}
}

// COPY is all-or-nothing: one bad row must not leave the good ones behind.
func TestBulkInsertTask_RejectsWholeBatchOnOneBadRow(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")

	tasks := []*models.TaskRow{
		newTaskRow(workflow.ID, "good-one", "research-agent"),
		newTaskRow(workflow.ID, "bad-one", "never-registered-agent"),
		newTaskRow(workflow.ID, "good-two", "research-agent"),
	}
	if err := stores(pool).TaskStore.BulkInsertTask(ctx, tasks); err == nil {
		t.Fatal("expected the batch to fail")
	}
	if count := countRows(t, pool, "tasks"); count != 0 {
		t.Errorf("tasks = %d, want the whole batch rolled back", count)
	}
}

// (workflow_id, task_key) is unique, which is what stops one submission from
// creating two rows the dispatcher would both consider runnable.
func TestBulkInsertTask_RejectsDuplicateTaskKeyInWorkflow(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")

	tasks := []*models.TaskRow{
		newTaskRow(workflow.ID, "same-key", "research-agent"),
		newTaskRow(workflow.ID, "same-key", "research-agent"),
	}
	if err := stores(pool).TaskStore.BulkInsertTask(ctx, tasks); err == nil {
		t.Fatal("expected a unique violation on (workflow_id, task_key)")
	}
}

// The same task_key in a different workflow is legal — each submission is an
// independent run of the same manifest.
func TestBulkInsertTask_AllowsSameTaskKeyAcrossWorkflows(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool)

	dbtest.SeedAgents(t, pool, "research-agent")
	first, err := store.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow())
	if err != nil {
		t.Fatalf("failed to create first workflow: %v", err)
	}
	second, err := store.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow())
	if err != nil {
		t.Fatalf("failed to create second workflow: %v", err)
	}

	tasks := []*models.TaskRow{
		newTaskRow(first.ID, "shared-key", "research-agent"),
		newTaskRow(second.ID, "shared-key", "research-agent"),
	}
	if err := store.TaskStore.BulkInsertTask(ctx, tasks); err != nil {
		t.Fatalf("the same task key in two workflows should be allowed, got: %v", err)
	}
}

// CHECK (remaining_deps >= 0) is the schema-level guard against an over-eager
// decrement in the committer, which does not exist yet. Pinned now so the
// constraint is known to work before anything relies on it.
func TestBulkInsertTask_RejectsNegativeRemainingDeps(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")

	task := newTaskRow(workflow.ID, "negative", "research-agent")
	task.RemainingDeps = -1

	if err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{task}); err == nil {
		t.Fatal("expected CHECK (remaining_deps >= 0) to reject a negative counter")
	}
}

// CHECK (attempt <= max_retries + 1) bounds retries in the schema rather than
// trusting the retry loop.
func TestBulkInsertTask_RejectsAttemptBeyondRetryBudget(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")

	task := newTaskRow(workflow.ID, "over-budget", "research-agent")
	task.MaxRetries = 2
	task.Attempt = 4

	if err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{task}); err == nil {
		t.Fatal("expected CHECK (attempt <= max_retries + 1) to reject the row")
	}
}

func TestGetTaskByID_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	_, err := stores(pool).TaskStore.GetTaskByID(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected an error for a missing task")
	}
	// Translated at the boundary so callers do not import pgx to recognise
	// absence.
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

func TestUpdateTask_WritesMutableFields(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	store := stores(pool).TaskStore

	task := newTaskRow(workflow.ID, "claimable", "research-agent")
	if err := store.BulkInsertTask(ctx, []*models.TaskRow{task}); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	started := time.Now().UTC().Truncate(time.Microsecond)
	expiry := started.Add(5 * time.Minute)
	message := "agent exited non-zero"

	task.Status = "failed"
	task.Attempt = 1
	task.LeaseEpoch = 7
	task.LeaseExpiry = &expiry
	task.StartedAt = &started
	task.FinishedAt = &started
	task.ErrorMessage = &message
	task.RemainingDeps = 0

	if err := store.UpdateTask(ctx, task); err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}

	got, err := store.GetTaskByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTaskByID failed: %v", err)
	}

	if got.Status != "failed" {
		t.Errorf("Status = %q, want failed", got.Status)
	}
	if got.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1", got.Attempt)
	}
	if got.LeaseEpoch != 7 {
		t.Errorf("LeaseEpoch = %d, want 7", got.LeaseEpoch)
	}
	if got.LeaseExpiry == nil || !got.LeaseExpiry.Equal(expiry) {
		t.Errorf("LeaseExpiry = %v, want %v", got.LeaseExpiry, expiry)
	}
	if got.ErrorMessage == nil || *got.ErrorMessage != message {
		t.Errorf("ErrorMessage = %v, want %q", got.ErrorMessage, message)
	}
}

// not_before is written as COALESCE(@notBefore, not_before) because the column
// is NOT NULL and an update that does not care about scheduling passes nil. The
// existing value has to survive that.
func TestUpdateTask_NilNotBeforeKeepsTheStoredValue(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	store := stores(pool).TaskStore

	scheduled := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Microsecond)
	task := newTaskRow(workflow.ID, "scheduled", "research-agent")
	task.NotBefore = &scheduled
	if err := store.BulkInsertTask(ctx, []*models.TaskRow{task}); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	task.NotBefore = nil
	task.Status = "running"
	if err := store.UpdateTask(ctx, task); err != nil {
		t.Fatalf("UpdateTask failed: %v", err)
	}

	got, err := store.GetTaskByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTaskByID failed: %v", err)
	}
	if got.NotBefore == nil || !got.NotBefore.Equal(scheduled) {
		t.Errorf("NotBefore = %v, want the stored %v to survive a nil update",
			got.NotBefore, scheduled)
	}
	if got.Status != "running" {
		t.Errorf("Status = %q, want running", got.Status)
	}
}

// depends_on is NOT NULL and has no COALESCE, so a nil slice is substituted in
// Go the same way the insert does it.
func TestUpdateTask_NilDependsOnBecomesEmptyArray(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	store := stores(pool).TaskStore

	task := newTaskRow(workflow.ID, "deps", "research-agent")
	task.DependsOn = []string{"upstream"}
	task.RemainingDeps = 1
	if err := store.BulkInsertTask(ctx, []*models.TaskRow{task}); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	task.DependsOn = nil
	task.RemainingDeps = 0
	if err := store.UpdateTask(ctx, task); err != nil {
		t.Fatalf("UpdateTask failed with a nil depends_on: %v", err)
	}

	got, err := store.GetTaskByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTaskByID failed: %v", err)
	}
	if got.DependsOn == nil || len(got.DependsOn) != 0 {
		t.Errorf("DependsOn = %v, want an empty array", got.DependsOn)
	}
}

func TestUpdateTask_MissingRowIsNotFound(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")

	task := newTaskRow(workflow.ID, "never-inserted", "research-agent")
	err := stores(pool).TaskStore.UpdateTask(ctx, task)
	if err == nil {
		t.Fatal("expected an error updating a row that does not exist")
	}
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

// The INTERVAL round trip has to survive a real column, not just the Go
// converters — pgtype encodes and Postgres re-normalises on the way back.
func TestTaskTimeout_RoundTripsThroughPostgres(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	store := stores(pool).TaskStore

	for i, seconds := range []int64{1, 30, 120, 300, 3600, 86_400} {
		timeout := pgtype.Interval{Microseconds: seconds * 1_000_000, Valid: true}
		task := newTaskRow(workflow.ID, "timeout-"+time.Duration(i).String(), "research-agent")
		task.Timeout = &timeout

		if err := store.BulkInsertTask(ctx, []*models.TaskRow{task}); err != nil {
			t.Fatalf("BulkInsertTask failed for %ds: %v", seconds, err)
		}
		got, err := store.GetTaskByID(ctx, task.ID)
		if err != nil {
			t.Fatalf("GetTaskByID failed for %ds: %v", seconds, err)
		}
		if got.Timeout == nil {
			t.Fatalf("Timeout for %ds came back null", seconds)
		}

		// Postgres normalises large intervals into days and hours, so the
		// microsecond field alone is not the whole value.
		total := got.Timeout.Microseconds +
			int64(got.Timeout.Days)*24*3600*1_000_000 +
			int64(got.Timeout.Months)*30*24*3600*1_000_000
		if total != seconds*1_000_000 {
			t.Errorf("timeout for %ds came back as %d microseconds (days=%d months=%d)",
				seconds, got.Timeout.Microseconds, got.Timeout.Days, got.Timeout.Months)
		}
	}
}

func TestListTasksByWorkflow_ScopesToTheWorkflow(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool)

	dbtest.SeedAgents(t, pool, "research-agent")
	first, err := store.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow())
	if err != nil {
		t.Fatalf("failed to create first workflow: %v", err)
	}
	second, err := store.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow())
	if err != nil {
		t.Fatalf("failed to create second workflow: %v", err)
	}

	err = store.TaskStore.BulkInsertTask(ctx, []*models.TaskRow{
		newTaskRow(first.ID, "a", "research-agent"),
		newTaskRow(first.ID, "b", "research-agent"),
		newTaskRow(second.ID, "c", "research-agent"),
	})
	if err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	listed, err := store.TaskStore.ListTasksByWorkflow(ctx, first.ID)
	if err != nil {
		t.Fatalf("ListTasksByWorkflow failed: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed %d tasks for the first workflow, want 2", len(listed))
	}
	for _, task := range listed {
		if task.WorkflowID != first.ID {
			t.Errorf("task %s belongs to workflow %s, want %s", task.TaskKey, task.WorkflowID, first.ID)
		}
	}
}

func TestListTasksByWorkflow_UnknownWorkflowIsEmpty(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	listed, err := stores(pool).TaskStore.ListTasksByWorkflow(ctx, uuid.New())
	if err != nil {
		t.Fatalf("ListTasksByWorkflow failed: %v", err)
	}
	if len(listed) != 0 {
		t.Errorf("listed %d tasks, want none", len(listed))
	}
}
