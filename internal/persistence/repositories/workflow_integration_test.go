//go:build integration

package repositories_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

func TestCreateWorkflow_RoundTripsEveryField(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).WorkflowStore

	want := newWorkflowRow()
	want.WorkflowName = "global-hiring-pipeline"
	want.WorkflowNameSpace = "recruiting"
	want.Manifest = []byte("name: global-hiring-pipeline\nnamespace: recruiting\n")
	want.Version = 3
	want.TaskCount = 7
	want.MaxParallelism = 8
	want.MaxTokensPerRun = 250_000
	want.DefaultTimeout = 2 * time.Minute

	created, err := store.CreateWorkflow(ctx, want)
	if err != nil {
		t.Fatalf("CreateWorkflow failed: %v", err)
	}

	// CreateWorkflow returns the inserted row via RETURNING, so what comes back
	// is what Postgres stored rather than the struct that went in.
	if created.ID != want.ID {
		t.Errorf("ID = %s, want %s", created.ID, want.ID)
	}
	if created.WorkflowName != want.WorkflowName {
		t.Errorf("WorkflowName = %q, want %q", created.WorkflowName, want.WorkflowName)
	}
	if created.WorkflowNameSpace != want.WorkflowNameSpace {
		t.Errorf("WorkflowNameSpace = %q, want %q", created.WorkflowNameSpace, want.WorkflowNameSpace)
	}
	if string(created.Manifest) != string(want.Manifest) {
		t.Errorf("Manifest = %q, want %q", created.Manifest, want.Manifest)
	}
	if created.Version != want.Version {
		t.Errorf("Version = %d, want %d", created.Version, want.Version)
	}
	if created.Status != "pending" {
		t.Errorf("Status = %q, want pending", created.Status)
	}
	if created.TaskCount != want.TaskCount {
		t.Errorf("TaskCount = %d, want %d", created.TaskCount, want.TaskCount)
	}
	if created.MaxParallelism != want.MaxParallelism {
		t.Errorf("MaxParallelism = %d, want %d", created.MaxParallelism, want.MaxParallelism)
	}
	if created.MaxTokensPerRun != want.MaxTokensPerRun {
		t.Errorf("MaxTokensPerRun = %d, want %d", created.MaxTokensPerRun, want.MaxTokensPerRun)
	}
	// The counters carry schema defaults rather than being written by the insert.
	if created.TaskCompleted != 0 || created.TaskFailed != 0 || created.TaskCancelled != 0 {
		t.Errorf("counters = %d/%d/%d, want all zero",
			created.TaskCompleted, created.TaskFailed, created.TaskCancelled)
	}
	if created.RunningCount != 0 {
		t.Errorf("RunningCount = %d, want 0", created.RunningCount)
	}
	if created.TokensUsed != 0 {
		t.Errorf("TokensUsed = %d, want 0", created.TokensUsed)
	}
}

// default_timeout is an INTERVAL column mapped to a Go time.Duration, which is
// the one field in this struct where the two type systems do not line up
// directly. A duration that came back as nanoseconds-read-as-microseconds would
// still be a valid duration, just silently wrong by three orders of magnitude.
func TestCreateWorkflow_DefaultTimeoutRoundTrips(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).WorkflowStore

	for _, timeout := range []time.Duration{
		time.Second,
		30 * time.Second,
		2 * time.Minute,
		time.Hour,
		24 * time.Hour,
	} {
		workflow := newWorkflowRow()
		workflow.DefaultTimeout = timeout

		created, err := store.CreateWorkflow(ctx, workflow)
		if err != nil {
			t.Fatalf("CreateWorkflow failed for %s: %v", timeout, err)
		}
		if created.DefaultTimeout != timeout {
			t.Errorf("DefaultTimeout returned %s, want %s", created.DefaultTimeout, timeout)
		}

		fetched, err := store.GetWorkflowByID(ctx, created.ID)
		if err != nil {
			t.Fatalf("GetWorkflowByID failed for %s: %v", timeout, err)
		}
		if fetched.DefaultTimeout != timeout {
			t.Errorf("DefaultTimeout read back as %s, want %s", fetched.DefaultTimeout, timeout)
		}
	}
}

func TestGetWorkflowByID(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).WorkflowStore

	created, err := store.CreateWorkflow(ctx, newWorkflowRow())
	if err != nil {
		t.Fatalf("CreateWorkflow failed: %v", err)
	}

	got, err := store.GetWorkflowByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetWorkflowByID failed: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("ID = %s, want %s", got.ID, created.ID)
	}
}

func TestGetWorkflowByID_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	_, err := stores(pool).WorkflowStore.GetWorkflowByID(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected an error for a missing workflow")
	}
	// Absence is translated at the repository boundary, the same as
	// GetTaskByID and DeleteWorkflow, so no caller has to import pgx to
	// recognise it.
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

// Name is not unique: submitting the same manifest twice is two independent
// runs. The lookup resolves that by recency rather than erroring.
func TestGetWorkflowByName_ReturnsMostRecent(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).WorkflowStore

	older := newWorkflowRow()
	older.WorkflowName = "repeated"
	older.CreatedAt = time.Now().UTC().Add(-time.Hour)
	if _, err := store.CreateWorkflow(ctx, older); err != nil {
		t.Fatalf("failed to create the older workflow: %v", err)
	}

	newer := newWorkflowRow()
	newer.WorkflowName = "repeated"
	created, err := store.CreateWorkflow(ctx, newer)
	if err != nil {
		t.Fatalf("failed to create the newer workflow: %v", err)
	}

	got, err := store.GetWorkflowByName(ctx, "repeated")
	if err != nil {
		t.Fatalf("GetWorkflowByName failed: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("got workflow %s, want the most recent %s", got.ID, created.ID)
	}
}

func TestGetWorkflowByName_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	_, err := stores(pool).WorkflowStore.GetWorkflowByName(ctx, "no-such-workflow")
	if err == nil {
		t.Fatal("expected an error for a missing workflow")
	}
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

// Deleting a workflow has to take its tasks with it, or the tasks table
// accumulates rows referencing a run that no longer exists.
func TestDeleteWorkflow_CascadesToTasks(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool)
	workflow := seedWorkflow(t, pool, "research-agent")

	err := store.TaskStore.BulkInsertTask(ctx, []*models.TaskRow{
		newTaskRow(workflow.ID, "a", "research-agent"),
		newTaskRow(workflow.ID, "b", "research-agent"),
	})
	if err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}
	if count := countRows(t, pool, "tasks"); count != 2 {
		t.Fatalf("tasks = %d before delete, want 2", count)
	}

	if err := store.WorkflowStore.DeleteWorkflow(ctx, workflow.ID); err != nil {
		t.Fatalf("DeleteWorkflow failed: %v", err)
	}

	if count := countRows(t, pool, "tasks"); count != 0 {
		t.Errorf("tasks = %d after deleting the workflow, want 0", count)
	}
	if count := countRows(t, pool, "workflows"); count != 0 {
		t.Errorf("workflows = %d after delete, want 0", count)
	}
}

func TestDeleteWorkflow_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	err := stores(pool).WorkflowStore.DeleteWorkflow(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected an error deleting a workflow that does not exist")
	}
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}
