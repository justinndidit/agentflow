//go:build integration

package engine_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
)

func newRetention(pool *pgxpool.Pool, days int, opts ...engine.RetentionOption) *engine.Retention {
	return engine.NewRetention(
		repositories.NewTxManager(pool, nopLogger()),
		&config.Retention{Enabled: true, MaxAgeDays: days},
		nopLogger(), opts...)
}

// seedFinishedWorkflow creates a workflow in a terminal state, with tasks and
// results, aged by backdating updated_at.
func seedFinishedWorkflow(t *testing.T, pool *pgxpool.Pool, status string, ageDays int) uuid.UUID {
	t.Helper()

	ctx := context.Background()
	dbtest.SeedAgents(t, pool, "research-agent")
	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))

	row := dbtest.NewWorkflowRow()
	row.TaskCount = 2
	workflow, err := stores.WorkflowStore.CreateWorkflow(ctx, row)
	if err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	tasks := []*models.TaskRow{
		dbtest.NewTaskRow(workflow.ID, "a", "research-agent"),
		dbtest.NewTaskRow(workflow.ID, "b", "research-agent"),
	}
	if err := stores.TaskStore.BulkInsertTask(ctx, tasks); err != nil {
		t.Fatalf("failed to seed tasks: %v", err)
	}
	for _, task := range tasks {
		err := stores.TaskResultStore.Insert(ctx, &models.TaskResult{
			TaskID: task.ID, Attempt: 1, Output: []byte(`{}`), CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("failed to seed a result: %v", err)
		}
	}

	_, err = pool.Exec(ctx,
		`UPDATE workflows SET status = $2::workflow_status,
		        updated_at = now() - ($3 || ' days')::interval
		  WHERE id = $1`,
		workflow.ID, status, strconv.Itoa(ageDays))
	if err != nil {
		t.Fatalf("failed to age the workflow: %v", err)
	}
	return workflow.ID
}

func workflowExists(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) bool {
	t.Helper()

	var exists bool
	err := pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM workflows WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		t.Fatalf("failed to check for the workflow: %v", err)
	}
	return exists
}

// Deleting the workflow is enough: tasks cascade from it, and task_results
// cascade from tasks. If that chain ever breaks, rows are stranded with no
// workflow to find them by.
func TestRetention_DeletesExpiredWorkflowsAndTheirRows(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	expired := seedFinishedWorkflow(t, pool, "completed", 40)

	deleted, err := newRetention(pool, 30).SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted %d workflows, want 1", deleted)
	}
	if workflowExists(t, pool, expired) {
		t.Error("the expired workflow survived")
	}

	if count := countTable(t, pool, "tasks"); count != 0 {
		t.Errorf("tasks = %d, want them cascaded away", count)
	}
	if count := countTable(t, pool, "task_results"); count != 0 {
		t.Errorf("task_results = %d, want them cascaded away", count)
	}
}

// Every terminal state is eligible; a failed run is no more worth keeping
// forever than a successful one.
func TestRetention_DeletesEveryTerminalState(t *testing.T) {
	ctx := context.Background()

	for _, status := range []string{"completed", "failed", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			pool := dbtest.Pool(t)
			id := seedFinishedWorkflow(t, pool, status, 40)

			if _, err := newRetention(pool, 30).SweepOnce(ctx); err != nil {
				t.Fatalf("SweepOnce failed: %v", err)
			}
			if workflowExists(t, pool, id) {
				t.Errorf("a %s workflow survived retention", status)
			}
		})
	}
}

// Live work is never eligible, however old it looks. A workflow stuck for a
// week is a bug to investigate, not rows to reclaim.
func TestRetention_NeverDeletesUnfinishedWork(t *testing.T) {
	ctx := context.Background()

	for _, status := range []string{"pending", "running"} {
		t.Run(status, func(t *testing.T) {
			pool := dbtest.Pool(t)
			id := seedFinishedWorkflow(t, pool, status, 400)

			deleted, err := newRetention(pool, 30).SweepOnce(ctx)
			if err != nil {
				t.Fatalf("SweepOnce failed: %v", err)
			}
			if deleted != 0 {
				t.Errorf("deleted %d workflows, want none — %s is live work", deleted, status)
			}
			if !workflowExists(t, pool, id) {
				t.Errorf("a %s workflow was deleted", status)
			}
		})
	}
}

// Inside the window, nothing goes.
func TestRetention_KeepsRecentWorkflows(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	recent := seedFinishedWorkflow(t, pool, "completed", 5)

	deleted, err := newRetention(pool, 30).SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce failed: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted %d workflows inside the retention window", deleted)
	}
	if !workflowExists(t, pool, recent) {
		t.Error("a workflow inside the window was deleted")
	}
}

// Age is measured from when a workflow finished, not when it started: one that
// ran for a long time should still get its full window afterwards.
func TestRetention_AgesFromCompletionNotCreation(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	id := seedFinishedWorkflow(t, pool, "completed", 1)
	// Created long ago, finished yesterday.
	if _, err := pool.Exec(ctx,
		`UPDATE workflows SET created_at = now() - interval '300 days' WHERE id = $1`, id); err != nil {
		t.Fatalf("failed to backdate creation: %v", err)
	}

	deleted, err := newRetention(pool, 30).SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce failed: %v", err)
	}
	if deleted != 0 {
		t.Error("a workflow that finished yesterday was deleted for having started long ago")
	}
	if !workflowExists(t, pool, id) {
		t.Error("the workflow was deleted")
	}
}

// A backlog is worked through over several passes rather than one statement
// locking a wide slice of the table.
func TestRetention_BatchBoundsOnePass(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	for range 7 {
		seedFinishedWorkflow(t, pool, "completed", 40)
	}

	retention := newRetention(pool, 30, engine.WithRetentionBatch(3))

	first, err := retention.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce failed: %v", err)
	}
	if first != 3 {
		t.Errorf("first pass deleted %d, want the batch size of 3", first)
	}

	total := first
	for range 10 {
		count, err := retention.SweepOnce(ctx)
		if err != nil {
			t.Fatalf("SweepOnce failed: %v", err)
		}
		total += count
		if count == 0 {
			break
		}
	}
	if total != 7 {
		t.Errorf("deleted %d in total, want 7", total)
	}
}

// A zero window means keep everything, which is what a deployment that wants
// its full history should get rather than losing it all.
func TestRetention_ZeroMaxAgeKeepsEverything(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	id := seedFinishedWorkflow(t, pool, "completed", 4000)

	deleted, err := newRetention(pool, 0).SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce failed: %v", err)
	}
	if deleted != 0 {
		t.Error("a zero retention window deleted workflows")
	}
	if !workflowExists(t, pool, id) {
		t.Error("a zero retention window deleted a workflow")
	}
}

// Run sweeps on a ticker until cancelled.
func TestRetention_RunSweepsUntilCancelled(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	id := seedFinishedWorkflow(t, pool, "completed", 40)
	retention := newRetention(pool, 30, engine.WithRetentionInterval(50*time.Millisecond))

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- retention.Run(runCtx) }()

	deadline := time.After(10 * time.Second)
	for workflowExists(t, pool, id) {
		select {
		case <-deadline:
			cancel()
			t.Fatal("retention did not delete the expired workflow")
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// A retained workflow's state is untouched — retention only removes, never
// rewrites.
func TestRetention_LeavesSurvivorsIntact(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	expired := seedFinishedWorkflow(t, pool, "completed", 40)
	kept := seedFinishedWorkflow(t, pool, "completed", 2)

	if _, err := newRetention(pool, 30).SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce failed: %v", err)
	}

	if workflowExists(t, pool, expired) {
		t.Error("the expired workflow survived")
	}
	if !workflowExists(t, pool, kept) {
		t.Fatal("the recent workflow was deleted")
	}

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	survivor, err := stores.WorkflowStore.GetWorkflowByID(ctx, kept)
	if err != nil {
		t.Fatalf("GetWorkflowByID failed: %v", err)
	}
	if survivor.Status != string(state.CompletedWorkflowStatus) {
		t.Errorf("survivor status = %q, want it untouched", survivor.Status)
	}

	tasks, err := stores.TaskStore.ListTasksByWorkflow(ctx, kept)
	if err != nil {
		t.Fatalf("ListTasksByWorkflow failed: %v", err)
	}
	if len(tasks) != 2 {
		t.Errorf("survivor has %d tasks, want 2", len(tasks))
	}
}
