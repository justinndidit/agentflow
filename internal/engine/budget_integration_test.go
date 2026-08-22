//go:build integration

package engine_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
)

// budgetFixture seeds a workflow whose tasks each report a known token cost, so
// the ceiling can be crossed deterministically.
func budgetFixture(t *testing.T, taskCount int, maxTokens int64) *commitFixture {
	t.Helper()

	graph := map[string][]string{}
	for i := range taskCount {
		graph["task-"+strconv.Itoa(i)] = nil
	}
	f := newCommitFixture(t, graph, noBackoff)

	_, err := f.pool.Exec(context.Background(),
		`UPDATE workflows SET max_tokens = $2 WHERE id = $1`, f.workflow.ID, maxTokens)
	if err != nil {
		t.Fatalf("failed to set the budget: %v", err)
	}
	return f
}

// Crossing the ceiling stops everything that has not started. The overspend is
// bounded by what was in flight, which is what a soft ceiling means.
func TestBudget_CrossingCancelsWorkNotYetStarted(t *testing.T) {
	ctx := context.Background()
	f := budgetFixture(t, 5, 1000)

	// Claim one task at a time, so the rest stay pending and are cancellable.
	first, err := f.stores.TaskStore.ClaimTasks(ctx, f.engineID, 1, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}

	// One task spends the entire budget.
	err = f.committer.Commit(ctx, repositories.FenceFor(first[0]), engine.Outcome{
		TokensUsed: 1000,
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	var pending, cancelled int
	err = f.pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE status = 'pending'),
		        count(*) FILTER (WHERE status = 'cancelled') FROM tasks`).Scan(&pending, &cancelled)
	if err != nil {
		t.Fatalf("failed to count tasks: %v", err)
	}

	if pending != 0 {
		t.Errorf("pending = %d, want everything unstarted cancelled", pending)
	}
	if cancelled != 4 {
		t.Errorf("cancelled = %d, want the 4 tasks that had not started", cancelled)
	}

	workflow := f.reloadWorkflow(t)
	if workflow.TaskCancelled != 4 {
		t.Errorf("task_cancelled = %d, want 4", workflow.TaskCancelled)
	}
	if workflow.TokensUsed != 1000 {
		t.Errorf("tokens_used = %d, want 1000", workflow.TokensUsed)
	}
}

// The reason is recorded on the task, so someone reading a cancelled task can
// tell a budget stop from a failure cascade.
func TestBudget_CancelledTasksSayWhy(t *testing.T) {
	ctx := context.Background()
	f := budgetFixture(t, 3, 500)

	claimed, err := f.stores.TaskStore.ClaimTasks(ctx, f.engineID, 1, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	err = f.committer.Commit(ctx, repositories.FenceFor(claimed[0]), engine.Outcome{TokensUsed: 900})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	var message string
	err = f.pool.QueryRow(ctx,
		`SELECT error_message FROM tasks WHERE status = 'cancelled' LIMIT 1`).Scan(&message)
	if err != nil {
		t.Fatalf("failed to read the cancellation reason: %v", err)
	}

	if !strings.Contains(message, "token budget") {
		t.Errorf("error_message = %q, want it to name the budget", message)
	}
	if !strings.Contains(message, "900") || !strings.Contains(message, "500") {
		t.Errorf("error_message = %q, want it to report spend against the ceiling", message)
	}
}

// A task already running is left alone: its compute is paid for and its result
// is still worth recording. This is precisely why the ceiling is soft.
func TestBudget_RunningWorkIsLeftToFinish(t *testing.T) {
	ctx := context.Background()
	f := budgetFixture(t, 3, 1000)

	// Claim two: one to blow the budget, one already in flight.
	claimed, err := f.stores.TaskStore.ClaimTasks(ctx, f.engineID, 2, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d tasks, want 2", len(claimed))
	}

	err = f.committer.Commit(ctx, repositories.FenceFor(claimed[0]), engine.Outcome{TokensUsed: 1500})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// The second was running, not pending, so it survives the cancellation.
	survivor, err := f.stores.TaskStore.GetTaskByID(ctx, claimed[1].ID)
	if err != nil {
		t.Fatalf("GetTaskByID failed: %v", err)
	}
	if survivor.Status != string(state.RunningTaskStatus) {
		t.Errorf("in-flight task status = %q, want it left running", survivor.Status)
	}

	// And its result is still accepted — the overspend is recorded rather than
	// discarded.
	err = f.committer.Commit(ctx, repositories.FenceFor(claimed[1]), engine.Outcome{TokensUsed: 700})
	if err != nil {
		t.Fatalf("the in-flight task's commit was refused: %v", err)
	}
	if got := f.reloadWorkflow(t).TokensUsed; got != 2200 {
		t.Errorf("tokens_used = %d, want the overspend recorded honestly", got)
	}
}

// A workflow inside its budget is untouched.
func TestBudget_UnderBudgetRunsNormally(t *testing.T) {
	ctx := context.Background()
	f := budgetFixture(t, 3, 10_000)

	claimed, err := f.stores.TaskStore.ClaimTasks(ctx, f.engineID, 1, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	err = f.committer.Commit(ctx, repositories.FenceFor(claimed[0]), engine.Outcome{TokensUsed: 100})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	var cancelled int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE status = 'cancelled'`).Scan(&cancelled); err != nil {
		t.Fatalf("failed to count cancelled tasks: %v", err)
	}
	if cancelled != 0 {
		t.Errorf("cancelled = %d, want none under budget", cancelled)
	}
}

// No ceiling declared means unlimited, rather than zero.
func TestBudget_NoCeilingIsUnlimited(t *testing.T) {
	ctx := context.Background()
	f := budgetFixture(t, 3, 0)

	claimed, err := f.stores.TaskStore.ClaimTasks(ctx, f.engineID, 1, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	err = f.committer.Commit(ctx, repositories.FenceFor(claimed[0]), engine.Outcome{
		TokensUsed: 1_000_000,
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	var cancelled int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE status = 'cancelled'`).Scan(&cancelled); err != nil {
		t.Fatalf("failed to count cancelled tasks: %v", err)
	}
	if cancelled != 0 {
		t.Errorf("cancelled = %d, want a workflow with no declared ceiling to be unlimited", cancelled)
	}
}

// Once stopped, later commits must not re-cancel or double-count: every commit
// after the first crossing sees a workflow that is already over.
func TestBudget_CrossingIsRecordedOnce(t *testing.T) {
	ctx := context.Background()
	f := budgetFixture(t, 4, 1000)

	claimed, err := f.stores.TaskStore.ClaimTasks(ctx, f.engineID, 2, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}

	for _, task := range claimed {
		err := f.committer.Commit(ctx, repositories.FenceFor(task), engine.Outcome{TokensUsed: 900})
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	workflow := f.reloadWorkflow(t)
	// Two tasks were pending when the first crossing happened; the second
	// commit must not count them again.
	if workflow.TaskCancelled != 2 {
		t.Errorf("task_cancelled = %d, want 2 counted exactly once", workflow.TaskCancelled)
	}
	if total := workflow.TaskCompleted + workflow.TaskFailed + workflow.TaskCancelled; total != workflow.TaskCount {
		t.Errorf("counters %d do not sum to task_total %d", total, workflow.TaskCount)
	}
}
