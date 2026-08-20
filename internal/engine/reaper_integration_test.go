//go:build integration

package engine_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
)

func newReaper(pool *pgxpool.Pool, opts ...engine.ReaperOption) *engine.Reaper {
	return engine.NewReaper(
		repositories.NewTxManager(pool, nopLogger()),
		fastEngineConfigWithCapacity(8),
		noBackoff,
		nopLogger(),
		opts...,
	)
}

// expireLease backdates a task's lease so it looks overrun, using the database
// clock the reaper compares against.
func expireLease(t *testing.T, pool *pgxpool.Pool, taskID uuid.UUID) {
	t.Helper()

	_, err := pool.Exec(context.Background(),
		`UPDATE tasks SET lease_expires_at = now() - interval '1 minute' WHERE id = $1`, taskID)
	if err != nil {
		t.Fatalf("failed to expire the lease: %v", err)
	}
}

// killEngine makes a node look dead by backdating its heartbeat.
func killEngine(t *testing.T, pool *pgxpool.Pool, engineID uuid.UUID) {
	t.Helper()

	_, err := pool.Exec(context.Background(),
		`UPDATE engines SET heartbeat_at = now() - interval '1 hour' WHERE id = $1`, engineID)
	if err != nil {
		t.Fatalf("failed to kill the engine: %v", err)
	}
}

// A hung container: the node is alive and heartbeating, but the work overran
// its lease.
func TestReaper_ReclaimsAnOverrunLease(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"hung": nil}, noBackoff)
	claimed := f.claim(t)["hung"]

	expireLease(t, f.pool, claimed.ID)

	count, err := newReaper(f.pool).ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("reclaimed %d tasks, want 1", count)
	}

	task := f.task(t, "hung")
	if task.Status != string(state.PendingTaskStatus) {
		t.Errorf("Status = %q, want pending", task.Status)
	}
	if task.EngineID != nil {
		t.Errorf("EngineID = %v, want it cleared", task.EngineID)
	}
	if task.LeaseExpiry != nil {
		t.Errorf("LeaseExpiry = %v, want it cleared", task.LeaseExpiry)
	}
	if task.FinishedAt != nil {
		t.Errorf("FinishedAt = %v, want null — the task has not finished", task.FinishedAt)
	}
	if task.ErrorMessage == nil || *task.ErrorMessage != string(repositories.ReasonLeaseExpired) {
		t.Errorf("ErrorMessage = %v, want the lease-expired reason", task.ErrorMessage)
	}
}

// A dead node: its lease has not expired yet, but it stopped heartbeating, so
// nothing it holds will ever finish.
func TestReaper_ReclaimsFromADeadEngine(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"orphaned": nil}, noBackoff)
	claimed := f.claim(t)["orphaned"]

	// Lease still valid — only the heartbeat is gone.
	if claimed.LeaseExpiry == nil || !claimed.LeaseExpiry.After(time.Now()) {
		t.Fatal("expected the claim to leave a live lease")
	}
	killEngine(t, f.pool, f.engineID)

	count, err := newReaper(f.pool).ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("reclaimed %d tasks, want 1", count)
	}

	task := f.task(t, "orphaned")
	if task.Status != string(state.PendingTaskStatus) {
		t.Errorf("Status = %q, want pending", task.Status)
	}
	if task.ErrorMessage == nil || *task.ErrorMessage != string(repositories.ReasonEngineLost) {
		t.Errorf("ErrorMessage = %v, want the engine-lost reason", task.ErrorMessage)
	}
}

// A node that shut down cleanly is definitely not still running its work, so
// anything it held is reclaimable immediately rather than after a lease timeout.
func TestReaper_ReclaimsFromAStoppedEngine(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"abandoned": nil}, noBackoff)
	f.claim(t)

	err := f.stores.EngineStore.SetStatus(ctx, f.engineID, state.StoppedEngineStatus)
	if err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}

	count, err := newReaper(f.pool).ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	if count != 1 {
		t.Errorf("reclaimed %d tasks, want the stopped node's work taken back", count)
	}
}

// A healthy node with a live lease must be left completely alone — reclaiming
// early duplicates work that is still running.
func TestReaper_LeavesHealthyWorkAlone(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"working": nil}, noBackoff)
	claimed := f.claim(t)["working"]

	count, err := newReaper(f.pool).ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	if count != 0 {
		t.Errorf("reclaimed %d tasks from a healthy node, want 0", count)
	}

	task := f.task(t, "working")
	if task.Status != string(state.RunningTaskStatus) {
		t.Errorf("Status = %q, want it left running", task.Status)
	}
	if task.EngineID == nil || *task.EngineID != *claimed.EngineID {
		t.Errorf("EngineID = %v, want it untouched", task.EngineID)
	}
}

// A draining node keeps heartbeating precisely so its in-flight work is not
// taken away while it finishes.
func TestReaper_LeavesDrainingNodesAlone(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"finishing": nil}, noBackoff)
	f.claim(t)

	if err := f.stores.EngineStore.SetStatus(ctx, f.engineID, state.DrainingEngineStatus); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}

	count, err := newReaper(f.pool).ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	if count != 0 {
		t.Errorf("reclaimed %d tasks from a draining node, want 0", count)
	}
}

// Tasks that never started are not the reaper's business.
func TestReaper_IgnoresPendingTasks(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"waiting": nil}, noBackoff)
	killEngine(t, f.pool, f.engineID)

	count, err := newReaper(f.pool).ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	if count != 0 {
		t.Errorf("reclaimed %d tasks, want 0 — nothing was running", count)
	}
}

// The reclaim does not bump the epoch; the next claim does. This is the sharpest
// version of the fencing test, because the *same* node reclaims and re-claims
// its own task — engine_id matches again, so only the epoch stands between the
// stalled worker and overwriting a newer result.
func TestReaper_EpochBumpFencesTheOriginalOwner(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"contested": nil}, noBackoff)

	original := f.claim(t)["contested"]
	staleFence := repositories.FenceFor(original)

	expireLease(t, f.pool, original.ID)
	if _, err := newReaper(f.pool).ReapOnce(ctx); err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	// The same engine claims it again.
	reclaimed, err := f.stores.TaskStore.ClaimTasks(ctx, f.engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("re-claimed %d tasks, want 1", len(reclaimed))
	}
	if *reclaimed[0].EngineID != staleFence.EngineID {
		t.Fatal("expected the same engine to re-claim, which is what makes the epoch the only guard")
	}
	if reclaimed[0].LeaseEpoch <= staleFence.LeaseEpoch {
		t.Fatalf("epoch did not advance: %d then %d", staleFence.LeaseEpoch, reclaimed[0].LeaseEpoch)
	}

	// The new owner finishes.
	err = f.committer.Commit(ctx, repositories.FenceFor(reclaimed[0]), engine.Outcome{
		Output: []byte(`{"winner":"second"}`),
	})
	if err != nil {
		t.Fatalf("the second attempt's commit failed: %v", err)
	}

	// The stalled original wakes up and writes. Same engine, same task, stale
	// epoch — it must be rejected.
	err = f.committer.Commit(ctx, staleFence, engine.Outcome{Output: []byte(`{"winner":"first"}`)})
	if !engine.IsFenced(err) {
		t.Fatalf("the stalled worker's write returned %v, want ErrFenced", err)
	}

	results, err := f.stores.TaskResultStore.ListByTask(ctx, original.ID)
	if err != nil {
		t.Fatalf("ListByTask failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("stored %d results, want only the second attempt's", len(results))
	}
}

// A reclaimed task with no retries left is finished for good, and everything
// downstream of it is cancelled rather than left pending forever.
func TestReaper_ExhaustedRetriesFailAndCascade(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"doomed":      nil,
		"child":       {"doomed"},
		"grandchild":  {"child"},
		"independent": nil,
	}, noBackoff)

	// Put the task on its final attempt so one reclaim exhausts it.
	if _, err := f.pool.Exec(ctx,
		`UPDATE tasks SET max_retries = 0 WHERE task_key = 'doomed'`); err != nil {
		t.Fatalf("failed to set the retry budget: %v", err)
	}

	claimed := f.claim(t)["doomed"]
	expireLease(t, f.pool, claimed.ID)

	if _, err := newReaper(f.pool).ReapOnce(ctx); err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	doomed := f.task(t, "doomed")
	if doomed.Status != string(state.FailedTaskStatus) {
		t.Fatalf("doomed Status = %q, want failed", doomed.Status)
	}
	if doomed.FinishedAt == nil {
		t.Error("doomed FinishedAt is null after permanent failure")
	}
	// engine_id survives on the terminal path so it stays clear which node lost
	// the task.
	if doomed.EngineID == nil {
		t.Error("doomed EngineID was cleared; the owning node is part of the record")
	}

	if got := f.task(t, "child").Status; got != string(state.CancelledTaskStatus) {
		t.Errorf("child Status = %q, want cancelled", got)
	}
	if got := f.task(t, "grandchild").Status; got != string(state.CancelledTaskStatus) {
		t.Errorf("grandchild Status = %q, want cancelled", got)
	}
	if got := f.task(t, "independent").Status; got == string(state.CancelledTaskStatus) {
		t.Error("the cascade reached an independent branch")
	}

	workflow := f.reloadWorkflow(t)
	if workflow.TaskFailed != 1 {
		t.Errorf("task_failed = %d, want 1", workflow.TaskFailed)
	}
	if workflow.TaskCancelled != 2 {
		t.Errorf("task_cancelled = %d, want 2", workflow.TaskCancelled)
	}
}

// A node that vanished reported nothing, so there is no result to record.
// Inventing an empty one would put a row in the evidence trail that no attempt
// produced.
func TestReaper_RecordsNoResultForVanishedWork(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"vanished": nil}, noBackoff)
	claimed := f.claim(t)["vanished"]

	expireLease(t, f.pool, claimed.ID)
	if _, err := newReaper(f.pool).ReapOnce(ctx); err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	if count := countTable(t, f.pool, "task_results"); count != 0 {
		t.Errorf("task_results = %d, want none for work that never reported", count)
	}
}

// A reclaimed task is claimable again and runs to completion — the loop that
// makes at-least-once execution real.
func TestReaper_ReclaimedTaskCompletesOnRetry(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"flaky": nil,
		"after": {"flaky"},
	}, noBackoff)

	first := f.claim(t)["flaky"]
	expireLease(t, f.pool, first.ID)
	if _, err := newReaper(f.pool).ReapOnce(ctx); err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	second := f.claim(t)["flaky"]
	if second.LeaseEpoch <= first.LeaseEpoch {
		t.Fatalf("the retry did not advance the epoch: %d then %d",
			first.LeaseEpoch, second.LeaseEpoch)
	}
	if err := f.committer.Commit(ctx, repositories.FenceFor(second), engine.Outcome{}); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if got := f.task(t, "flaky").Status; got != string(state.CompletedTaskStatus) {
		t.Errorf("flaky Status = %q, want completed", got)
	}
	// Readiness propagated despite the detour through the reaper.
	if got := f.task(t, "after").RemainingDeps; got != 0 {
		t.Errorf("after remaining_deps = %d, want 0", got)
	}
}

// Every node runs a reaper, so concurrent reaping has to be safe — that is what
// removes the need for leader election.
//
// What this pins is the outcome: each task reclaimed exactly once, nothing left
// leased, nothing lost. It does not isolate SKIP LOCKED as the reason, and it
// should not claim to — swapping SKIP LOCKED for a plain FOR UPDATE leaves the
// result unchanged, because a reclaimed task clears the very columns the
// predicate matches on. The test does detect real double-processing: allowing
// an already-reclaimed row back into the candidate set makes the reapers spin
// and this fail.
func TestReaper_ConcurrentReapersDoNotDoubleProcess(t *testing.T) {
	const (
		taskCount = 60
		reapers   = 6
	)

	ctx := context.Background()
	graph := map[string][]string{}
	for i := range taskCount {
		graph["task-"+strconv.Itoa(i)] = nil
	}
	f := newCommitFixture(t, graph, noBackoff)

	claimed := f.claim(t)
	if len(claimed) != taskCount {
		t.Fatalf("claimed %d tasks, want %d", len(claimed), taskCount)
	}
	if _, err := f.pool.Exec(ctx,
		`UPDATE tasks SET lease_expires_at = now() - interval '1 minute'`); err != nil {
		t.Fatalf("failed to expire the leases: %v", err)
	}

	var (
		mu    sync.Mutex
		total int
		wg    sync.WaitGroup
		start = make(chan struct{})
	)

	for range reapers {
		pool, err := pgxpool.New(ctx, dbtest.DSN(t))
		if err != nil {
			t.Fatalf("failed to open a pool: %v", err)
		}
		t.Cleanup(pool.Close)
		reaper := newReaper(pool, engine.WithReapBatch(7))

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			for {
				count, err := reaper.ReapOnce(ctx)
				if err != nil {
					t.Errorf("ReapOnce failed: %v", err)
					return
				}
				if count == 0 {
					return
				}
				mu.Lock()
				total += count
				mu.Unlock()
			}
		}()
	}

	close(start)
	wg.Wait()

	// Each task reclaimed exactly once across every reaper.
	if total != taskCount {
		t.Errorf("reapers reclaimed %d tasks in total, want %d", total, taskCount)
	}

	var pending, running int
	err := f.pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE status = 'pending'),
		        count(*) FILTER (WHERE status = 'running') FROM tasks`).Scan(&pending, &running)
	if err != nil {
		t.Fatalf("failed to count tasks: %v", err)
	}
	if pending != taskCount {
		t.Errorf("pending = %d, want all %d back in the queue", pending, taskCount)
	}
	if running != 0 {
		t.Errorf("running = %d, want none left leased", running)
	}
}

// The batch bounds one pass, and repeated passes drain the backlog.
func TestReaper_BatchSizeBoundsOnePass(t *testing.T) {
	ctx := context.Background()
	graph := map[string][]string{}
	for i := range 10 {
		graph["task-"+strconv.Itoa(i)] = nil
	}
	f := newCommitFixture(t, graph, noBackoff)

	f.claim(t)
	if _, err := f.pool.Exec(ctx,
		`UPDATE tasks SET lease_expires_at = now() - interval '1 minute'`); err != nil {
		t.Fatalf("failed to expire the leases: %v", err)
	}

	reaper := newReaper(f.pool, engine.WithReapBatch(3))

	count, err := reaper.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	if count != 3 {
		t.Errorf("first pass reclaimed %d, want the batch size of 3", count)
	}

	total := count
	for range 10 {
		count, err := reaper.ReapOnce(ctx)
		if err != nil {
			t.Fatalf("ReapOnce failed: %v", err)
		}
		total += count
		if count == 0 {
			break
		}
	}
	if total != 10 {
		t.Errorf("reclaimed %d in total, want 10", total)
	}
}

func TestReaper_NothingToDo(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{}, noBackoff)

	count, err := newReaper(f.pool).ReapOnce(ctx)
	if err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	if count != 0 {
		t.Errorf("reclaimed %d tasks from an empty database", count)
	}
}

// Run sweeps on a ticker until cancelled.
func TestReaper_RunSweepsUntilCancelled(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"hung": nil}, noBackoff)
	claimed := f.claim(t)["hung"]
	expireLease(t, f.pool, claimed.ID)

	reaper := newReaper(f.pool, engine.WithReapInterval(50*time.Millisecond))

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reaper.Run(runCtx) }()

	deadline := time.After(5 * time.Second)
	for {
		if f.task(t, "hung").Status == string(state.PendingTaskStatus) {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("the reaper did not reclaim the task before timing out")
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

// A reclaim leaves the task's dependency counter alone: the dependencies that
// were satisfied before the node vanished are still satisfied.
func TestReaper_DoesNotTouchRemainingDeps(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"upstream":   nil,
		"downstream": {"upstream"},
	}, noBackoff)

	upstream := f.claim(t)["upstream"]
	if err := f.committer.Commit(ctx, repositories.FenceFor(upstream), engine.Outcome{}); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	downstream := f.claim(t)["downstream"]
	if downstream == nil {
		t.Fatal("downstream was not claimable after its dependency finished")
	}
	expireLease(t, f.pool, downstream.ID)

	if _, err := newReaper(f.pool).ReapOnce(ctx); err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	reclaimed := f.task(t, "downstream")
	if reclaimed.RemainingDeps != 0 {
		t.Errorf("remaining_deps = %d, want it left at 0 across the reclaim", reclaimed.RemainingDeps)
	}
	if reclaimed.Status != string(state.PendingTaskStatus) {
		t.Errorf("Status = %q, want pending", reclaimed.Status)
	}
}
