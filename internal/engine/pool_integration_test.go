//go:build integration

package engine_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/justinndidit/agentflow/internal/state"
)

func newPool(f *commitFixture, capacity int, rt runtime.Runtime) *engine.Pool {
	return engine.NewPool(capacity, rt,
		engine.NewCommitter(repositories.NewTxManager(f.pool, nopLogger()), nopLogger(), noBackoff),
		engine.NewTemplateResolver(f.stores.TaskResultStore),
		engine.NewCachedAgentImages(f.stores.AgentStore),
		testLeaseTTL, nopLogger())
}

func TestPool_ReservesSlotsOnHandoff(t *testing.T) {
	graph := map[string][]string{}
	for i := range 3 {
		graph["task-"+strconv.Itoa(i)] = nil
	}
	f := newCommitFixture(t, graph, noBackoff)

	// Blocks until released, so slots stay reserved for the assertion.
	gate := make(chan struct{})
	pool := newPool(f, 4, runtime.RuntimeFunc(func(ctx context.Context, req runtime.Request) (*runtime.Response, error) {
		<-gate
		return &runtime.Response{Output: []byte(`{}`)}, nil
	}))

	if got := pool.FreeSlots(); got != 4 {
		t.Fatalf("FreeSlots = %d before any work, want 4", got)
	}

	claimed := f.claimedNow(t)
	if err := pool.Handle(context.Background(), claimed); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	// Reserved synchronously, so the dispatcher cannot claim against capacity
	// that is already spoken for.
	if got := pool.FreeSlots(); got != 1 {
		t.Errorf("FreeSlots = %d after handing over 3 tasks, want 1", got)
	}

	close(gate)
	if err := pool.Drain(context.Background()); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	if got := pool.FreeSlots(); got != 4 {
		t.Errorf("FreeSlots = %d after draining, want 4", got)
	}
}

// The dispatcher already limits its claim to FreeSlots, so an oversized batch
// is a bug. Accepting part of it would leave the rest leased to a node that
// never started them, which is worse than rejecting the lot.
func TestPool_RejectsAnOversizedBatch(t *testing.T) {
	graph := map[string][]string{}
	for i := range 4 {
		graph["task-"+strconv.Itoa(i)] = nil
	}
	f := newCommitFixture(t, graph, noBackoff)

	pool := newPool(f, 2, runtime.NewEcho(0))
	claimed := f.claimedNow(t)
	if len(claimed) != 4 {
		t.Fatalf("claimed %d tasks, want 4", len(claimed))
	}

	if err := pool.Handle(context.Background(), claimed); err == nil {
		t.Fatal("expected Handle to reject a batch larger than capacity")
	}
	if got := pool.FreeSlots(); got != 2 {
		t.Errorf("FreeSlots = %d after a rejected batch, want the reservation rolled back", got)
	}
}

func TestPool_EmptyBatch(t *testing.T) {
	f := newCommitFixture(t, map[string][]string{}, noBackoff)
	pool := newPool(f, 2, runtime.NewEcho(0))

	if err := pool.Handle(context.Background(), nil); err != nil {
		t.Errorf("Handle(nil) returned %v", err)
	}
	if got := pool.FreeSlots(); got != 2 {
		t.Errorf("FreeSlots = %d, want 2", got)
	}
}

// A task that runs to completion is committed by the pool without any further
// prompting.
func TestPool_CommitsSuccess(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"work": nil}, noBackoff)

	pool := newPool(f, 2, runtime.NewEcho(0))
	if err := pool.Handle(ctx, f.claimedNow(t)); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	task := f.task(t, "work")
	if task.Status != string(state.CompletedTaskStatus) {
		t.Errorf("Status = %q, want completed", task.Status)
	}

	results, err := f.stores.TaskResultStore.ListByTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("ListByTask failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("stored %d results, want 1", len(results))
	}
}

// A failing runtime is committed as a failure, which returns the task to the
// queue while it still has retries.
func TestPool_CommitsFailure(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"doomed": nil}, noBackoff)

	echo := runtime.NewEcho(0)
	echo.FailKeys = map[string]bool{"doomed": true}
	pool := newPool(f, 2, echo)

	if err := pool.Handle(ctx, f.claimedNow(t)); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	task := f.task(t, "doomed")
	if task.Status != string(state.PendingTaskStatus) {
		t.Errorf("Status = %q, want pending for a retry", task.Status)
	}
	if task.ErrorMessage == nil {
		t.Error("ErrorMessage is null after a failed attempt")
	}
}

// The attempt is bounded by the shorter of the task's timeout and its lease.
// Work that outlived its lease would be reclaimed and run twice.
func TestPool_BoundsAttemptByLease(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"slow": nil}, noBackoff)

	// A runtime slower than the lease allows.
	pool := engine.NewPool(2, runtime.NewEcho(10*time.Second),
		engine.NewCommitter(repositories.NewTxManager(f.pool, nopLogger()), nopLogger(), noBackoff),
		engine.StaticResolver{},
		engine.StaticAgentImages("unused"),
		300*time.Millisecond, nopLogger())

	started := time.Now()
	if err := pool.Handle(ctx, f.claimedNow(t)); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	elapsed := time.Since(started)

	if elapsed > 5*time.Second {
		t.Errorf("the attempt ran for %s; it was not bounded by the lease", elapsed)
	}
	// Cut short counts as a failed attempt, not a silent success.
	task := f.task(t, "slow")
	if task.Status == string(state.CompletedTaskStatus) {
		t.Error("a task cut short by its deadline was recorded as completed")
	}
}

// A worker whose lease was reclaimed mid-run must discard its result rather
// than overwrite the newer one, and the pool must treat that as expected.
func TestPool_DiscardsResultWhenFenced(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"contested": nil}, noBackoff)

	claimed := f.claimedNow(t)
	gate := make(chan struct{})
	pool := newPool(f, 2, runtime.RuntimeFunc(func(ctx context.Context, req runtime.Request) (*runtime.Response, error) {
		<-gate
		return &runtime.Response{Output: []byte(`{"from":"stalled"}`)}, nil
	}))

	if err := pool.Handle(ctx, claimed); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	// While it is "running", the lease is reclaimed and the task is redone.
	expireLease(t, f.pool, claimed[0].ID)
	if _, err := newReaper(f.pool).ReapOnce(ctx); err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	replacement := f.claimedNow(t)
	if len(replacement) != 1 {
		t.Fatalf("re-claimed %d tasks, want 1", len(replacement))
	}
	err := f.committer.Commit(ctx, repositories.FenceFor(replacement[0]), engine.Outcome{
		Output: []byte(`{"from":"replacement"}`),
	})
	if err != nil {
		t.Fatalf("the replacement's commit failed: %v", err)
	}

	// Now let the stalled worker finish. Its commit must be fenced out.
	close(gate)
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	results, err := f.stores.TaskResultStore.ListByTask(ctx, claimed[0].ID)
	if err != nil {
		t.Fatalf("ListByTask failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("stored %d results, want only the replacement's", len(results))
	}
	if got := f.task(t, "contested").Status; got != string(state.CompletedTaskStatus) {
		t.Errorf("Status = %q, want the replacement's completion to stand", got)
	}
}

// Drain gives up when its context expires, leaving the work to lease expiry
// rather than blocking shutdown indefinitely.
func TestPool_DrainRespectsItsDeadline(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"stuck": nil}, noBackoff)

	gate := make(chan struct{})
	defer close(gate)

	pool := newPool(f, 2, runtime.RuntimeFunc(func(ctx context.Context, req runtime.Request) (*runtime.Response, error) {
		<-gate
		return &runtime.Response{}, nil
	}))
	if err := pool.Handle(ctx, f.claimedNow(t)); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	drainCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	if err := pool.Drain(drainCtx); err == nil {
		t.Error("expected Drain to report that it gave up")
	}
}
