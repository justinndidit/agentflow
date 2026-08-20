//go:build integration

package engine_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
)

const testLeaseTTL = 5 * time.Minute

// noBackoff keeps retry scheduling out of the way of tests that are not about
// scheduling; a retried task is immediately eligible again.
var noBackoff = engine.Backoff{Base: time.Nanosecond, Max: time.Nanosecond}

type commitFixture struct {
	pool      *pgxpool.Pool
	stores    *repositories.Stores
	committer *engine.Committer
	engineID  uuid.UUID
	workflow  *models.WorkflowRow

	// claimed accumulates across calls. A claim takes everything that is ready,
	// so a test that drives one task through several attempts also picks up its
	// unrelated siblings on the first pass; keeping them means a later step can
	// still commit one without re-claiming it.
	claimed map[string]*models.TaskRow
}

// newCommitFixture seeds a workflow whose tasks form the shape the caller
// describes: each entry is a task key and the keys it depends on.
func newCommitFixture(t *testing.T, graph map[string][]string, backoff engine.Backoff) *commitFixture {
	t.Helper()

	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))

	workflowRow := dbtest.NewWorkflowRow()
	workflowRow.TaskCount = len(graph)
	workflow, err := stores.WorkflowStore.CreateWorkflow(ctx, workflowRow)
	if err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	tasks := make([]*models.TaskRow, 0, len(graph))
	for key, deps := range graph {
		task := dbtest.NewTaskRow(workflow.ID, key, "research-agent")
		task.DependsOn = deps
		task.RemainingDeps = len(deps)
		tasks = append(tasks, task)
	}
	if len(tasks) > 0 {
		if err := stores.TaskStore.BulkInsertTask(ctx, tasks); err != nil {
			t.Fatalf("failed to seed tasks: %v", err)
		}
	}

	engineRow := models.NewEngineRow("node-a", 8)
	registered, err := stores.EngineStore.Register(ctx, &engineRow)
	if err != nil {
		t.Fatalf("failed to register engine: %v", err)
	}

	return &commitFixture{
		pool:      pool,
		stores:    stores,
		committer: engine.NewCommitter(repositories.NewTxManager(pool, nopLogger()), nopLogger(), backoff),
		engineID:  registered.ID,
		workflow:  workflow,
		claimed:   map[string]*models.TaskRow{},
	}
}

// claim takes whatever is ready, merges it into the fixture's record of claimed
// tasks, and returns that record. A re-claimed task overwrites its earlier
// entry, so callers always see the current lease.
func (f *commitFixture) claim(t *testing.T) map[string]*models.TaskRow {
	t.Helper()

	claimed, err := f.stores.TaskStore.ClaimTasks(context.Background(), f.engineID, 100, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	for _, task := range claimed {
		f.claimed[task.TaskKey] = task
	}
	return f.claimed
}

// claimedNow is claim without the accumulated history, for tests asserting on
// what a single pass took.
func (f *commitFixture) claimedNow(t *testing.T) []*models.TaskRow {
	t.Helper()

	claimed, err := f.stores.TaskStore.ClaimTasks(context.Background(), f.engineID, 100, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	for _, task := range claimed {
		f.claimed[task.TaskKey] = task
	}
	return claimed
}

func (f *commitFixture) task(t *testing.T, key string) *models.TaskRow {
	t.Helper()

	tasks, err := f.stores.TaskStore.ListTasksByWorkflow(context.Background(), f.workflow.ID)
	if err != nil {
		t.Fatalf("ListTasksByWorkflow failed: %v", err)
	}
	for _, task := range tasks {
		if task.TaskKey == key {
			return task
		}
	}
	t.Fatalf("no task %q in workflow", key)
	return nil
}

func (f *commitFixture) reloadWorkflow(t *testing.T) *models.WorkflowRow {
	t.Helper()

	workflow, err := f.stores.WorkflowStore.GetWorkflowByID(context.Background(), f.workflow.ID)
	if err != nil {
		t.Fatalf("GetWorkflowByID failed: %v", err)
	}
	return workflow
}

func TestCommitter_CompletesAClaimedTask(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"only": nil}, noBackoff)
	claimed := f.claim(t)

	err := f.committer.Commit(ctx, repositories.FenceFor(claimed["only"]), engine.Outcome{
		Output:     []byte(`{"jobs":42}`),
		TokensUsed: 1200,
		CostMicros: 900,
		Duration:   1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	task := f.task(t, "only")
	if task.Status != string(state.CompletedTaskStatus) {
		t.Errorf("Status = %q, want completed", task.Status)
	}
	if task.FinishedAt == nil {
		t.Error("FinishedAt is null after completion")
	}
	// The lease is over; leaving it set would make a completed task look like
	// something the reaper should examine.
	if task.LeaseExpiry != nil {
		t.Errorf("LeaseExpiry = %v, want null after completion", task.LeaseExpiry)
	}

	result, err := f.stores.TaskResultStore.GetByAttempt(ctx, task.ID, 1)
	if err != nil {
		t.Fatalf("GetByAttempt failed: %v", err)
	}
	if string(result.Output) != `{"jobs": 42}` && string(result.Output) != `{"jobs":42}` {
		t.Errorf("Output = %s, want the worker's JSON", result.Output)
	}
	if result.TokensUsed != 1200 {
		t.Errorf("TokensUsed = %d, want 1200", result.TokensUsed)
	}
	if result.DurationMS != 1500 {
		t.Errorf("DurationMS = %d, want 1500", result.DurationMS)
	}

	workflow := f.reloadWorkflow(t)
	if workflow.TaskCompleted != 1 {
		t.Errorf("task_completed = %d, want 1", workflow.TaskCompleted)
	}
	if workflow.TokensUsed != 1200 {
		t.Errorf("tokens_used = %d, want 1200", workflow.TokensUsed)
	}
	if workflow.Status != string(state.CompletedWorkflowStatus) {
		t.Errorf("workflow Status = %q, want completed", workflow.Status)
	}
}

// The fence is the whole point of the milestone. A write carrying a superseded
// epoch must affect nothing.
func TestCommitter_RejectsAStaleEpoch(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"only": nil}, noBackoff)
	claimed := f.claim(t)

	fence := repositories.FenceFor(claimed["only"])
	stale := fence
	stale.LeaseEpoch = fence.LeaseEpoch - 1

	err := f.committer.Commit(ctx, stale, engine.Outcome{Output: []byte(`{}`)})
	if !engine.IsFenced(err) {
		t.Fatalf("Commit with a stale epoch returned %v, want ErrFenced", err)
	}

	task := f.task(t, "only")
	if task.Status != string(state.RunningTaskStatus) {
		t.Errorf("Status = %q, want the task untouched at running", task.Status)
	}
	if count := countTable(t, f.pool, "task_results"); count != 0 {
		t.Errorf("task_results = %d, want the fenced write to record nothing", count)
	}
}

// A write from a node that no longer owns the task is rejected even if the
// epoch happens to match.
func TestCommitter_RejectsAForeignEngine(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"only": nil}, noBackoff)
	claimed := f.claim(t)

	fence := repositories.FenceFor(claimed["only"])
	fence.EngineID = uuid.New()

	if err := f.committer.Commit(ctx, fence, engine.Outcome{}); !engine.IsFenced(err) {
		t.Fatalf("Commit from a foreign engine returned %v, want ErrFenced", err)
	}
}

// Committing twice is the same race as a zombie write: the second attempt finds
// the task no longer running and is fenced out.
func TestCommitter_RejectsDoubleCommit(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"only": nil}, noBackoff)
	claimed := f.claim(t)
	fence := repositories.FenceFor(claimed["only"])

	if err := f.committer.Commit(ctx, fence, engine.Outcome{}); err != nil {
		t.Fatalf("first Commit failed: %v", err)
	}
	if err := f.committer.Commit(ctx, fence, engine.Outcome{}); !engine.IsFenced(err) {
		t.Fatalf("second Commit returned %v, want ErrFenced", err)
	}

	// And the counters were not double-counted.
	workflow := f.reloadWorkflow(t)
	if workflow.TaskCompleted != 1 {
		t.Errorf("task_completed = %d, want 1", workflow.TaskCompleted)
	}
}

// The full zombie sequence from the architecture doc, written literally:
// A claims, the lease is reclaimed, B claims and finishes, then A wakes up.
func TestCommitter_ZombieWriteAfterReclaim(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"contested": nil}, noBackoff)

	// t0: engine A claims.
	engineA := f.claim(t)["contested"]
	fenceA := repositories.FenceFor(engineA)

	// t2: the lease expires and the task is returned to pending. The reaper
	// does this in M4; here it is done directly so the race can be exercised.
	if _, err := f.pool.Exec(ctx,
		`UPDATE tasks SET status = 'pending', engine_id = NULL, lease_expires_at = NULL
		  WHERE id = $1`, engineA.ID); err != nil {
		t.Fatalf("failed to simulate reclaim: %v", err)
	}

	// t3: engine B claims the same task and finishes it. The claim bumps the
	// epoch, which is what makes A stale.
	engineB := f.claim(t)["contested"]
	if engineB == nil {
		t.Fatal("the reclaimed task was not claimable again")
	}
	if engineB.LeaseEpoch <= fenceA.LeaseEpoch {
		t.Fatalf("reclaim did not advance the epoch: %d then %d",
			fenceA.LeaseEpoch, engineB.LeaseEpoch)
	}
	err := f.committer.Commit(ctx, repositories.FenceFor(engineB), engine.Outcome{
		Output: []byte(`{"winner":"b"}`),
	})
	if err != nil {
		t.Fatalf("engine B's commit failed: %v", err)
	}

	// t4: engine A wakes up and writes. It must not overwrite B's result.
	err = f.committer.Commit(ctx, fenceA, engine.Outcome{Output: []byte(`{"winner":"a"}`)})
	if !engine.IsFenced(err) {
		t.Fatalf("the zombie write returned %v, want ErrFenced", err)
	}

	results, err := f.stores.TaskResultStore.ListByTask(ctx, engineA.ID)
	if err != nil {
		t.Fatalf("ListByTask failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("stored %d results, want only engine B's", len(results))
	}
	if string(results[0].Output) != `{"winner": "b"}` && string(results[0].Output) != `{"winner":"b"}` {
		t.Errorf("stored output = %s, want engine B's", results[0].Output)
	}
}

func countTable(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()

	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&count); err != nil {
		t.Fatalf("failed to count %s: %v", table, err)
	}
	return count
}

// Completion decrements exactly the tasks that name it, and readiness
// propagates one level at a time.
func TestCommitter_DecrementsDependents(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"fetch":  nil,
		"rank":   {"fetch"},
		"score":  {"fetch"},
		"report": {"rank", "score"},
	}, noBackoff)

	fetch := f.claim(t)["fetch"]
	if err := f.committer.Commit(ctx, repositories.FenceFor(fetch), engine.Outcome{}); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if got := f.task(t, "rank").RemainingDeps; got != 0 {
		t.Errorf("rank remaining_deps = %d, want 0", got)
	}
	if got := f.task(t, "score").RemainingDeps; got != 0 {
		t.Errorf("score remaining_deps = %d, want 0", got)
	}
	// report depends on two tasks, neither of which has finished.
	if got := f.task(t, "report").RemainingDeps; got != 2 {
		t.Errorf("report remaining_deps = %d, want 2 — it does not depend on fetch", got)
	}
}

// The bug the schema divergence sets up: depends_on holds task keys, which are
// unique only within a workflow. Without the workflow_id scope, one run
// decrements the other's counters and tasks start before their dependencies
// have finished.
func TestCommitter_DecrementIsScopedToItsWorkflow(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	committer := engine.NewCommitter(repositories.NewTxManager(pool, nopLogger()), nopLogger(), noBackoff)

	engineRow := models.NewEngineRow("node-a", 8)
	registered, err := stores.EngineStore.Register(ctx, &engineRow)
	if err != nil {
		t.Fatalf("failed to register engine: %v", err)
	}

	// Two runs of the same manifest: identical task keys, different workflows.
	workflows := make([]*models.WorkflowRow, 2)
	for i := range workflows {
		row := dbtest.NewWorkflowRow()
		row.TaskCount = 2
		workflow, err := stores.WorkflowStore.CreateWorkflow(ctx, row)
		if err != nil {
			t.Fatalf("failed to create workflow %d: %v", i, err)
		}
		workflows[i] = workflow

		upstream := dbtest.NewTaskRow(workflow.ID, "fetch", "research-agent")
		downstream := dbtest.NewTaskRow(workflow.ID, "rank", "research-agent")
		downstream.DependsOn = []string{"fetch"}
		downstream.RemainingDeps = 1

		if err := stores.TaskStore.BulkInsertTask(ctx, []*models.TaskRow{upstream, downstream}); err != nil {
			t.Fatalf("failed to seed workflow %d: %v", i, err)
		}
	}

	// Finish the first run's fetch only.
	claimed, err := stores.TaskStore.ClaimTasks(ctx, registered.ID, 100, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	var firstFetch *models.TaskRow
	for _, task := range claimed {
		if task.WorkflowID == workflows[0].ID && task.TaskKey == "fetch" {
			firstFetch = task
		}
	}
	if firstFetch == nil {
		t.Fatal("did not claim the first workflow's fetch")
	}
	if err := committer.Commit(ctx, repositories.FenceFor(firstFetch), engine.Outcome{}); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	remaining := func(workflowID uuid.UUID) int {
		tasks, err := stores.TaskStore.ListTasksByWorkflow(ctx, workflowID)
		if err != nil {
			t.Fatalf("ListTasksByWorkflow failed: %v", err)
		}
		for _, task := range tasks {
			if task.TaskKey == "rank" {
				return task.RemainingDeps
			}
		}
		t.Fatal("no rank task")
		return -1
	}

	if got := remaining(workflows[0].ID); got != 0 {
		t.Errorf("first run's rank remaining_deps = %d, want 0", got)
	}
	if got := remaining(workflows[1].ID); got != 1 {
		t.Errorf("second run's rank remaining_deps = %d, want 1 — the decrement leaked across workflows", got)
	}
}

// Three upstreams finishing at once on separate connections must leave the
// shared downstream at exactly zero. A lost update strands it forever; a double
// decrement drives it negative and trips CHECK (remaining_deps >= 0).
func TestCommitter_ConcurrentFanInReachesZeroExactlyOnce(t *testing.T) {
	const upstreams = 8

	ctx := context.Background()
	graph := map[string][]string{}
	deps := make([]string, 0, upstreams)
	for i := range upstreams {
		key := "up-" + strconv.Itoa(i)
		graph[key] = nil
		deps = append(deps, key)
	}
	graph["sink"] = deps

	f := newCommitFixture(t, graph, noBackoff)
	claimed := f.claim(t)
	if len(claimed) != upstreams {
		t.Fatalf("claimed %d tasks, want the %d upstreams", len(claimed), upstreams)
	}

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for i := range upstreams {
		task := claimed["up-"+strconv.Itoa(i)]
		pool, err := pgxpool.New(ctx, dbtest.DSN(t))
		if err != nil {
			t.Fatalf("failed to open a pool: %v", err)
		}
		t.Cleanup(pool.Close)

		committer := engine.NewCommitter(repositories.NewTxManager(pool, nopLogger()), nopLogger(), noBackoff)

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := committer.Commit(ctx, repositories.FenceFor(task), engine.Outcome{}); err != nil {
				t.Errorf("concurrent Commit failed: %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	sink := f.task(t, "sink")
	if sink.RemainingDeps != 0 {
		t.Errorf("sink remaining_deps = %d, want exactly 0", sink.RemainingDeps)
	}
	if sink.Status != string(state.PendingTaskStatus) {
		t.Errorf("sink Status = %q, want it pending and now claimable", sink.Status)
	}

	workflow := f.reloadWorkflow(t)
	if workflow.TaskCompleted != upstreams {
		t.Errorf("task_completed = %d, want %d", workflow.TaskCompleted, upstreams)
	}
}

// A retry keeps its dependency counter: the dependencies satisfied before the
// failure are still satisfied, and decrementing again would corrupt it.
func TestCommitter_RetryLeavesRemainingDepsAlone(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"fetch": nil,
		"rank":  {"fetch"},
	}, noBackoff)

	fetch := f.claim(t)["fetch"]
	if err := f.committer.Commit(ctx, repositories.FenceFor(fetch), engine.Outcome{}); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	rank := f.claim(t)["rank"]
	if rank == nil {
		t.Fatal("rank was not claimable after its dependency finished")
	}
	err := f.committer.Commit(ctx, repositories.FenceFor(rank), engine.Outcome{
		Err: errors.New("agent exited non-zero"),
	})
	if err != nil {
		t.Fatalf("Commit of a failure returned %v", err)
	}

	retried := f.task(t, "rank")
	if retried.Status != string(state.PendingTaskStatus) {
		t.Errorf("Status = %q, want pending for a retry", retried.Status)
	}
	if retried.RemainingDeps != 0 {
		t.Errorf("remaining_deps = %d, want it left at 0 across the retry", retried.RemainingDeps)
	}
	if retried.EngineID != nil {
		t.Errorf("EngineID = %v, want it cleared on retry", retried.EngineID)
	}
	if retried.FinishedAt != nil {
		t.Errorf("FinishedAt = %v, want null — a retried task has not finished", retried.FinishedAt)
	}
	if retried.ErrorMessage == nil || *retried.ErrorMessage != "agent exited non-zero" {
		t.Errorf("ErrorMessage = %v, want the failure recorded", retried.ErrorMessage)
	}
}

// Each attempt keeps its own result row, so the evidence of why a run failed
// survives the retry that follows it.
func TestCommitter_EachAttemptKeepsItsResult(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"flaky": nil}, noBackoff)

	first := f.claim(t)["flaky"]
	err := f.committer.Commit(ctx, repositories.FenceFor(first), engine.Outcome{
		Output: []byte(`{"attempt":1}`),
		Err:    errors.New("first failure"),
	})
	if err != nil {
		t.Fatalf("first Commit failed: %v", err)
	}

	second := f.claim(t)["flaky"]
	if second == nil {
		t.Fatal("the failed task was not claimable again")
	}
	err = f.committer.Commit(ctx, repositories.FenceFor(second), engine.Outcome{
		Output: []byte(`{"attempt":2}`),
	})
	if err != nil {
		t.Fatalf("second Commit failed: %v", err)
	}

	results, err := f.stores.TaskResultStore.ListByTask(ctx, first.ID)
	if err != nil {
		t.Fatalf("ListByTask failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("stored %d results, want one per attempt", len(results))
	}
	if results[0].Attempt != 1 || results[1].Attempt != 2 {
		t.Errorf("attempts = %d and %d, want 1 and 2", results[0].Attempt, results[1].Attempt)
	}
}

// Exhausting the retry budget fails the task permanently and cancels everything
// downstream, which can never become ready. Independent branches survive.
func TestCommitter_PermanentFailureCascades(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"doomed":      nil,
		"child":       {"doomed"},
		"grandchild":  {"child"},
		"independent": nil,
	}, noBackoff)

	// Burn the retry budget: max_retries is 3 in the fixture, so four attempts.
	for attempt := 1; attempt <= 4; attempt++ {
		claimed := f.claim(t)["doomed"]
		if claimed == nil {
			t.Fatalf("doomed was not claimable on attempt %d", attempt)
		}
		err := f.committer.Commit(ctx, repositories.FenceFor(claimed), engine.Outcome{
			Err: errors.New("permanent failure"),
		})
		if err != nil {
			t.Fatalf("Commit on attempt %d failed: %v", attempt, err)
		}
	}

	doomed := f.task(t, "doomed")
	if doomed.Status != string(state.FailedTaskStatus) {
		t.Fatalf("doomed Status = %q after exhausting retries, want failed", doomed.Status)
	}
	if doomed.Attempt != 4 {
		t.Errorf("doomed Attempt = %d, want 4", doomed.Attempt)
	}
	if doomed.FinishedAt == nil {
		t.Error("doomed FinishedAt is null after permanent failure")
	}

	// Transitive: both the direct dependent and its own dependent.
	if got := f.task(t, "child").Status; got != string(state.CancelledTaskStatus) {
		t.Errorf("child Status = %q, want cancelled", got)
	}
	if got := f.task(t, "grandchild").Status; got != string(state.CancelledTaskStatus) {
		t.Errorf("grandchild Status = %q, want cancelled", got)
	}
	// Cancelling a whole workflow on one failure would throw away work that has
	// already been paid for. The task was picked up by the same claim pass that
	// took doomed, so it is running rather than pending — what matters is that
	// the cascade did not reach it.
	if got := f.task(t, "independent").Status; got == string(state.CancelledTaskStatus) {
		t.Error("the cascade cancelled an independent branch")
	}

	workflow := f.reloadWorkflow(t)
	if workflow.TaskFailed != 1 {
		t.Errorf("task_failed = %d, want 1", workflow.TaskFailed)
	}
	if workflow.TaskCancelled != 2 {
		t.Errorf("task_cancelled = %d, want 2", workflow.TaskCancelled)
	}
	// Three of four accounted for; the workflow is not finished yet.
	if workflow.Status == string(state.CompletedWorkflowStatus) {
		t.Error("workflow reported completed while a task is still runnable")
	}
}

// A workflow that loses a branch ends failed even though every task that could
// finish did.
func TestCommitter_WorkflowEndsFailedWhenATaskFailed(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"doomed": nil,
		"fine":   nil,
	}, noBackoff)

	for attempt := 1; attempt <= 4; attempt++ {
		claimed := f.claim(t)["doomed"]
		if claimed == nil {
			t.Fatalf("doomed was not claimable on attempt %d", attempt)
		}
		err := f.committer.Commit(ctx, repositories.FenceFor(claimed), engine.Outcome{
			Err: errors.New("permanent failure"),
		})
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	fine := f.claim(t)["fine"]
	if fine == nil {
		t.Fatal("the healthy task was not claimable")
	}
	if err := f.committer.Commit(ctx, repositories.FenceFor(fine), engine.Outcome{}); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	workflow := f.reloadWorkflow(t)
	if workflow.Status != string(state.FailedWorkflowStatus) {
		t.Errorf("workflow Status = %q, want failed", workflow.Status)
	}
	if workflow.TaskCompleted+workflow.TaskFailed+workflow.TaskCancelled != workflow.TaskCount {
		t.Errorf("counters %d+%d+%d do not sum to task_total %d",
			workflow.TaskCompleted, workflow.TaskFailed,
			workflow.TaskCancelled, workflow.TaskCount)
	}
}

// A whole chain driven by claim and commit alone, with no engine process — the
// milestone's done-when.
func TestCommitter_DrivesAChainToCompletion(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"first":  nil,
		"second": {"first"},
		"third":  {"second"},
	}, noBackoff)

	for _, key := range []string{"first", "second", "third"} {
		pass := f.claimedNow(t)
		if len(pass) != 1 {
			t.Fatalf("claimed %d tasks at step %s, want only that one to be ready", len(pass), key)
		}
		task := pass[0]
		if task.TaskKey != key {
			t.Fatalf("claimed %q at step %s, want %s", task.TaskKey, key, key)
		}
		if err := f.committer.Commit(ctx, repositories.FenceFor(task), engine.Outcome{}); err != nil {
			t.Fatalf("Commit of %s failed: %v", key, err)
		}
	}

	workflow := f.reloadWorkflow(t)
	if workflow.Status != string(state.CompletedWorkflowStatus) {
		t.Errorf("workflow Status = %q, want completed", workflow.Status)
	}
	if workflow.TaskCompleted != 3 {
		t.Errorf("task_completed = %d, want 3", workflow.TaskCompleted)
	}
}
