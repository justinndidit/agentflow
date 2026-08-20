//go:build integration

package repositories_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/state"
)

const testLeaseTTL = 5 * time.Minute

// registerEngine inserts a node so claims have a valid engine_id —
// tasks.engine_id is a foreign key, so an unregistered node cannot claim.
func registerEngine(t *testing.T, pool *pgxpool.Pool, hostname string) uuid.UUID {
	t.Helper()

	row := models.NewEngineRow(hostname, 8)
	registered, err := stores(pool).EngineStore.Register(context.Background(), &row)
	if err != nil {
		t.Fatalf("failed to register engine %s: %v", hostname, err)
	}
	return registered.ID
}

// seedReadyTasks inserts n tasks that are immediately claimable.
func seedReadyTasks(t *testing.T, pool *pgxpool.Pool, workflowID uuid.UUID, keys ...string) {
	t.Helper()

	tasks := make([]*models.TaskRow, 0, len(keys))
	for _, key := range keys {
		tasks = append(tasks, newTaskRow(workflowID, key, "research-agent"))
	}
	if err := stores(pool).TaskStore.BulkInsertTask(context.Background(), tasks); err != nil {
		t.Fatalf("failed to seed tasks: %v", err)
	}
}

func claimedKeys(tasks []*models.TaskRow) []string {
	keys := make([]string, 0, len(tasks))
	for _, task := range tasks {
		keys = append(keys, task.TaskKey)
	}
	return keys
}

func TestClaimTasks_MarksTheTaskRunningAndLeased(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")
	seedReadyTasks(t, pool, workflow.ID, "only")

	before, err := stores(pool).EngineStore.Now(ctx)
	if err != nil {
		t.Fatalf("failed to read the database clock: %v", err)
	}

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d tasks, want 1", len(claimed))
	}

	task := claimed[0]
	if task.Status != string(state.RunningTaskStatus) {
		t.Errorf("Status = %q, want running", task.Status)
	}
	if task.EngineID == nil || *task.EngineID != engineID {
		t.Errorf("EngineID = %v, want %s", task.EngineID, engineID)
	}
	// The epoch is what fences a stale worker's late write in M3/M4. A claim
	// that does not bump it makes the reaper unsafe.
	if task.LeaseEpoch != 1 {
		t.Errorf("LeaseEpoch = %d, want 1 after the first claim", task.LeaseEpoch)
	}
	if task.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1 after the first claim", task.Attempt)
	}
	if task.StartedAt == nil {
		t.Error("StartedAt is null after a claim")
	}
	if task.LeaseExpiry == nil {
		t.Fatal("LeaseExpiry is null after a claim")
	}
	if !task.LeaseExpiry.After(before) {
		t.Errorf("LeaseExpiry %v is not in the future relative to %v", task.LeaseExpiry, before)
	}
	// Leased for roughly the TTL, from the database clock rather than ours.
	if drift := task.LeaseExpiry.Sub(before.Add(testLeaseTTL)); drift > time.Minute || drift < -time.Minute {
		t.Errorf("LeaseExpiry %v is not about %s after %v", task.LeaseExpiry, testLeaseTTL, before)
	}
}

// The claim persists; the returned rows are not just an in-memory view.
func TestClaimTasks_PersistsTheClaim(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")
	seedReadyTasks(t, pool, workflow.ID, "only")

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}

	stored, err := stores(pool).TaskStore.GetTaskByID(ctx, claimed[0].ID)
	if err != nil {
		t.Fatalf("GetTaskByID failed: %v", err)
	}
	if stored.Status != string(state.RunningTaskStatus) {
		t.Errorf("stored Status = %q, want running", stored.Status)
	}
	if stored.LeaseEpoch != 1 {
		t.Errorf("stored LeaseEpoch = %d, want 1", stored.LeaseEpoch)
	}
}

// A claimed task is no longer pending, so a second pass finds nothing. This is
// what stops one node claiming the same work twice in a tight poll loop.
func TestClaimTasks_DoesNotReclaimRunningWork(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")
	seedReadyTasks(t, pool, workflow.ID, "a", "b")

	first, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("first ClaimTasks failed: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("first claim took %d tasks, want 2", len(first))
	}

	second, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("second ClaimTasks failed: %v", err)
	}
	if len(second) != 0 {
		t.Errorf("second claim took %v, want nothing left", claimedKeys(second))
	}
}

// remaining_deps is the entire readiness mechanism: a task with outstanding
// dependencies must be invisible to the claim.
func TestClaimTasks_SkipsBlockedTasks(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")

	ready := newTaskRow(workflow.ID, "ready", "research-agent")
	blocked := newTaskRow(workflow.ID, "blocked", "research-agent")
	blocked.DependsOn = []string{"ready"}
	blocked.RemainingDeps = 1

	if err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{ready, blocked}); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TaskKey != "ready" {
		t.Errorf("claimed %v, want only [ready]", claimedKeys(claimed))
	}
}

// not_before carries the retry backoff. Claiming through it would defeat the
// backoff entirely and turn a provider outage into a hot loop.
func TestClaimTasks_RespectsNotBefore(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")

	now := newTaskRow(workflow.ID, "due-now", "research-agent")
	later := newTaskRow(workflow.ID, "due-later", "research-agent")
	if err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{now, later}); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}
	// Pushed out using the database clock, the same one the claim compares to.
	if _, err := pool.Exec(ctx,
		`UPDATE tasks SET not_before = now() + interval '1 hour' WHERE id = $1`, later.ID); err != nil {
		t.Fatalf("failed to push out not_before: %v", err)
	}

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TaskKey != "due-now" {
		t.Errorf("claimed %v, want only [due-now]", claimedKeys(claimed))
	}
}

// Higher priority first, then oldest first within a priority.
func TestClaimTasks_OrdersByPriorityThenAge(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")

	// Inserted deliberately out of order.
	low := newTaskRow(workflow.ID, "low", "research-agent")
	low.Priority = int8(state.LowTaskPriority)

	highOld := newTaskRow(workflow.ID, "high-old", "research-agent")
	highOld.Priority = int8(state.HighTaskPriority)
	highOld.CreatedAt = time.Now().UTC().Add(-time.Hour)

	highNew := newTaskRow(workflow.ID, "high-new", "research-agent")
	highNew.Priority = int8(state.HighTaskPriority)

	medium := newTaskRow(workflow.ID, "medium", "research-agent")
	medium.Priority = int8(state.MediumTaskPriority)

	err := stores(pool).TaskStore.BulkInsertTask(ctx,
		[]*models.TaskRow{low, highNew, medium, highOld})
	if err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 2, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d tasks, want 2", len(claimed))
	}

	got := map[string]bool{}
	for _, key := range claimedKeys(claimed) {
		got[key] = true
	}
	if !got["high-old"] || !got["high-new"] {
		t.Errorf("claimed %v, want both high-priority tasks first", claimedKeys(claimed))
	}

	// With only one slot left, the older of two equal priorities wins. Reset and
	// re-check the age tiebreak on its own.
	dbtest.Reset(t, pool)
	workflow = seedWorkflow(t, pool, "research-agent")
	engineID = registerEngine(t, pool, "node-b")

	older := newTaskRow(workflow.ID, "older", "research-agent")
	older.Priority = int8(state.HighTaskPriority)
	older.CreatedAt = time.Now().UTC().Add(-time.Hour)
	newer := newTaskRow(workflow.ID, "newer", "research-agent")
	newer.Priority = int8(state.HighTaskPriority)

	if err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{newer, older}); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	claimed, err = stores(pool).TaskStore.ClaimTasks(ctx, engineID, 1, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TaskKey != "older" {
		t.Errorf("claimed %v, want [older] on the age tiebreak", claimedKeys(claimed))
	}
}

// Never claim past capacity: a lease you cannot honour is worse than a task
// left unclaimed, because it expires and gets reclaimed anyway.
func TestClaimTasks_RespectsLimit(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")
	seedReadyTasks(t, pool, workflow.ID, "a", "b", "c", "d", "e")

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 3, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 3 {
		t.Errorf("claimed %d tasks, want 3", len(claimed))
	}

	var stillPending int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE status = 'pending'`).Scan(&stillPending); err != nil {
		t.Fatalf("failed to count pending tasks: %v", err)
	}
	if stillPending != 2 {
		t.Errorf("pending = %d, want 2 left behind", stillPending)
	}
}

// A node with no free slots must not issue a claim at all.
func TestClaimTasks_ZeroLimitClaimsNothing(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")
	seedReadyTasks(t, pool, workflow.ID, "a", "b")

	for _, limit := range []int{0, -1} {
		claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, limit, testLeaseTTL)
		if err != nil {
			t.Fatalf("ClaimTasks(limit=%d) failed: %v", limit, err)
		}
		if len(claimed) != 0 {
			t.Errorf("ClaimTasks(limit=%d) took %v, want nothing", limit, claimedKeys(claimed))
		}
	}

	if count := countRows(t, pool, "tasks"); count != 2 {
		t.Errorf("tasks = %d, want both still present", count)
	}
}

func TestClaimTasks_NothingReady(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 0 {
		t.Errorf("claimed %v from an empty queue", claimedKeys(claimed))
	}
}

// The regression test for CHECK (attempt <= max_retries + 1).
//
// The claim increments attempt across the whole batch in one statement. A task
// already at its ceiling would abort the entire statement — not just its own
// row — so a single exhausted task would stall every claim this node makes.
// Filtering on attempt <= max_retries in the inner select is what prevents it.
func TestClaimTasks_ExhaustedTaskDoesNotPoisonTheBatch(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")

	exhausted := newTaskRow(workflow.ID, "exhausted", "research-agent")
	exhausted.MaxRetries = 1
	exhausted.Attempt = 2 // at the ceiling: one more increment violates the CHECK

	healthy := newTaskRow(workflow.ID, "healthy", "research-agent")

	err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{exhausted, healthy})
	if err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("the exhausted task poisoned the batch: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TaskKey != "healthy" {
		t.Errorf("claimed %v, want only [healthy]", claimedKeys(claimed))
	}

	// And it stays claimable-free rather than being silently retried forever.
	stored, err := stores(pool).TaskStore.GetTaskByID(ctx, exhausted.ID)
	if err != nil {
		t.Fatalf("GetTaskByID failed: %v", err)
	}
	if stored.Status != string(state.PendingTaskStatus) {
		t.Errorf("exhausted task Status = %q, want it left pending for the reaper", stored.Status)
	}
}

// A task with retries remaining is claimable right up to its last attempt.
func TestClaimTasks_ClaimsTheFinalAttempt(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")

	task := newTaskRow(workflow.ID, "last-chance", "research-agent")
	task.MaxRetries = 2
	task.Attempt = 2 // attempt <= max_retries, so the increment to 3 is legal

	if err := stores(pool).TaskStore.BulkInsertTask(ctx, []*models.TaskRow{task}); err != nil {
		t.Fatalf("BulkInsertTask failed: %v", err)
	}

	claimed, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 10, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d tasks, want the final attempt to be claimable", len(claimed))
	}
	if claimed[0].Attempt != 3 {
		t.Errorf("Attempt = %d, want 3", claimed[0].Attempt)
	}
}

// A reclaimed task keeps its original started_at rather than resetting it, so
// the total wall-clock a task has been in flight stays measurable across
// retries.
func TestClaimTasks_PreservesTheOriginalStartedAt(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedWorkflow(t, pool, "research-agent")
	engineID := registerEngine(t, pool, "node-a")
	seedReadyTasks(t, pool, workflow.ID, "retried")

	first, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 1, testLeaseTTL)
	if err != nil {
		t.Fatalf("first ClaimTasks failed: %v", err)
	}
	originalStart := *first[0].StartedAt

	// Simulate a reaper returning it to pending.
	if _, err := pool.Exec(ctx,
		`UPDATE tasks SET status = 'pending', engine_id = NULL, lease_expires_at = NULL WHERE id = $1`,
		first[0].ID); err != nil {
		t.Fatalf("failed to reset the task: %v", err)
	}

	second, err := stores(pool).TaskStore.ClaimTasks(ctx, engineID, 1, testLeaseTTL)
	if err != nil {
		t.Fatalf("second ClaimTasks failed: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("reclaimed %d tasks, want 1", len(second))
	}
	if !second[0].StartedAt.Equal(originalStart) {
		t.Errorf("StartedAt = %v, want the original %v", second[0].StartedAt, originalStart)
	}
	// The epoch advances on every claim — this is what fences the previous owner.
	if second[0].LeaseEpoch != 2 {
		t.Errorf("LeaseEpoch = %d, want 2 after a second claim", second[0].LeaseEpoch)
	}
	if second[0].Attempt != 2 {
		t.Errorf("Attempt = %d, want 2 after a second claim", second[0].Attempt)
	}
}
