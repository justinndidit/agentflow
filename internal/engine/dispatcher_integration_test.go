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
)

func taskStore(pool *pgxpool.Pool) repositories.TaskStore {
	return repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil)).TaskStore
}

// collector records what the dispatcher hands it.
type collector struct {
	mu    sync.Mutex
	tasks []*models.TaskRow
	err   error

	// delay throttles the handler. Handle is called synchronously from the
	// claim loop, so a delay here bounds how fast one dispatcher can drain a
	// queue — which is what keeps a two-node test from being decided by
	// goroutine scheduling.
	delay time.Duration
}

func (c *collector) Handle(_ context.Context, tasks []*models.TaskRow) error {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return c.err
	}
	c.tasks = append(c.tasks, tasks...)
	delay := c.delay
	c.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	return nil
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.tasks)
}

// dispatcherFixture wires a registered engine, a seeded workflow and n ready
// tasks, returning the dispatcher under test.
func dispatcherFixture(t *testing.T, pool *pgxpool.Pool, capacity int, taskCount int, opts ...engine.DispatcherOption) (*engine.Dispatcher, *collector) {
	t.Helper()

	ctx := context.Background()
	dbtest.SeedAgents(t, pool, "research-agent")

	store := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	workflow, err := store.WorkflowStore.CreateWorkflow(ctx, dbtest.NewWorkflowRow())
	if err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	if taskCount > 0 {
		tasks := make([]*models.TaskRow, 0, taskCount)
		for i := range taskCount {
			tasks = append(tasks, dbtest.NewTaskRow(workflow.ID, "task-"+strconv.Itoa(i), "research-agent"))
		}
		if err := store.TaskStore.BulkInsertTask(ctx, tasks); err != nil {
			t.Fatalf("failed to seed tasks: %v", err)
		}
	}

	engineRow := models.NewEngineRow("node-a", capacity)
	registered, err := store.EngineStore.Register(ctx, &engineRow)
	if err != nil {
		t.Fatalf("failed to register engine: %v", err)
	}

	handler := &collector{}
	dispatcher := engine.NewDispatcher(
		store.TaskStore,
		registered.ID,
		engine.StaticCapacity(capacity),
		handler,
		fastEngineConfigWithCapacity(capacity),
		nopLogger(),
		opts...,
	)
	return dispatcher, handler
}

func TestDispatcher_ClaimsUpToCapacity(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dispatcher, handler := dispatcherFixture(t, pool, 3, 10)

	claimed, err := dispatcher.ClaimOnce(ctx)
	if err != nil {
		t.Fatalf("ClaimOnce failed: %v", err)
	}

	// Never claim past capacity: a lease this node cannot start work against
	// just expires and gets reclaimed.
	if claimed != 3 {
		t.Errorf("claimed %d tasks, want 3", claimed)
	}
	if handler.count() != 3 {
		t.Errorf("handler received %d tasks, want 3", handler.count())
	}
}

// A full node claims nothing at all rather than issuing a query it cannot act
// on.
func TestDispatcher_FullNodeClaimsNothing(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dispatcher, handler := dispatcherFixture(t, pool, 0, 5)

	claimed, err := dispatcher.ClaimOnce(ctx)
	if err != nil {
		t.Fatalf("ClaimOnce failed: %v", err)
	}
	if claimed != 0 {
		t.Errorf("claimed %d tasks with no free slots, want 0", claimed)
	}
	if handler.count() != 0 {
		t.Errorf("handler received %d tasks, want none", handler.count())
	}

	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status = 'pending'`).Scan(&pending); err != nil {
		t.Fatalf("failed to count pending: %v", err)
	}
	if pending != 5 {
		t.Errorf("pending = %d, want all 5 untouched", pending)
	}
}

func TestDispatcher_EmptyQueue(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dispatcher, handler := dispatcherFixture(t, pool, 4, 0)

	claimed, err := dispatcher.ClaimOnce(ctx)
	if err != nil {
		t.Fatalf("ClaimOnce failed: %v", err)
	}
	if claimed != 0 || handler.count() != 0 {
		t.Errorf("claimed %d and handled %d from an empty queue", claimed, handler.count())
	}
}

// The batch size caps a single claim independently of capacity, so a node with
// a large pool still issues bounded queries.
func TestDispatcher_BatchSizeCapsTheClaim(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dispatcher, _ := dispatcherFixture(t, pool, 20, 20, engine.WithBatchSize(4))

	claimed, err := dispatcher.ClaimOnce(ctx)
	if err != nil {
		t.Fatalf("ClaimOnce failed: %v", err)
	}
	if claimed != 4 {
		t.Errorf("claimed %d tasks, want the batch size of 4", claimed)
	}
}

// A handler that refuses the work leaves the tasks leased rather than resetting
// them: the lease expires and the reaper returns them, which is the same path a
// crash between claim and start takes.
func TestDispatcher_HandlerErrorLeavesTasksLeased(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dispatcher, handler := dispatcherFixture(t, pool, 3, 3)
	handler.err = errors.New("pool is shutting down")

	_, err := dispatcher.ClaimOnce(ctx)
	if err == nil {
		t.Fatal("expected ClaimOnce to report the handler's error")
	}

	var running, pending int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE status = 'running'),
		        count(*) FILTER (WHERE status = 'pending')
		   FROM tasks`).Scan(&running, &pending); err != nil {
		t.Fatalf("failed to count tasks: %v", err)
	}
	if running != 3 {
		t.Errorf("running = %d, want the 3 claimed tasks to stay leased", running)
	}
	if pending != 0 {
		t.Errorf("pending = %d, want 0 — nothing is rolled back on handler error", pending)
	}
}

// Run keeps claiming while a pass comes back full, so a node with free capacity
// and a full queue does not wait out a poll interval between batches.
func TestDispatcher_RunDrainsTheQueue(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dispatcher, handler := dispatcherFixture(t, pool, 5, 40,
		engine.WithPollInterval(50*time.Millisecond))

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- dispatcher.Run(runCtx) }()

	deadline := time.After(10 * time.Second)
	for handler.count() < 40 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("dispatcher handled %d of 40 tasks before timing out", handler.count())
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

	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status = 'pending'`).Scan(&pending); err != nil {
		t.Fatalf("failed to count pending: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending = %d, want the queue drained", pending)
	}
}

// Work submitted after the dispatcher is already running gets picked up on a
// later tick — the poll floor covering for a notification that has not been
// wired up yet.
func TestDispatcher_PicksUpLateArrivals(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dispatcher, handler := dispatcherFixture(t, pool, 4, 0,
		engine.WithPollInterval(50*time.Millisecond))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = dispatcher.Run(runCtx) }()

	// Nothing to do yet.
	time.Sleep(150 * time.Millisecond)
	if handler.count() != 0 {
		t.Fatalf("handler received %d tasks before any were submitted", handler.count())
	}

	store := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	workflow, err := store.WorkflowStore.GetWorkflowByName(ctx, "test-workflow")
	if err != nil {
		t.Fatalf("GetWorkflowByName failed: %v", err)
	}
	if err := store.TaskStore.BulkInsertTask(ctx, []*models.TaskRow{
		dbtest.NewTaskRow(workflow.ID, "late-1", "research-agent"),
		dbtest.NewTaskRow(workflow.ID, "late-2", "research-agent"),
	}); err != nil {
		t.Fatalf("failed to insert late tasks: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for handler.count() < 2 {
		select {
		case <-deadline:
			t.Fatalf("dispatcher handled %d of 2 late tasks before timing out", handler.count())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestDispatcher_RunWithoutEngineID(t *testing.T) {
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	dispatcher := engine.NewDispatcher(
		taskStore(pool),
		uuid.Nil,
		engine.StaticCapacity(1),
		engine.HandlerFunc(func(context.Context, []*models.TaskRow) error { return nil }),
		fastEngineConfigWithCapacity(1),
		nopLogger(),
	)

	if err := dispatcher.Run(context.Background()); err == nil {
		t.Fatal("expected Run with no engine id to fail")
	}
}

// Two dispatchers against one database split the queue and never hand the same
// task to both handlers — the claim's guarantee, now observed one level up.
func TestDispatcher_TwoNodesSplitTheQueue(t *testing.T) {
	const taskCount = 60

	ctx := context.Background()
	setup := dbtest.Pool(t)
	dbtest.SeedAgents(t, setup, "research-agent")

	store := repositories.NewStore(repositories.NewPostgresRepository(setup, nopLogger(), nil))
	workflow, err := store.WorkflowStore.CreateWorkflow(ctx, dbtest.NewWorkflowRow())
	if err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	tasks := make([]*models.TaskRow, 0, taskCount)
	for i := range taskCount {
		tasks = append(tasks, dbtest.NewTaskRow(workflow.ID, "task-"+strconv.Itoa(i), "research-agent"))
	}
	if err := store.TaskStore.BulkInsertTask(ctx, tasks); err != nil {
		t.Fatalf("failed to seed tasks: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	handlers := make([]*collector, 2)
	for i := range handlers {
		pool, err := pgxpool.New(ctx, dbtest.DSN(t))
		if err != nil {
			t.Fatalf("failed to open a pool: %v", err)
		}
		t.Cleanup(pool.Close)

		engineRow := models.NewEngineRow("node-"+strconv.Itoa(i), 4)
		registered, err := store.EngineStore.Register(ctx, &engineRow)
		if err != nil {
			t.Fatalf("failed to register engine: %v", err)
		}

		// Throttled so neither node can drain the whole queue before the other
		// has started; without it this test is decided by which goroutine the
		// scheduler runs first, and the no-double-handling assertion below
		// becomes vacuous whenever one node wins the race.
		handlers[i] = &collector{delay: 15 * time.Millisecond}
		dispatcher := engine.NewDispatcher(
			taskStore(pool),
			registered.ID,
			engine.StaticCapacity(4),
			handlers[i],
			fastEngineConfigWithCapacity(4),
			nopLogger(),
			engine.WithPollInterval(20*time.Millisecond),
		)
		go func() { _ = dispatcher.Run(runCtx) }()
	}

	deadline := time.After(30 * time.Second)
	for handlers[0].count()+handlers[1].count() < taskCount {
		select {
		case <-deadline:
			t.Fatalf("handled %d of %d tasks before timing out",
				handlers[0].count()+handlers[1].count(), taskCount)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()

	seen := map[uuid.UUID]bool{}
	for _, handler := range handlers {
		handler.mu.Lock()
		for _, task := range handler.tasks {
			if seen[task.ID] {
				t.Errorf("task %s was handed to both nodes", task.ID)
			}
			seen[task.ID] = true
		}
		handler.mu.Unlock()
	}
	if len(seen) != taskCount {
		t.Errorf("%d distinct tasks handled, want %d", len(seen), taskCount)
	}

	// Both nodes actually participated. Without this the no-double-handling
	// assertion above would pass trivially whenever one node happened to drain
	// the queue alone, so this is what keeps the test honest rather than a
	// fairness requirement of the scheduler.
	if handlers[0].count() == 0 || handlers[1].count() == 0 {
		t.Errorf("one node did all the work: %d and %d",
			handlers[0].count(), handlers[1].count())
	}
}
