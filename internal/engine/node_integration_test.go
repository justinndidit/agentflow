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

	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/justinndidit/agentflow/internal/state"
)

// countingRuntime records every task it is asked to run, so tests can assert on
// how many times work actually executed rather than only on final state.
type countingRuntime struct {
	inner runtime.Runtime

	mu       sync.Mutex
	requests []runtime.Request
}

func (c *countingRuntime) Name() string { return "counting/" + c.inner.Name() }

func (c *countingRuntime) Execute(ctx context.Context, req runtime.Request) (*runtime.Response, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	c.mu.Unlock()
	return c.inner.Execute(ctx, req)
}

func (c *countingRuntime) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

func (c *countingRuntime) keys() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	counts := map[string]int{}
	for _, req := range c.requests {
		counts[req.TaskKey]++
	}
	return counts
}

// nodeConfig points a node at the test database with a tight heartbeat so
// shutdown and reaping happen in test time.
func nodeConfig(t *testing.T, capacity int) *config.Config {
	t.Helper()

	cfg := dbtest.Config(t)
	cfg.Engine = &config.Engine{
		Capacity:          capacity,
		HeartbeatInterval: 1,
		LeaseTTL:          30,
		PollInterval:      1,
		ReapInterval:      1,
	}
	return cfg
}

// seedGraph creates a workflow with the given shape and returns it.
func seedGraph(t *testing.T, pool *pgxpool.Pool, graph map[string][]string) *models.WorkflowRow {
	t.Helper()

	ctx := context.Background()
	dbtest.SeedAgents(t, pool, "research-agent")
	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))

	row := dbtest.NewWorkflowRow()
	row.TaskCount = len(graph)
	workflow, err := stores.WorkflowStore.CreateWorkflow(ctx, row)
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
	if err := stores.TaskStore.BulkInsertTask(ctx, tasks); err != nil {
		t.Fatalf("failed to seed tasks: %v", err)
	}
	return workflow
}

func waitForWorkflow(t *testing.T, pool *pgxpool.Pool, workflowID uuid.UUID, want string, within time.Duration) *models.WorkflowRow {
	t.Helper()

	ctx := context.Background()
	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	deadline := time.After(within)

	for {
		workflow, err := stores.WorkflowStore.GetWorkflowByID(ctx, workflowID)
		if err != nil {
			t.Fatalf("GetWorkflowByID failed: %v", err)
		}
		if workflow.Status == want {
			return workflow
		}

		select {
		case <-deadline:
			t.Fatalf("workflow status is %q after %s, want %q "+
				"(completed=%d failed=%d cancelled=%d of %d)",
				workflow.Status, within, want,
				workflow.TaskCompleted, workflow.TaskFailed,
				workflow.TaskCancelled, workflow.TaskCount)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// The milestone's done-when: a node picks up a submitted workflow and runs it
// to completion with no containers involved.
func TestNode_RunsAWorkflowToCompletion(t *testing.T) {
	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{
		"fetch":  nil,
		"rank":   {"fetch"},
		"score":  {"fetch"},
		"report": {"rank", "score"},
	})

	rt := &countingRuntime{inner: runtime.NewEcho(0)}
	node := engine.NewNode(nodeConfig(t, 4), pool, rt, nopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- node.Run(ctx) }()

	completed := waitForWorkflow(t, pool, workflow.ID, string(state.CompletedWorkflowStatus), 30*time.Second)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("node.Run returned %v, want nil on cancellation", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("node.Run did not return after cancellation")
	}

	if completed.TaskCompleted != 4 {
		t.Errorf("task_completed = %d, want 4", completed.TaskCompleted)
	}
	// Every task ran exactly once: nothing was reclaimed and redone.
	for key, count := range rt.keys() {
		if count != 1 {
			t.Errorf("task %s executed %d times, want 1", key, count)
		}
	}
	if rt.count() != 4 {
		t.Errorf("runtime executed %d tasks, want 4", rt.count())
	}
	// Cost accounting accumulated from the workers rather than being invented.
	if completed.TokensUsed != 400 {
		t.Errorf("tokens_used = %d, want 400", completed.TokensUsed)
	}
}

// Dependencies are honoured end to end: a task cannot start before everything
// it depends on has committed.
func TestNode_RespectsDependencyOrder(t *testing.T) {
	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{
		"first":  nil,
		"second": {"first"},
		"third":  {"second"},
	})

	rt := &countingRuntime{inner: runtime.NewEcho(0)}
	node := engine.NewNode(nodeConfig(t, 4), pool, rt, nopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = node.Run(ctx) }()

	waitForWorkflow(t, pool, workflow.ID, string(state.CompletedWorkflowStatus), 30*time.Second)

	rt.mu.Lock()
	order := make([]string, 0, len(rt.requests))
	for _, req := range rt.requests {
		order = append(order, req.TaskKey)
	}
	rt.mu.Unlock()

	want := []string{"first", "second", "third"}
	if len(order) != len(want) {
		t.Fatalf("executed %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("executed %v, want %v", order, want)
		}
	}
}

// A failing task exhausts its retries, fails permanently, and takes its
// dependents with it — while an independent branch still finishes.
func TestNode_FailureCascadesAndIndependentBranchCompletes(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{
		"doomed":      nil,
		"child":       {"doomed"},
		"independent": nil,
	})

	// One attempt, no retries, so the run finishes quickly.
	if _, err := pool.Exec(ctx, `UPDATE tasks SET max_retries = 0`); err != nil {
		t.Fatalf("failed to set the retry budget: %v", err)
	}

	echo := runtime.NewEcho(0)
	echo.FailKeys = map[string]bool{"doomed": true}
	node := engine.NewNode(nodeConfig(t, 4), pool, echo, nopLogger())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = node.Run(runCtx) }()

	final := waitForWorkflow(t, pool, workflow.ID, string(state.FailedWorkflowStatus), 30*time.Second)

	if final.TaskFailed != 1 {
		t.Errorf("task_failed = %d, want 1", final.TaskFailed)
	}
	if final.TaskCancelled != 1 {
		t.Errorf("task_cancelled = %d, want 1", final.TaskCancelled)
	}
	if final.TaskCompleted != 1 {
		t.Errorf("task_completed = %d, want the independent branch to have finished", final.TaskCompleted)
	}
}

// A node never runs more work at once than its capacity allows.
func TestNode_NeverExceedsCapacity(t *testing.T) {
	const capacity = 3

	pool := dbtest.Pool(t)
	graph := map[string][]string{}
	for i := range 20 {
		graph["task-"+strconv.Itoa(i)] = nil
	}
	workflow := seedGraph(t, pool, graph)

	tracker := &concurrencyTracker{inner: runtime.NewEcho(30 * time.Millisecond)}
	node := engine.NewNode(nodeConfig(t, capacity), pool, tracker, nopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = node.Run(ctx) }()

	waitForWorkflow(t, pool, workflow.ID, string(state.CompletedWorkflowStatus), 60*time.Second)

	if peak := tracker.peak(); peak > capacity {
		t.Errorf("peak concurrency was %d, want no more than the capacity of %d", peak, capacity)
	}
	if peak := tracker.peak(); peak < 2 {
		t.Errorf("peak concurrency was %d; the pool is not running work in parallel", peak)
	}
}

// concurrencyTracker records the high-water mark of simultaneous executions.
type concurrencyTracker struct {
	inner runtime.Runtime

	mu      sync.Mutex
	current int
	highest int
}

func (c *concurrencyTracker) Name() string { return "tracker" }

func (c *concurrencyTracker) Execute(ctx context.Context, req runtime.Request) (*runtime.Response, error) {
	c.mu.Lock()
	c.current++
	if c.current > c.highest {
		c.highest = c.current
	}
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.current--
		c.mu.Unlock()
	}()

	return c.inner.Execute(ctx, req)
}

func (c *concurrencyTracker) peak() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.highest
}

// Two nodes against one database split a workflow and finish it together, with
// every task running exactly once.
func TestNode_TwoNodesShareAWorkflow(t *testing.T) {
	const taskCount = 30

	ctx := context.Background()
	setup := dbtest.Pool(t)

	graph := map[string][]string{}
	for i := range taskCount {
		graph["task-"+strconv.Itoa(i)] = nil
	}
	workflow := seedGraph(t, setup, graph)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	runtimes := make([]*countingRuntime, 2)
	for i := range runtimes {
		pool, err := pgxpool.New(ctx, dbtest.DSN(t))
		if err != nil {
			t.Fatalf("failed to open a pool: %v", err)
		}
		t.Cleanup(pool.Close)

		runtimes[i] = &countingRuntime{inner: runtime.NewEcho(10 * time.Millisecond)}
		node := engine.NewNode(nodeConfig(t, 4), pool, runtimes[i], nopLogger())
		go func() { _ = node.Run(runCtx) }()
	}

	waitForWorkflow(t, setup, workflow.ID, string(state.CompletedWorkflowStatus), 60*time.Second)

	executions := map[string]int{}
	for _, rt := range runtimes {
		for key, count := range rt.keys() {
			executions[key] += count
		}
	}

	if len(executions) != taskCount {
		t.Errorf("%d distinct tasks executed, want %d", len(executions), taskCount)
	}
	for key, count := range executions {
		if count != 1 {
			t.Errorf("task %s executed %d times across the fleet, want 1", key, count)
		}
	}
	if runtimes[0].count() == 0 || runtimes[1].count() == 0 {
		t.Errorf("one node did all the work: %d and %d", runtimes[0].count(), runtimes[1].count())
	}
}

// Graceful shutdown lets in-flight work finish and leaves the node stopped.
func TestNode_GracefulShutdownFinishesInFlightWork(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{"slow": nil})

	rt := &countingRuntime{inner: runtime.NewEcho(700 * time.Millisecond)}
	node := engine.NewNode(nodeConfig(t, 2), pool, rt, nopLogger())

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- node.Run(runCtx) }()

	// Wait until the task is actually running, then interrupt mid-flight.
	deadline := time.After(15 * time.Second)
	for rt.count() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("the node never started the task")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("node.Run returned %v, want a clean shutdown", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("node.Run did not return")
	}

	// The in-flight task was allowed to finish rather than abandoned.
	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	final, err := stores.WorkflowStore.GetWorkflowByID(ctx, workflow.ID)
	if err != nil {
		t.Fatalf("GetWorkflowByID failed: %v", err)
	}
	if final.TaskCompleted != 1 {
		t.Errorf("task_completed = %d, want the in-flight task to have finished during drain", final.TaskCompleted)
	}

	var stopped int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM engines WHERE status = 'stopped'`).Scan(&stopped)
	if err != nil {
		t.Fatalf("failed to count stopped engines: %v", err)
	}
	if stopped != 1 {
		t.Errorf("stopped engines = %d, want the node to have released itself", stopped)
	}
}
