//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/justinndidit/agentflow/internal/blob"
	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/justinndidit/agentflow/internal/state"
)

var errTest = errors.New("simulated failure")

// runUpstream claims a task and commits the given output for it, so a
// downstream task has something real to resolve against.
func runUpstream(t *testing.T, f *commitFixture, key, output string) {
	t.Helper()

	task := f.claim(t)[key]
	if task == nil {
		t.Fatalf("%s was not claimable", key)
	}
	err := f.committer.Commit(context.Background(), repositories.FenceFor(task), engine.Outcome{
		Output: []byte(output),
	})
	if err != nil {
		t.Fatalf("failed to commit %s: %v", key, err)
	}
}

// setTemplate rewrites a task's stored input_template.
func setTemplate(t *testing.T, f *commitFixture, key, template string) {
	t.Helper()

	_, err := f.pool.Exec(context.Background(),
		`UPDATE tasks SET input_template = $2::jsonb WHERE workflow_id = $1 AND task_key = $3`,
		f.workflow.ID, template, key)
	if err != nil {
		t.Fatalf("failed to set the template for %s: %v", key, err)
	}
}

func TestTemplateResolver_SubstitutesUpstreamOutput(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"fetch": nil,
		"rank":  {"fetch"},
	}, noBackoff)

	setTemplate(t, f, "rank", `{"jobs":"{{ tasks.fetch.output.jobs }}","note":"literal"}`)
	runUpstream(t, f, "fetch", `{"jobs":["backend","platform"],"count":2}`)

	rank := f.claim(t)["rank"]
	if rank == nil {
		t.Fatal("rank was not claimable after its dependency finished")
	}

	resolver := engine.NewTemplateResolver(f.stores.TaskResultStore)
	resolved, err := resolver.Resolve(ctx, rank)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(resolved, &got); err != nil {
		t.Fatalf("resolved input is not valid JSON: %v", err)
	}

	// The array arrives as an array, not as a string containing one.
	jobs, ok := got["jobs"].([]any)
	if !ok {
		t.Fatalf("jobs = %T, want a list", got["jobs"])
	}
	if len(jobs) != 2 || jobs[0] != "backend" {
		t.Errorf("jobs = %v, want the upstream list", jobs)
	}
	if got["note"] != "literal" {
		t.Errorf("note = %v, want the literal preserved", got["note"])
	}
}

// A task with no references never touches the database, and gets its stored
// template back byte for byte.
func TestTemplateResolver_NoReferencesSkipsTheDatabase(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"plain": nil}, noBackoff)

	task := f.claim(t)["plain"]
	resolver := engine.NewTemplateResolver(f.stores.TaskResultStore)

	resolved, err := resolver.Resolve(ctx, task)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if string(resolved) != string(task.InputTemplate) {
		t.Errorf("resolved = %s, want the template verbatim", resolved)
	}
}

// The output used is the one from the attempt that actually completed, not the
// newest row for that task. A task that failed twice and then succeeded has
// three result rows, and only the third describes what its dependents get.
func TestTemplateResolver_UsesTheCompletedAttempt(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"flaky": nil,
		"after": {"flaky"},
	}, noBackoff)

	setTemplate(t, f, "after", `{"value":"{{ tasks.flaky.output.value }}"}`)

	// Two failed attempts, each recording its own output.
	for attempt := 1; attempt <= 2; attempt++ {
		task := f.claim(t)["flaky"]
		if task == nil {
			t.Fatalf("flaky was not claimable on attempt %d", attempt)
		}
		err := f.committer.Commit(ctx, repositories.FenceFor(task), engine.Outcome{
			Output: []byte(`{"value":"from-a-failed-attempt"}`),
			Err:    errTest,
		})
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// Then a success.
	runUpstream(t, f, "flaky", `{"value":"from-the-successful-attempt"}`)

	after := f.claim(t)["after"]
	if after == nil {
		t.Fatal("after was not claimable")
	}

	resolved, err := engine.NewTemplateResolver(f.stores.TaskResultStore).Resolve(ctx, after)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if !strings.Contains(string(resolved), "from-the-successful-attempt") {
		t.Errorf("resolved = %s, want the completed attempt's output", resolved)
	}
}

// depends_on holds task keys, unique only within a workflow, so resolution must
// never reach into another run's results.
func TestTemplateResolver_ScopedToItsWorkflow(t *testing.T) {
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

	// Two runs of the same shape, with different upstream outputs.
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
		downstream.InputTemplate = []byte(`{"value":"{{ tasks.fetch.output.value }}"}`)

		if err := stores.TaskStore.BulkInsertTask(ctx, []*models.TaskRow{upstream, downstream}); err != nil {
			t.Fatalf("failed to seed workflow %d: %v", i, err)
		}
	}

	claimed, err := stores.TaskStore.ClaimTasks(ctx, registered.ID, 100, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}
	for _, task := range claimed {
		value := "second-run"
		if task.WorkflowID == workflows[0].ID {
			value = "first-run"
		}
		err := committer.Commit(ctx, repositories.FenceFor(task), engine.Outcome{
			Output: []byte(`{"value":"` + value + `"}`),
		})
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// Resolve the second run's downstream task; it must see its own upstream.
	ready, err := stores.TaskStore.ClaimTasks(ctx, registered.ID, 100, testLeaseTTL)
	if err != nil {
		t.Fatalf("ClaimTasks failed: %v", err)
	}

	resolver := engine.NewTemplateResolver(stores.TaskResultStore)
	for _, task := range ready {
		resolved, err := resolver.Resolve(ctx, task)
		if err != nil {
			t.Fatalf("Resolve failed: %v", err)
		}

		want := "second-run"
		if task.WorkflowID == workflows[0].ID {
			want = "first-run"
		}
		if !strings.Contains(string(resolved), want) {
			t.Errorf("workflow %s resolved to %s, want %q — resolution crossed workflows",
				task.WorkflowID, resolved, want)
		}
	}
}

// A reference to a shape the upstream did not produce fails the task rather
// than silently substituting a zero value.
func TestPool_ResolutionFailureFailsTheTask(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"fetch": nil,
		"rank":  {"fetch"},
	}, noBackoff)

	// The manifest expects a key the upstream never produces.
	setTemplate(t, f, "rank", `{"jobs":"{{ tasks.fetch.output.jobs }}"}`)
	runUpstream(t, f, "fetch", `{"something_else":1}`)

	rank := f.claim(t)["rank"]
	if rank == nil {
		t.Fatal("rank was not claimable")
	}

	// A runtime that would succeed if it ever ran, so a pass here can only mean
	// resolution failed the task before execution.
	executed := &countingRuntime{inner: runtime.NewEcho(0)}
	pool := engine.NewPool(2, executed,
		engine.NewCommitter(repositories.NewTxManager(f.pool, nopLogger()), nopLogger(), noBackoff),
		engine.NewTemplateResolver(f.stores.TaskResultStore),
		engine.NewCachedAgents(f.stores.AgentStore),
		blob.Disabled{}, testLeaseTTL, nopLogger())

	if err := pool.Handle(ctx, []*models.TaskRow{rank}); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if executed.count() != 0 {
		t.Errorf("the runtime ran %d times; a task whose input cannot be resolved must not execute",
			executed.count())
	}

	task := f.task(t, "rank")
	if task.Status != string(state.PendingTaskStatus) {
		t.Errorf("Status = %q, want pending for a retry", task.Status)
	}
	if task.ErrorMessage == nil || !strings.Contains(*task.ErrorMessage, "jobs") {
		t.Errorf("ErrorMessage = %v, want it to name the missing field", task.ErrorMessage)
	}
}

// The resolved input is recorded, not the template. When a failed attempt has
// to be explained after the fact, what the worker actually received is the only
// useful record.
func TestPool_RecordsTheResolvedInput(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{
		"fetch": nil,
		"rank":  {"fetch"},
	}, noBackoff)

	setTemplate(t, f, "rank", `{"jobs":"{{ tasks.fetch.output.jobs }}"}`)
	runUpstream(t, f, "fetch", `{"jobs":["backend"]}`)

	rank := f.claim(t)["rank"]
	pool := engine.NewPool(2, runtime.NewEcho(0),
		engine.NewCommitter(repositories.NewTxManager(f.pool, nopLogger()), nopLogger(), noBackoff),
		engine.NewTemplateResolver(f.stores.TaskResultStore),
		engine.NewCachedAgents(f.stores.AgentStore),
		blob.Disabled{}, testLeaseTTL, nopLogger())

	if err := pool.Handle(ctx, []*models.TaskRow{rank}); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	result, err := f.stores.TaskResultStore.GetByAttempt(ctx, rank.ID, rank.Attempt)
	if err != nil {
		t.Fatalf("GetByAttempt failed: %v", err)
	}

	if strings.Contains(string(result.ResolvedInput), "{{") {
		t.Errorf("resolved_input = %s, want the substituted value rather than the template",
			result.ResolvedInput)
	}
	if !strings.Contains(string(result.ResolvedInput), "backend") {
		t.Errorf("resolved_input = %s, want the upstream value", result.ResolvedInput)
	}

	// And the worker saw the same thing, since echo returns its input.
	if !strings.Contains(string(result.Output), "backend") {
		t.Errorf("output = %s, want the worker to have received the resolved input", result.Output)
	}
}

// End to end through a running node: a downstream task consumes what its
// upstream produced.
func TestNode_ResolvesTemplatesBetweenTasks(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{
		"fetch": nil,
		"rank":  {"fetch"},
	})

	_, err := pool.Exec(ctx,
		`UPDATE tasks SET input_template = '{"seen":"{{ tasks.fetch.output.role }}"}'::jsonb
		  WHERE workflow_id = $1 AND task_key = 'rank'`, workflow.ID)
	if err != nil {
		t.Fatalf("failed to set the template: %v", err)
	}

	rt := &countingRuntime{inner: runtime.NewEcho(0)}
	node := engine.NewNode(nodeConfig(t, 4), pool, rt, blob.Disabled{}, nopLogger())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = node.Run(runCtx) }()

	waitForWorkflow(t, pool, workflow.ID, string(state.CompletedWorkflowStatus), 30*time.Second)

	rt.mu.Lock()
	defer rt.mu.Unlock()

	var rankInput string
	for _, req := range rt.requests {
		if req.TaskKey == "rank" {
			rankInput = string(req.Input)
		}
	}
	if rankInput == "" {
		t.Fatal("rank never ran")
	}
	// dbtest.NewTaskRow's template is {"role":"engineer"}, which fetch echoes,
	// so rank should see "engineer" substituted in.
	if !strings.Contains(rankInput, "engineer") {
		t.Errorf("rank received %s, want the upstream value substituted", rankInput)
	}
	if strings.Contains(rankInput, "{{") {
		t.Errorf("rank received %s, want the template resolved", rankInput)
	}
}
