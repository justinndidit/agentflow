//go:build integration

package engine_test

import (
	"context"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/justinndidit/agentflow/internal/blob"
	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/justinndidit/agentflow/internal/state"
)

// echoAgentImage is the reference agent from examples/echo-agent: it reads the
// resolved input from stdin and writes it back on stdout, which is the whole
// worker contract and nothing else.
const echoAgentImage = "agentflow/echo-agent:test"

// buildEchoAgent builds the reference image once per test run.
func buildEchoAgent(t *testing.T) {
	t.Helper()

	cmd := exec.Command("docker", "build", "-q", "-t", echoAgentImage, "examples/echo-agent")
	cmd.Dir = dbtest.RepoRoot()

	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build the reference agent image: %v\n%s", err, output)
	}
}

// registerAgent points an agent name at an image.
func registerAgent(t *testing.T, f *commitFixture, name, image string) {
	t.Helper()

	_, err := f.pool.Exec(context.Background(),
		`INSERT INTO agents (id, name, agent_image) VALUES (gen_random_uuid(), $1, $2)
		 ON CONFLICT (name) DO UPDATE SET agent_image = EXCLUDED.agent_image`,
		name, image)
	if err != nil {
		t.Fatalf("failed to register agent %s: %v", name, err)
	}
}

// The milestone's point: a workflow completes with every task executed as a real
// container, not a simulated one.
func TestDockerRuntime_RunsAWorkflowInContainers(t *testing.T) {
	ctx := context.Background()
	buildEchoAgent(t)

	f := newCommitFixture(t, map[string][]string{
		"first":  nil,
		"second": {"first"},
		"third":  {"second"},
	}, noBackoff)
	registerAgent(t, f, "research-agent", echoAgentImage)

	docker, err := runtime.NewDocker(nopLogger())
	if err != nil {
		t.Fatalf("failed to connect to Docker: %v", err)
	}
	t.Cleanup(func() { _ = docker.Close() })

	cfg := nodeConfig(t, 3)
	node := engine.NewNode(cfg, f.pool, docker, blob.Disabled{}, nopLogger())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = node.Run(runCtx) }()

	waitForWorkflow(t, f.pool, f.workflow.ID, string(state.CompletedWorkflowStatus), 120*time.Second)

	// The stored output is what the container actually printed, so a passing
	// run proves the whole path: image resolved, container started, input
	// delivered on stdin, stdout parsed, result committed.
	for _, key := range []string{"first", "second", "third"} {
		task := f.task(t, key)
		if task.Status != string(state.CompletedTaskStatus) {
			t.Errorf("task %s ended %q, want completed", key, task.Status)
		}

		results, err := f.stores.TaskResultStore.ListByTask(ctx, task.ID)
		if err != nil {
			t.Fatalf("ListByTask failed: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("task %s has %d results, want 1", key, len(results))
		}
		if len(results[0].Output) == 0 {
			t.Errorf("task %s produced no output", key)
		}
	}
}

// An agent whose image does not exist fails its tasks rather than the engine.
func TestDockerRuntime_UnknownImageFailsTheTask(t *testing.T) {
	ctx := context.Background()

	f := newCommitFixture(t, map[string][]string{"doomed": nil}, noBackoff)
	registerAgent(t, f, "research-agent",
		"agentflow/definitely-not-published:"+strconv.FormatInt(time.Now().UnixNano(), 36))

	if _, err := f.pool.Exec(ctx, `UPDATE tasks SET max_retries = 0`); err != nil {
		t.Fatalf("failed to set the retry budget: %v", err)
	}

	docker, err := runtime.NewDocker(nopLogger(), runtime.WithPullPolicy(runtime.PullNever))
	if err != nil {
		t.Fatalf("failed to connect to Docker: %v", err)
	}
	t.Cleanup(func() { _ = docker.Close() })

	node := engine.NewNode(nodeConfig(t, 2), f.pool, docker, blob.Disabled{}, nopLogger())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = node.Run(runCtx) }()

	waitForWorkflow(t, f.pool, f.workflow.ID, string(state.FailedWorkflowStatus), 60*time.Second)

	task := f.task(t, "doomed")
	if task.ErrorMessage == nil {
		t.Fatal("no error message recorded for a task whose image is missing")
	}
}

// An agent name with no row in agents fails the task with a clear reason rather
// than crashing the node.
func TestDockerRuntime_UnregisteredAgentFailsTheTask(t *testing.T) {
	ctx := context.Background()

	f := newCommitFixture(t, map[string][]string{"orphan": nil}, noBackoff)
	// The task's agent exists (the FK requires it), but the image is blank.
	registerAgent(t, f, "research-agent", "")

	if _, err := f.pool.Exec(ctx, `UPDATE tasks SET max_retries = 0`); err != nil {
		t.Fatalf("failed to set the retry budget: %v", err)
	}

	docker, err := runtime.NewDocker(nopLogger(), runtime.WithPullPolicy(runtime.PullNever))
	if err != nil {
		t.Fatalf("failed to connect to Docker: %v", err)
	}
	t.Cleanup(func() { _ = docker.Close() })

	node := engine.NewNode(nodeConfig(t, 2), f.pool, docker, blob.Disabled{}, nopLogger())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = node.Run(runCtx) }()

	waitForWorkflow(t, f.pool, f.workflow.ID, string(state.FailedWorkflowStatus), 60*time.Second)

	task := f.task(t, "orphan")
	if task.ErrorMessage == nil {
		t.Fatal("no error message recorded for an agent with no image")
	}
}
