//go:build integration

package runtime_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/rs/zerolog"

	"github.com/justinndidit/agentflow/internal/runtime"
)

// testImage is present on any machine that can run the rest of this suite and
// is small enough that a pull costs nothing if it is not.
const testImage = "alpine:latest"

func nopLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

func newDocker(t *testing.T, opts ...runtime.DockerOption) *runtime.Docker {
	t.Helper()

	docker, err := runtime.NewDocker(nopLogger(), opts...)
	if err != nil {
		t.Fatalf("failed to connect to Docker: %v", err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	return docker
}

// request builds a task whose container runs the given shell command.
func request(command string) runtime.Request {
	return runtime.Request{
		TaskID:         uuid.New(),
		WorkflowID:     uuid.New(),
		TaskKey:        "test-task",
		AgentName:      "test-agent",
		AgentImage:     testImage,
		Attempt:        1,
		IdempotencyKey: "stable-key",
		Input:          []byte(`{"role":"engineer"}`),
		Command:        []string{"sh", "-c", command},
	}
}

func TestDocker_RunsAContainerAndReadsStdout(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	response, err := docker.Execute(ctx, request(`echo '{"jobs":["backend"]}'`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(response.Output) != `{"jobs":["backend"]}` {
		t.Errorf("Output = %s, want the container's stdout", response.Output)
	}
}

// The resolved input arrives on stdin, and stdin is closed so a worker reading
// to EOF is not left waiting for the deadline.
func TestDocker_DeliversInputOnStdin(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	response, err := docker.Execute(ctx, request(`cat`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(response.Output) != `{"role":"engineer"}` {
		t.Errorf("Output = %s, want the input echoed from stdin", response.Output)
	}
}

// The worker contract's environment variables, from the architecture doc.
func TestDocker_SuppliesTheContractEnvironment(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := request(`printf '{"task":"%s","attempt":"%s","key":"%s","workflow":"%s"}' ` +
		`"$AGENTFLOW_TASK_KEY" "$AGENTFLOW_ATTEMPT" "$AGENTFLOW_IDEMPOTENCY_KEY" "$AGENTFLOW_WORKFLOW_ID"`)
	req.Attempt = 3

	response, err := docker.Execute(ctx, req)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	output := string(response.Output)
	for _, want := range []string{
		`"task":"test-task"`,
		`"attempt":"3"`,
		`"key":"stable-key"`,
		`"workflow":"` + req.WorkflowID.String() + `"`,
	} {
		if !strings.Contains(output, want) {
			t.Errorf("output %s is missing %s", output, want)
		}
	}
}

// Non-zero exit is a failed attempt, and stderr becomes the reason. Without
// that, a failure is reported with no way to tell why.
func TestDocker_NonZeroExitFailsWithStderr(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := docker.Execute(ctx, request(`echo "model provider refused" >&2; exit 3`))
	if err == nil {
		t.Fatal("expected a non-zero exit to fail the attempt")
	}
	if !strings.Contains(err.Error(), "exited 3") {
		t.Errorf("error = %q, want it to report the exit code", err)
	}
	if !strings.Contains(err.Error(), "model provider refused") {
		t.Errorf("error = %q, want it to carry the container's stderr", err)
	}
}

// Docker frames stdout and stderr into one multiplexed stream. Reading it raw
// would interleave stderr into the JSON and corrupt every result.
func TestDocker_SeparatesStdoutFromStderr(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	response, err := docker.Execute(ctx,
		request(`echo "progress: 50%" >&2; echo '{"ok":true}'; echo "done" >&2`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(response.Output) != `{"ok":true}` {
		t.Errorf("Output = %s; stderr leaked into stdout", response.Output)
	}
}

// A worker that exits cleanly having written nothing is an empty result, not a
// failure: not every agent produces output.
func TestDocker_EmptyStdoutIsAnEmptyResult(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	response, err := docker.Execute(ctx, request(`exit 0`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(response.Output) != `{}` {
		t.Errorf("Output = %s, want an empty object", response.Output)
	}
}

// Output goes straight into a JSONB column, so malformed JSON has to fail the
// task rather than the commit — otherwise a worker bug is reported as an engine
// problem.
func TestDocker_InvalidJSONFailsTheTask(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := docker.Execute(ctx, request(`echo "not json at all"`))
	if err == nil {
		t.Fatal("expected invalid JSON to fail the attempt")
	}
	if !strings.Contains(err.Error(), "invalid JSON") {
		t.Errorf("error = %q, want it to name the problem", err)
	}
}

// The attempt is bounded by its context. Work that outlived the deadline would
// outlive the lease protecting it, and the same task would run twice.
func TestDocker_RespectsTheDeadline(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := time.Now()
	_, err := docker.Execute(ctx, request(`sleep 300`))
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("expected the deadline to fail the attempt")
	}
	if elapsed > 30*time.Second {
		t.Errorf("Execute took %s; the container outlived its deadline", elapsed)
	}
}

// A node claims continuously, so a container leaked per attempt fills the
// host's disk in hours. Every path has to clean up, including the failures.
//
// Counted by label rather than in total: the rest of the suite is starting and
// stopping its own containers on the same daemon, so a global count is a number
// other tests move underneath this one.
func TestDocker_RemovesContainers(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	workflowID := uuid.New()
	attempt := func(command string) {
		req := request(command)
		req.WorkflowID = workflowID
		_, _ = docker.Execute(ctx, req)
	}

	// Success, non-zero exit, and bad output: three different exit paths.
	attempt(`echo '{"ok":true}'`)
	attempt(`exit 7`)
	attempt(`echo nonsense`)

	if leaked := countLabelled(t, runtime.LabelWorkflowID, workflowID.String()); leaked != 0 {
		t.Errorf("%d containers survived their attempts; they are leaking", leaked)
	}
}

// Labels let an operator on a busy node tell which task a container belongs to.
func TestDocker_LabelsContainers(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The container has to still exist to be inspected, so it blocks until the
	// deadline and is then torn down like any other timed-out attempt.
	req := request(`sleep 30`)
	shortCtx, shortCancel := context.WithTimeout(ctx, 3*time.Second)
	defer shortCancel()

	found := make(chan int, 1)
	go func() {
		time.Sleep(time.Second)
		found <- countLabelled(t, runtime.LabelTaskID, req.TaskID.String())
	}()

	_, _ = docker.Execute(shortCtx, req)

	if got := <-found; got != 1 {
		t.Errorf("found %d containers labelled with the task id, want 1", got)
	}
}

// The engine never mounts the Docker socket. A worker that could reach it could
// start unconstrained containers, which makes every limit decorative.
func TestDocker_NoDockerSocketInContainer(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	response, err := docker.Execute(ctx,
		request(`if [ -S /var/run/docker.sock ]; then echo '{"socket":true}'; else echo '{"socket":false}'; fi`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if strings.Contains(string(response.Output), "true") {
		t.Error("the Docker socket is reachable from inside a task container")
	}
}

// A writable root would let a worker persist state between attempts on the same
// node, quietly breaking the idempotency contract by making a retry behave
// differently from a first run.
func TestDocker_RootFilesystemIsReadOnly(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	response, err := docker.Execute(ctx,
		request(`if touch /probe 2>/dev/null; then echo '{"writable":true}'; else echo '{"writable":false}'; fi`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if strings.Contains(string(response.Output), "true") {
		t.Error("the container's root filesystem is writable")
	}
}

func TestDocker_MissingImageFails(t *testing.T) {
	docker := newDocker(t, runtime.WithPullPolicy(runtime.PullNever))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := request(`echo '{}'`)
	req.AgentImage = "agentflow/definitely-not-published:" + uuid.New().String()[:8]

	if _, err := docker.Execute(ctx, req); err == nil {
		t.Fatal("expected a missing image to fail the attempt")
	}
}

func TestDocker_NoImageIsAnError(t *testing.T) {
	docker := newDocker(t)

	req := request(`echo '{}'`)
	req.AgentImage = ""

	if _, err := docker.Execute(context.Background(), req); err == nil {
		t.Fatal("expected an empty image to fail")
	}
}

// countLabelled counts containers carrying a label, running or not, so a leak
// from any exit path shows up without depending on what else is on the daemon.
func countLabelled(t *testing.T, label, value string) int {
	t.Helper()

	cli, err := client.New(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		t.Fatalf("failed to connect to Docker: %v", err)
	}
	defer cli.Close()

	filters := client.Filters{}
	filters.Add("label", label+"="+value)

	containers, err := cli.ContainerList(context.Background(), client.ContainerListOptions{
		All:     true,
		Filters: filters,
	})
	if err != nil {
		t.Fatalf("failed to list containers: %v", err)
	}
	return len(containers.Items)
}

func TestDocker_Name(t *testing.T) {
	if got := newDocker(t).Name(); got != "docker" {
		t.Errorf("Name() = %q, want docker", got)
	}
}

// The inline limit is a hard cap on what the engine will accept from stdout.
//
// The engine reads a container's output into its own memory before it can judge
// the size, so an unbounded read is a way for one worker to take down a node
// running everyone else's tasks. Beyond the limit the attempt fails and the
// author is pointed at the path that does not go through the engine at all.
func TestDocker_OversizeStdoutFailsTheAttempt(t *testing.T) {
	limits := runtime.DefaultLimits
	limits.MaxOutputBytes = 4096
	docker := newDocker(t, runtime.WithLimits(limits))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Comfortably past the cap, and valid JSON, so the only thing wrong with it
	// is the size.
	_, err := docker.Execute(ctx, request(
		`printf '{"data":"'; for i in $(seq 1 2000); do printf 'xxxxxxxxxx'; done; printf '"}'`))
	if err == nil {
		t.Fatal("expected oversize output to fail the attempt")
	}
	if !strings.Contains(err.Error(), "AGENTFLOW_ARTIFACT_URI") {
		t.Errorf("error = %q, want it to point the author at the artifact URI", err)
	}
	if !strings.Contains(err.Error(), "4096") {
		t.Errorf("error = %q, want it to name the limit", err)
	}
}

// Output right up to the limit is still accepted; the cap is a ceiling, not a
// margin to stay well clear of.
func TestDocker_OutputAtTheLimitIsAccepted(t *testing.T) {
	limits := runtime.DefaultLimits
	limits.MaxOutputBytes = 4096
	docker := newDocker(t, runtime.WithLimits(limits))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// {"data":"…"} with 1000 payload bytes, well inside 4096.
	response, err := docker.Execute(ctx, request(
		`printf '{"data":"'; for i in $(seq 1 100); do printf 'xxxxxxxxxx'; done; printf '"}'`))
	if err != nil {
		t.Fatalf("Execute failed for output inside the limit: %v", err)
	}
	if len(response.Output) == 0 {
		t.Error("no output returned")
	}
}

// The stderr kept for error_message is capped too: logs can be enormous and the
// column is not a log store.
func TestDocker_StderrIsTruncated(t *testing.T) {
	limits := runtime.DefaultLimits
	limits.StderrLimit = 256
	docker := newDocker(t, runtime.WithLimits(limits))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := docker.Execute(ctx, request(
		`for i in $(seq 1 500); do echo "a very long failure explanation" >&2; done; exit 1`))
	if err == nil {
		t.Fatal("expected a non-zero exit to fail")
	}
	if len(err.Error()) > 4096 {
		t.Errorf("error message is %d bytes; stderr is not being truncated", len(err.Error()))
	}
}

// The artifact destination reaches the worker as an environment variable, which
// is the whole mechanism by which a large output bypasses the engine.
func TestDocker_PassesArtifactURI(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := request(`printf '{"uri":"%s"}' "$AGENTFLOW_ARTIFACT_URI"`)
	req.ArtifactURI = "https://blobs.example.com/presigned-destination"

	response, err := docker.Execute(ctx, req)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !strings.Contains(string(response.Output), "presigned-destination") {
		t.Errorf("output = %s, want the artifact URI passed through", response.Output)
	}
}

// With no blob storage configured there is no destination, and the variable is
// absent rather than empty — an agent testing for it gets a clear answer.
func TestDocker_OmitsArtifactURIWhenUnset(t *testing.T) {
	docker := newDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	response, err := docker.Execute(ctx, request(
		`if [ -z "${AGENTFLOW_ARTIFACT_URI+set}" ]; then echo '{"present":false}'; else echo '{"present":true}'; fi`))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if strings.Contains(string(response.Output), "true") {
		t.Error("AGENTFLOW_ARTIFACT_URI is set even with no blob storage configured")
	}
}
