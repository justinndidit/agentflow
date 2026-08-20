package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/rs/zerolog"
)

// Limits are the resources a single task's container may use.
//
// These are not advisory. A worker is arbitrary third-party code running on a
// node that is also running other people's work, so an unbounded container is a
// way for one task to take down everything else on the host.
type Limits struct {
	// MemoryBytes caps resident memory. Exceeding it means the kernel OOM-kills
	// the container, which surfaces as a failed attempt.
	MemoryBytes int64

	// NanoCPUs caps CPU time; 1_000_000_000 is one core.
	NanoCPUs int64

	// PidsLimit caps process count, which is what stops a fork bomb.
	PidsLimit int64

	// NetworkMode is passed to Docker as-is. "none" isolates the container
	// entirely; agents that call an LLM API need "bridge".
	NetworkMode string

	// StderrLimit bounds how much of a failed container's stderr is kept for
	// error_message. Logs can be enormous and the column is not a log store.
	StderrLimit int

	// MaxOutputBytes is the largest stdout the engine will accept inline.
	//
	// This is a hard cap, not a hint. The engine reads a container's output
	// into its own memory before it can judge the size, so an unbounded read is
	// a way for one worker to take down a node that is running everyone else's
	// tasks too. Beyond this the attempt fails and the author is pointed at
	// AGENTFLOW_ARTIFACT_URI, which is the path that does not go through the
	// engine at all.
	MaxOutputBytes int
}

// DefaultLimits are deliberately modest. A node runs several of these at once,
// and the ceiling that matters is the host's, not one task's ambition.
var DefaultLimits = Limits{
	MemoryBytes: 512 * 1024 * 1024,
	NanoCPUs:    1_000_000_000,
	PidsLimit:   256,
	// Agents generally need to reach a model provider. A deployment that runs
	// only local models should set this to "none".
	NetworkMode: "bridge",
	StderrLimit: 4096,
	// Matches the architecture doc's inline threshold. Anything larger belongs
	// in blob storage, written by the worker rather than relayed by the engine.
	MaxOutputBytes: 256 * 1024,
}

// Docker runs each task as a container.
//
// The contract, from the architecture doc: resolved JSON arrives on stdin, JSON
// comes back on stdout, exit 0 means success and anything else is a failed
// attempt with stderr captured as the reason. The engine never inspects what is
// inside the image — that is the whole point of the boundary.
type Docker struct {
	client *client.Client
	limits Limits
	logger *zerolog.Logger

	// pullPolicy decides whether a missing image is fetched. Pulling inside a
	// task's deadline spends the lease on a download, so it is opt-in.
	pullPolicy PullPolicy
}

type PullPolicy string

const (
	// PullIfMissing fetches an image the daemon does not already have.
	PullIfMissing PullPolicy = "if-missing"

	// PullNever fails a task whose image is absent. Appropriate where images
	// are pre-loaded onto nodes and an unexpected pull would mean a
	// misconfiguration rather than a first run.
	PullNever PullPolicy = "never"
)

type DockerOption func(*Docker)

func WithLimits(limits Limits) DockerOption {
	return func(d *Docker) { d.limits = limits }
}

func WithPullPolicy(policy PullPolicy) DockerOption {
	return func(d *Docker) { d.pullPolicy = policy }
}

// NewDocker connects to the local Docker daemon.
func NewDocker(logger *zerolog.Logger, opts ...DockerOption) (*Docker, error) {
	cli, err := client.New(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connect to docker: %w", err)
	}

	d := &Docker{
		client:     cli,
		limits:     DefaultLimits,
		logger:     logger,
		pullPolicy: PullIfMissing,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d, nil
}

func (d *Docker) Name() string { return "docker" }

// Close releases the daemon connection.
func (d *Docker) Close() error { return d.client.Close() }

// Execute runs one attempt as a container and returns what it wrote to stdout.
//
// Every container is removed when the attempt ends, including on the failure and
// cancellation paths. A node claims tasks continuously, so a leaked container
// per attempt fills the host's disk in hours.
func (d *Docker) Execute(ctx context.Context, req Request) (*Response, error) {
	if req.AgentImage == "" {
		return nil, fmt.Errorf("task %s: agent %s has no image", req.TaskKey, req.AgentName)
	}

	if err := d.ensureImage(ctx, req.AgentImage); err != nil {
		return nil, err
	}

	id, err := d.create(ctx, req)
	if err != nil {
		return nil, err
	}
	defer d.remove(ctx, id)

	if err := d.writeInput(ctx, id, req.Input); err != nil {
		return nil, err
	}

	if _, err := d.client.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		return nil, fmt.Errorf("task %s: start container: %w", req.TaskKey, err)
	}

	code, waitErr := d.wait(ctx, id)
	stdout, stderr, oversize, logErr := d.logs(ctx, id)
	if logErr != nil {
		d.logger.Warn().Err(logErr).
			Str("func", "Execute").
			Str("task_key", req.TaskKey).
			Msg("failed to read container logs")
	}

	if waitErr != nil {
		// Includes the deadline being hit. The container is killed by the
		// deferred removal, so nothing outlives the lease.
		return nil, fmt.Errorf("task %s: %w", req.TaskKey, waitErr)
	}

	if code != 0 {
		return nil, fmt.Errorf("task %s: agent exited %d: %s",
			req.TaskKey, code, truncate(stderr, d.limits.StderrLimit))
	}

	if oversize {
		return nil, fmt.Errorf(
			"task %s: agent wrote more than %d bytes to stdout; "+
				"write large output to AGENTFLOW_ARTIFACT_URI and return a reference to it",
			req.TaskKey, d.limits.MaxOutputBytes)
	}

	return d.response(req, stdout)
}

// response parses what the worker wrote to stdout.
//
// A zero-exit worker that produced nothing is treated as an empty result rather
// than an error: not every agent has an output, and input_template is NOT NULL
// but output is not.
func (d *Docker) response(req Request, stdout string) (*Response, error) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return &Response{Output: []byte(`{}`)}, nil
	}

	// Validated rather than passed through, because it goes straight into a
	// JSONB column and a malformed payload would fail the commit instead of the
	// task — reporting an engine problem for what is really a worker bug.
	if !json.Valid([]byte(trimmed)) {
		return nil, fmt.Errorf("task %s: agent wrote invalid JSON to stdout: %s",
			req.TaskKey, truncate(trimmed, d.limits.StderrLimit))
	}

	return &Response{Output: []byte(trimmed)}, nil
}

// Container labels, so an operator staring at `docker ps` on a busy node can
// tell which task a container belongs to, and so anything cleaning up after a
// crashed engine can find what it left behind.
const (
	LabelTaskID     = "agentflow.task-id"
	LabelWorkflowID = "agentflow.workflow-id"
	LabelTaskKey    = "agentflow.task-key"
	LabelAttempt    = "agentflow.attempt"
	LabelAgent      = "agentflow.agent"
)

func (d *Docker) create(ctx context.Context, req Request) (string, error) {
	config := &container.Config{
		Image: req.AgentImage,
		Env:   environment(req),
		// Empty for a real agent, so the image's own entrypoint runs.
		Cmd: req.Command,
		Labels: map[string]string{
			LabelTaskID:     req.TaskID.String(),
			LabelWorkflowID: req.WorkflowID.String(),
			LabelTaskKey:    req.TaskKey,
			LabelAttempt:    strconv.Itoa(req.Attempt),
			LabelAgent:      req.AgentName,
		},
		// stdin has to be attached at creation; it cannot be added later.
		OpenStdin:   true,
		StdinOnce:   true,
		AttachStdin: true,
		Tty:         false,
	}

	hostConfig := &container.HostConfig{
		Resources: container.Resources{
			Memory:    d.limits.MemoryBytes,
			NanoCPUs:  d.limits.NanoCPUs,
			PidsLimit: &d.limits.PidsLimit,
		},
		NetworkMode: container.NetworkMode(d.limits.NetworkMode),

		// A writable root lets a worker persist state between attempts on the
		// same node, which would quietly break the idempotency contract by
		// making a retry behave differently from a first run.
		ReadonlyRootfs: true,

		// The engine never mounts the Docker socket. A worker that could reach
		// it could start unconstrained containers, which makes every limit
		// above decorative.
		Binds: nil,

		// Dropping all capabilities and blocking privilege escalation means a
		// compromised worker cannot become root on the host.
		CapDrop:     []string{"ALL"},
		SecurityOpt: []string{"no-new-privileges"},

		AutoRemove: false, // removed explicitly, so logs can be read first
	}

	created, err := d.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     config,
		HostConfig: hostConfig,
	})
	if err != nil {
		return "", fmt.Errorf("task %s: create container from %s: %w",
			req.TaskKey, req.AgentImage, err)
	}
	return created.ID, nil
}

// environment renders the worker contract's variables.
func environment(req Request) []string {
	env := []string{
		"AGENTFLOW_TASK_ID=" + req.TaskID.String(),
		"AGENTFLOW_WORKFLOW_ID=" + req.WorkflowID.String(),
		"AGENTFLOW_TASK_KEY=" + req.TaskKey,
		"AGENTFLOW_AGENT_NAME=" + req.AgentName,
		"AGENTFLOW_ATTEMPT=" + strconv.Itoa(req.Attempt),
		"AGENTFLOW_IDEMPOTENCY_KEY=" + req.IdempotencyKey,
	}
	if req.ArtifactURI != "" {
		env = append(env, "AGENTFLOW_ARTIFACT_URI="+req.ArtifactURI)
	}
	return env
}

// writeInput streams the resolved input to the container's stdin and closes it,
// so a worker reading to EOF is not left waiting.
func (d *Docker) writeInput(ctx context.Context, id string, input []byte) error {
	attached, err := d.client.ContainerAttach(ctx, id, client.ContainerAttachOptions{
		Stream: true,
		Stdin:  true,
	})
	if err != nil {
		return fmt.Errorf("attach stdin: %w", err)
	}
	defer attached.Close()

	if len(input) > 0 {
		if _, err := attached.Conn.Write(input); err != nil {
			return fmt.Errorf("write stdin: %w", err)
		}
	}

	// EOF, or a worker that reads until the stream ends blocks until the
	// deadline and the attempt fails for no reason.
	if err := attached.CloseWrite(); err != nil {
		return fmt.Errorf("close stdin: %w", err)
	}
	return nil
}

func (d *Docker) wait(ctx context.Context, id string) (int64, error) {
	waiting := d.client.ContainerWait(ctx, id, client.ContainerWaitOptions{
		Condition: container.WaitConditionNotRunning,
	})

	select {
	case err := <-waiting.Error:
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return 0, fmt.Errorf("exceeded its deadline: %w", ctx.Err())
			}
			return 0, fmt.Errorf("wait for container: %w", err)
		}
		return 0, nil
	case result := <-waiting.Result:
		if result.Error != nil {
			return result.StatusCode, fmt.Errorf("container error: %s", result.Error.Message)
		}
		return result.StatusCode, nil
	}
}

// logs reads the container's output after it has exited, reporting whether
// stdout exceeded the inline limit.
func (d *Docker) logs(ctx context.Context, id string) (stdout, stderr string, oversize bool, err error) {
	// Its own context: the attempt's has usually expired by the time a
	// timed-out container is being explained, and that is exactly when its
	// stderr matters most.
	logCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	reader, err := d.client.ContainerLogs(logCtx, id, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return "", "", false, err
	}
	defer reader.Close()

	// Both buffers are capped. stderr only ever becomes a truncated error
	// message, and stdout beyond the limit is a failed attempt regardless — so
	// there is no reason to hold more of either than will be used.
	out := &cappedBuffer{limit: d.limits.MaxOutputBytes}
	errOut := &cappedBuffer{limit: d.limits.StderrLimit}

	if err := demultiplex(reader, out, errOut); err != nil {
		return out.String(), errOut.String(), out.Overflowed, err
	}
	return out.String(), errOut.String(), out.Overflowed, nil
}

// cappedBuffer accumulates up to limit bytes and records whether more arrived.
//
// It keeps writing rather than erroring, because the log stream has to be
// drained to the end: abandoning it mid-frame would leave the connection in an
// unknown state and lose the stderr that explains what the worker did.
type cappedBuffer struct {
	limit      int
	buffer     bytes.Buffer
	Overflowed bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if c.limit > 0 {
		remaining := c.limit - c.buffer.Len()
		if remaining <= 0 {
			c.Overflowed = true
			return len(p), nil
		}
		if len(p) > remaining {
			c.Overflowed = true
			c.buffer.Write(p[:remaining])
			return len(p), nil
		}
	}
	return c.buffer.Write(p)
}

func (c *cappedBuffer) String() string { return c.buffer.String() }

// demultiplex splits Docker's multiplexed log stream into stdout and stderr.
//
// Without a TTY the daemon frames output as an 8-byte header followed by a
// payload, with the stream identified by the first byte. Reading the stream
// raw would interleave stderr into the JSON on stdout and corrupt every result.
func demultiplex(reader io.Reader, stdout, stderr io.Writer) error {
	header := make([]byte, 8)

	for {
		if _, err := io.ReadFull(reader, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}

		size := int64(header[4])<<24 | int64(header[5])<<16 | int64(header[6])<<8 | int64(header[7])
		if size == 0 {
			continue
		}

		target := stdout
		if header[0] == 2 {
			target = stderr
		}
		if _, err := io.CopyN(target, reader, size); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
	}
}

// ensureImage pulls the image if the policy allows and the daemon lacks it.
func (d *Docker) ensureImage(ctx context.Context, ref string) error {
	_, err := d.client.ImageInspect(ctx, ref)
	if err == nil {
		return nil
	}

	if d.pullPolicy == PullNever {
		return fmt.Errorf("image %s is not present and the pull policy is %s", ref, PullNever)
	}

	d.logger.Info().Str("func", "ensureImage").Str("image", ref).Msg("pulling agent image")

	pull, err := d.client.ImagePull(ctx, ref, client.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("pull image %s: %w", ref, err)
	}
	defer pull.Close()

	// The pull is only complete once the stream is consumed; returning early
	// leaves the image half-fetched and the container create fails.
	if err := pull.Wait(ctx); err != nil {
		return fmt.Errorf("pull image %s: %w", ref, err)
	}
	return nil
}

// remove deletes the container, killing it first if it is somehow still up.
func (d *Docker) remove(ctx context.Context, id string) {
	// Its own context, because the usual reason for removal is the attempt's
	// context having just expired — and that is precisely when the container
	// most needs to be torn down.
	removeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	_, err := d.client.ContainerRemove(removeCtx, id, client.ContainerRemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	})
	if err != nil {
		d.logger.Error().Err(err).
			Str("func", "remove").
			Str("container", id).
			Msg("failed to remove container; it will leak on this host")
	}
}

func truncate(text string, limit int) string {
	text = strings.TrimSpace(text)
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit] + "… (truncated)"
}
