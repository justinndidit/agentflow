package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/justinndidit/agentflow/internal/blob"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/justinndidit/agentflow/internal/telemetry"
	"github.com/rs/zerolog"
)

// Pool runs claimed tasks on this node, bounded by its capacity.
//
// The bound is a property of the host — how many containers this machine can
// actually run — not of any workflow's manifest. A workflow asking for eight
// workers is asking for something the scheduler cannot honour per-node, because
// its tasks are spread across a fleet whose size it does not know.
//
// Pool satisfies the dispatcher's Capacity and Handler interfaces: it reports
// what it can take, and takes it.
type Pool struct {
	capacity  int
	runtime   runtime.Runtime
	committer *Committer
	resolver  InputResolver
	agents    AgentLookup
	blobs     blob.Store
	logger    *zerolog.Logger
	leaseTTL  time.Duration

	mu       sync.Mutex
	inflight int

	wg sync.WaitGroup
}

func NewPool(
	capacity int,
	rt runtime.Runtime,
	committer *Committer,
	resolver InputResolver,
	agents AgentLookup,
	blobs blob.Store,
	leaseTTL time.Duration,
	logger *zerolog.Logger,
) *Pool {
	if capacity < 1 {
		capacity = 1
	}
	if resolver == nil {
		resolver = StaticResolver{}
	}
	if agents == nil {
		agents = StaticAgent{}
	}
	if blobs == nil {
		blobs = blob.Disabled{}
	}
	return &Pool{
		capacity:  capacity,
		runtime:   rt,
		committer: committer,
		resolver:  resolver,
		agents:    agents,
		blobs:     blobs,
		leaseTTL:  leaseTTL,
		logger:    logger,
	}
}

// FreeSlots reports how much more work this node can start.
//
// Slots are reserved when a task is handed over, not when its goroutine gets
// scheduled. Counting only started work would let the dispatcher claim against
// capacity that is already spoken for, and every task over the line would hold a
// lease this node cannot honour.
func (p *Pool) FreeSlots() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.capacity - p.inflight
}

// Handle takes a batch of claimed tasks and starts them.
//
// The whole batch is reserved or none of it is. The dispatcher already limits
// its claim to FreeSlots, so an oversized batch is a bug rather than
// backpressure — and accepting part of one would leave the rest leased to a node
// that never started them, which is strictly worse than rejecting the lot and
// letting the leases expire.
func (p *Pool) Handle(ctx context.Context, tasks []*models.TaskRow) error {
	if len(tasks) == 0 {
		return nil
	}

	if !p.reserve(len(tasks)) {
		return fmt.Errorf("pool: cannot take %d tasks with %d of %d slots free",
			len(tasks), p.FreeSlots(), p.capacity)
	}

	for _, task := range tasks {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.release(1)
			p.execute(ctx, task)
		}()
	}
	return nil
}

func (p *Pool) reserve(n int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.inflight+n > p.capacity {
		return false
	}
	p.inflight += n
	return true
}

func (p *Pool) release(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight -= n
}

// Drain waits for in-flight work to finish, or for ctx to expire.
//
// Shutdown deliberately lets running tasks complete rather than cancelling
// them: their results are worth keeping, and abandoning them means the work is
// redone by whichever node reclaims the lease. The caller stops the dispatcher
// first so nothing new arrives, then drains, then marks the node stopped.
func (p *Pool) Drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info().Str("func", "Drain").Msg("pool drained")
		return nil
	case <-ctx.Done():
		remaining := p.capacity - p.FreeSlots()
		p.logger.Warn().
			Str("func", "Drain").
			Int("abandoned", remaining).
			Msg("drain timed out; abandoning in-flight tasks to lease expiry")
		return ctx.Err()
	}
}

// execute runs one task and commits whatever came of it.
func (p *Pool) execute(ctx context.Context, task *models.TaskRow) {
	fence := repositories.FenceFor(task)

	// The attempt's span hangs off the workflow's root, which was never created
	// on this node — its identity is derived from the workflow id rather than
	// propagated, so a task claimed hours later on a different machine still
	// lands in the right trace.
	spanCtx, span := telemetry.StartAttemptSpan(
		telemetry.WorkflowContext(ctx, task.WorkflowID), task)
	defer span.End()

	telemetry.Meters().PoolInflight(spanCtx, 1)
	defer telemetry.Meters().PoolInflight(spanCtx, -1)

	// Not the dispatcher's context: cancelling it means "stop claiming", not
	// "abandon what is already running". Draining is what waits for these.
	taskCtx, cancel := context.WithTimeout(context.WithoutCancel(spanCtx), p.deadline(task))
	defer cancel()

	started := time.Now()

	// Resolution happens here rather than at claim time so one task's bad
	// upstream data fails that task alone, instead of aborting a whole claimed
	// batch before any of it starts.
	input, err := p.resolver.Resolve(taskCtx, task)
	if err != nil {
		telemetry.RecordFailure(span, "resolve", err)
		// A task failure, not an engine error: an upstream agent produced a
		// shape this manifest did not expect. It is recorded with the template
		// as the resolved input, since that is what the attempt actually had.
		p.commit(ctx, task, fence, Outcome{
			Duration:      time.Since(started),
			ResolvedInput: task.InputTemplate,
			Err:           err,
		})
		return
	}

	// The agent is resolved here rather than inside the runtime, so a Runtime
	// implementation never needs database access and can be swapped freely.
	agent, err := p.agents.Lookup(taskCtx, task.AgentName)
	if err != nil {
		telemetry.RecordFailure(span, "agent", err)
		p.commit(ctx, task, fence, Outcome{
			Duration:      time.Since(started),
			ResolvedInput: input,
			Err:           err,
		})
		return
	}

	// Presigned before the run so the worker can upload directly. The engine
	// hands out a destination and never touches the bytes: a large artifact
	// must not pass through a process that is also running everyone else's
	// tasks.
	artifactKey := blob.ArtifactKey(task.WorkflowID, task.ID, task.Attempt)
	artifactURL, err := p.blobs.PresignPut(taskCtx, artifactKey, p.deadline(task))
	if err != nil {
		// Not fatal to the attempt. A worker that had no use for the URL would
		// otherwise be failed by a storage outage it never depended on.
		p.logger.Warn().Err(err).
			Str("func", "execute").
			Str("task_key", task.TaskKey).
			Msg("could not presign an artifact destination; running without one")
		artifactURL = ""
	}

	response, runErr := p.runtime.Execute(taskCtx, runtime.Request{
		TaskID:     task.ID,
		WorkflowID: task.WorkflowID,
		TaskKey:    task.TaskKey,
		AgentName:  task.AgentName,
		AgentImage: agent.Image,
		Command:    agent.Command,
		Attempt:    task.Attempt,
		// Stable across attempts by design, so a worker can recognise work it
		// already did rather than repeat its side effects.
		IdempotencyKey: task.ID.String(),
		Input:          input,
		ArtifactURI:    artifactURL,
	})

	outcome := Outcome{
		Duration: time.Since(started),
		// What the worker was actually given, which is the only useful record
		// when a failed attempt has to be explained after the fact.
		ResolvedInput: input,
	}
	if runErr != nil {
		outcome.Err = runErr
		telemetry.RecordFailure(span, "execute", runErr)
		telemetry.Meters().TaskFailed(spanCtx, task.AgentName, outcome.Duration)
	} else if response != nil {
		outcome.Output = response.Output
		outcome.TokensUsed = response.TokensUsed
		outcome.CostMicros = response.CostMicros

		telemetry.RecordSuccess(span, response.TokensUsed)
		telemetry.Meters().TaskCompleted(spanCtx, task.AgentName,
			response.TokensUsed, response.CostMicros, outcome.Duration)
	}

	// Recorded whether or not the attempt succeeded: a failed run that uploaded
	// a partial artifact is often the only evidence of what it was doing.
	if artifactURL != "" {
		outcome.ArtifactURI = p.storedArtifact(spanCtx, task, artifactKey)
		telemetry.RecordArtifact(span, outcome.ArtifactURI)
	}

	p.commit(spanCtx, task, fence, outcome)
}

// storedArtifact reports the durable reference for anything the worker uploaded,
// or nil if it uploaded nothing.
//
// Most tasks return their output inline and never touch the presigned URL, so
// absence is the common case rather than a problem.
func (p *Pool) storedArtifact(ctx context.Context, task *models.TaskRow, key string) *string {
	// Its own context: the attempt's has often just expired, and an artifact a
	// timed-out worker managed to upload is still worth recording.
	statCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	object, err := p.blobs.Stat(statCtx, key)
	if err != nil {
		p.logger.Error().Err(err).
			Str("func", "storedArtifact").
			Str("task_key", task.TaskKey).
			Str("key", key).
			Msg("could not check for an uploaded artifact")
		return nil
	}
	if object == nil {
		return nil
	}

	p.logger.Info().
		Str("func", "storedArtifact").
		Str("task_key", task.TaskKey).
		Str("uri", object.URI).
		Int64("bytes", object.Size).
		Msg("task wrote an artifact")

	return &object.URI
}

// commit records an outcome, treating a superseded lease as expected rather
// than as a failure.
func (p *Pool) commit(
	ctx context.Context,
	task *models.TaskRow,
	fence repositories.Fence,
	outcome Outcome,
) {

	// The commit must not inherit the task's deadline: a task that ran right up
	// to its timeout would then be unable to record that it did.
	commitCtx, commitCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer commitCancel()

	if commitErr := p.committer.Commit(commitCtx, fence, outcome); commitErr != nil {
		if IsFenced(commitErr) {
			// Expected, not a failure: this node's lease was reclaimed while the
			// work ran, and another node has already redone it. Discard.
			telemetry.Meters().WriteFenced(ctx, task.AgentName)
			p.logger.Warn().
				Str("func", "execute").
				Str("task_id", task.ID.String()).
				Str("task_key", task.TaskKey).
				Int64("lease_epoch", fence.LeaseEpoch).
				Msg("lease was superseded while running; discarding result")
			return
		}
		p.logger.Error().Err(commitErr).
			Str("func", "execute").
			Str("task_id", task.ID.String()).
			Msg("failed to commit task outcome; leaving it to lease expiry")
	}
}

// deadline bounds an attempt by the shorter of the task's own timeout and the
// lease it is running under.
//
// Letting a task outlive its lease guarantees duplicate work: the reaper
// reclaims it, another node starts it, and the original is still running. The
// lease is the real ceiling, whatever the manifest asked for.
func (p *Pool) deadline(task *models.TaskRow) time.Duration {
	limit := p.leaseTTL
	if limit <= 0 {
		limit = time.Minute
	}

	if task.Timeout != nil && task.Timeout.Valid {
		requested := time.Duration(task.Timeout.Microseconds) * time.Microsecond
		if requested > 0 && requested < limit {
			return requested
		}
	}
	return limit
}
