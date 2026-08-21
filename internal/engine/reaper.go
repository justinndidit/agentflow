package engine

import (
	"context"
	"time"

	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/internal/telemetry"
	"github.com/rs/zerolog"
)

// DefaultReapInterval is how often a node looks for abandoned work. It is
// deliberately coarse relative to the heartbeat: reaping early is worse than
// reaping late, because it duplicates work that is still running.
const DefaultReapInterval = 15 * time.Second

// DefaultReapBatch bounds one reclaim pass. The pass and its follow-up cascades
// run in one transaction, so the batch is what bounds how long that transaction
// holds locks.
const DefaultReapBatch = 50

// Reaper returns work abandoned by nodes that can no longer finish it.
//
// Every node runs one, and no leader election is needed, because reclaiming is
// idempotent: the act of taking a task back clears the columns that made it a
// candidate, so a second reaper arriving a moment later finds nothing to do.
// That is a large complexity saving on one of the fussier parts of any
// distributed system.
//
// The reaper is what makes at-least-once execution actually happen. It is also
// what makes fencing mandatory: without lease_epoch, reclaiming a task from a
// node that is merely slow rather than dead lets that node's late write land on
// top of the result its replacement produced.
type Reaper struct {
	txManager *repositories.TxManager
	logger    *zerolog.Logger

	leaseTTL  time.Duration
	backoff   Backoff
	batchSize int
	interval  time.Duration
}

type ReaperOption func(*Reaper)

// WithReapInterval overrides how often Run sweeps. Mainly for tests.
func WithReapInterval(interval time.Duration) ReaperOption {
	return func(r *Reaper) { r.interval = interval }
}

// WithReapBatch overrides how many tasks one pass reclaims.
func WithReapBatch(size int) ReaperOption {
	return func(r *Reaper) { r.batchSize = size }
}

func NewReaper(
	txManager *repositories.TxManager,
	cfg *config.Engine,
	backoff Backoff,
	logger *zerolog.Logger,
	opts ...ReaperOption,
) *Reaper {
	r := &Reaper{
		txManager: txManager,
		logger:    logger,
		leaseTTL:  time.Duration(cfg.LeaseTTL) * time.Second,
		backoff:   backoff,
		batchSize: DefaultReapBatch,
		interval:  DefaultReapInterval,
	}
	for _, opt := range opts {
		opt(r)
	}

	// Defend against a config that never went through validation — a struct
	// literal in a test, or a caller assembling one by hand. A zero interval
	// panics NewTicker, which would take the whole node down at startup for
	// what is really just a missing field.
	if r.interval <= 0 {
		r.interval = DefaultReapInterval
	}
	if r.batchSize <= 0 {
		r.batchSize = DefaultReapBatch
	}
	if r.leaseTTL <= 0 {
		r.leaseTTL = time.Minute
	}
	return r
}

// Run sweeps until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.Info().
		Str("func", "Run").
		Dur("interval", r.interval).
		Dur("lease_ttl", r.leaseTTL).
		Int("batch_size", r.batchSize).
		Msg("reaper started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Info().Str("func", "Run").Msg("reaper stopped")
			return nil

		case <-ticker.C:
			if _, err := r.ReapOnce(ctx); err != nil {
				// Logged and retried next tick. A node that cannot reap is not
				// broken — every other node is running the same loop.
				r.logger.Error().Err(err).
					Str("func", "Run").
					Msg("reap failed, retrying next tick")
			}
		}
	}
}

// ReapOnce reclaims one batch and returns how many tasks it took back.
//
// The reclaim and the follow-up work for permanently failed tasks share one
// transaction. Splitting them would leave a window where a task is failed but
// its dependents are still pending, which is a workflow that can never
// terminate — precisely the state the cascade exists to prevent.
func (r *Reaper) ReapOnce(ctx context.Context) (int, error) {
	var count int

	err := r.txManager.WithTransaction(ctx, func(ctx context.Context, stores *repositories.Stores) error {
		// The backoff for a task that never reported back: it is scheduled from
		// its attempt number, which the reclaim reports after the fact.
		reclaimed, err := stores.TaskStore.ReclaimExpired(
			ctx, r.leaseTTL, r.backoff.For(1), r.batchSize)
		if err != nil {
			return err
		}
		count = len(reclaimed)

		for _, task := range reclaimed {
			telemetry.Meters().TaskReclaimed(ctx, string(task.Reason))

			if task.Retryable() {
				if err := r.rescheduleRetry(ctx, stores, task); err != nil {
					return err
				}
				continue
			}
			if err := r.finishPermanently(ctx, stores, task); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

// rescheduleRetry pushes the reclaimed task out by the backoff for the attempt
// that was actually lost, rather than the placeholder the reclaim applied.
func (r *Reaper) rescheduleRetry(
	ctx context.Context,
	stores *repositories.Stores,
	task repositories.Reclaimed,
) error {
	delay := r.backoff.For(task.Attempt)
	if err := stores.TaskStore.RescheduleAfter(ctx, task.TaskID, delay); err != nil {
		return err
	}

	r.logger.Warn().
		Str("func", "ReapOnce").
		Str("task_id", task.TaskID.String()).
		Str("task_key", task.TaskKey).
		Str("reason", string(task.Reason)).
		Int("attempt", task.Attempt).
		Dur("retry_in", delay).
		Msg("reclaimed task, returning it to the queue")

	return nil
}

// finishPermanently cancels what can never run now that this task is finished
// for good, and moves the workflow counters.
//
// No task_results row is written: the node that held this task vanished without
// reporting anything, so there is no output and no resolved input to record.
// Inventing an empty result would put a row in the evidence trail that no
// attempt actually produced.
func (r *Reaper) finishPermanently(
	ctx context.Context,
	stores *repositories.Stores,
	task repositories.Reclaimed,
) error {
	cancelled, err := stores.TaskStore.CancelDependents(ctx, task.WorkflowID, task.TaskKey)
	if err != nil {
		return err
	}

	err = stores.WorkflowStore.RecordTaskOutcome(
		ctx, task.WorkflowID, state.FailedTaskStatus, cancelled, 0)
	if err != nil {
		return err
	}

	r.logger.Error().
		Str("func", "ReapOnce").
		Str("task_id", task.TaskID.String()).
		Str("task_key", task.TaskKey).
		Str("reason", string(task.Reason)).
		Int("attempt", task.Attempt).
		Int("cancelled_dependents", cancelled).
		Msg("reclaimed task had no retries left; failed permanently")

	return nil
}
