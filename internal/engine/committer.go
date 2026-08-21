package engine

import (
	"context"
	"errors"
	"time"

	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/internal/telemetry"
	"github.com/rs/zerolog"
)

// ReadyChannel is the notification channel dispatchers listen on. The committer
// signals it whenever a task it just finished unblocked another, so a waiting
// node starts work immediately rather than at its next poll.
const ReadyChannel = "agentflow_ready"

// Outcome is what a worker reports back about one attempt.
type Outcome struct {
	// Output is the worker's JSON result, stored inline. Large payloads belong
	// in blob storage with ArtifactURI pointing at them; that is not built yet.
	Output []byte

	// ArtifactURI references a blob for outputs too large to sit in Postgres.
	ArtifactURI *string

	// ResolvedInput is the concrete input the worker was given, after template
	// resolution. Recorded because it is usually the only evidence of why a
	// failed attempt failed.
	ResolvedInput []byte

	TokensUsed int64
	CostMicros int64
	Duration   time.Duration

	// Err is nil on success. Its message is stored on the task row.
	Err error
}

// Committer writes terminal outcomes.
//
// Every write is fenced, and every write is one transaction covering all of:
// the guarded task update, the result row, the dependent decrement, the workflow
// counters, and the notification. If any step fails none of it happened, and the
// lease expiry produces a clean retry rather than a half-recorded finish.
type Committer struct {
	txManager *repositories.TxManager
	logger    *zerolog.Logger
	backoff   Backoff
}

func NewCommitter(txManager *repositories.TxManager, logger *zerolog.Logger, backoff Backoff) *Committer {
	return &Committer{
		txManager: txManager,
		logger:    logger,
		backoff:   backoff,
	}
}

// Commit records the outcome of one attempt under the fence it was issued with.
//
// A returned ErrFenced is an expected outcome, not a failure of this call: it
// means the lease moved on while the work was running and another node has
// already redone it. The caller should discard its result and carry on.
func (c *Committer) Commit(ctx context.Context, fence repositories.Fence, outcome Outcome) error {
	if outcome.Err != nil {
		return c.commitFailure(ctx, fence, outcome)
	}
	return c.commitSuccess(ctx, fence, outcome)
}

func (c *Committer) commitSuccess(ctx context.Context, fence repositories.Fence, outcome Outcome) error {
	return c.txManager.WithTransaction(ctx, func(ctx context.Context, stores *repositories.Stores) error {
		commit, err := stores.TaskStore.MarkCompleted(ctx, fence)
		if err != nil {
			return err
		}

		if err := c.recordResult(ctx, stores, fence, commit.Attempt, outcome); err != nil {
			return err
		}

		// Only a success can unblock anything.
		ready, err := stores.TaskStore.DecrementDependents(ctx, commit.WorkflowID, commit.TaskKey)
		if err != nil {
			return err
		}

		err = stores.WorkflowStore.RecordTaskOutcome(
			ctx, commit.WorkflowID, state.CompletedTaskStatus, 0, outcome.TokensUsed)
		if err != nil {
			return err
		}

		c.logger.Info().
			Str("func", "Commit").
			Str("task_id", fence.TaskID.String()).
			Str("task_key", commit.TaskKey).
			Int("attempt", commit.Attempt).
			Int("unblocked", len(ready)).
			Msg("task completed")

		if len(ready) == 0 {
			return nil
		}
		// Delivered on commit, so a rolled-back transaction never wakes a
		// dispatcher for work that does not exist.
		return stores.TaskStore.Notify(ctx, ReadyChannel, commit.WorkflowID.String())
	})
}

func (c *Committer) commitFailure(ctx context.Context, fence repositories.Fence, outcome Outcome) error {
	return c.txManager.WithTransaction(ctx, func(ctx context.Context, stores *repositories.Stores) error {
		// The backoff is computed for the attempt that just failed. The
		// statement decides whether it is used at all, since only it can see
		// the row's retry budget without racing the reaper.
		commit, err := stores.TaskStore.MarkFailed(
			ctx, fence, outcome.Err.Error(), c.backoff.For(1))
		if err != nil {
			return err
		}

		// Recompute now that the real attempt number is known, and rewrite the
		// schedule if this is a retry. Cheap, and it keeps the first statement
		// from needing a second round trip to learn the attempt.
		if commit.Status == state.PendingTaskStatus {
			if err := c.rescheduleRetry(ctx, stores, fence, commit, outcome); err != nil {
				return err
			}
			return nil
		}

		if err := c.recordResult(ctx, stores, fence, commit.Attempt, outcome); err != nil {
			return err
		}

		// Permanently failed: everything downstream can never become ready, so
		// it is cancelled rather than left pending forever.
		cancelled, err := stores.TaskStore.CancelDependents(ctx, commit.WorkflowID, commit.TaskKey)
		if err != nil {
			return err
		}

		err = stores.WorkflowStore.RecordTaskOutcome(
			ctx, commit.WorkflowID, state.FailedTaskStatus, cancelled, outcome.TokensUsed)
		if err != nil {
			return err
		}

		telemetry.Meters().TasksCancelled(ctx, cancelled)

		c.logger.Warn().
			Err(outcome.Err).
			Str("func", "Commit").
			Str("task_id", fence.TaskID.String()).
			Str("task_key", commit.TaskKey).
			Int("attempt", commit.Attempt).
			Int("cancelled_dependents", cancelled).
			Msg("task failed permanently")

		return nil
	})
}

// rescheduleRetry records the failed attempt and pushes not_before out by the
// backoff for the attempt that actually ran.
func (c *Committer) rescheduleRetry(
	ctx context.Context,
	stores *repositories.Stores,
	fence repositories.Fence,
	commit *repositories.TaskCommit,
	outcome Outcome,
) error {
	if err := c.recordResult(ctx, stores, fence, commit.Attempt, outcome); err != nil {
		return err
	}

	delay := c.backoff.For(commit.Attempt)
	if err := stores.TaskStore.RescheduleAfter(ctx, fence.TaskID, delay); err != nil {
		return err
	}

	c.logger.Info().
		Err(outcome.Err).
		Str("func", "Commit").
		Str("task_id", fence.TaskID.String()).
		Str("task_key", commit.TaskKey).
		Int("attempt", commit.Attempt).
		Dur("retry_in", delay).
		Msg("task failed, retrying")

	return nil
}

func (c *Committer) recordResult(
	ctx context.Context,
	stores *repositories.Stores,
	fence repositories.Fence,
	attempt int,
	outcome Outcome,
) error {
	return stores.TaskResultStore.Insert(ctx, &models.TaskResult{
		TaskID:        fence.TaskID,
		Attempt:       attempt,
		Output:        outcome.Output,
		ArtifactURI:   outcome.ArtifactURI,
		ResolvedInput: outcome.ResolvedInput,
		TokensUsed:    outcome.TokensUsed,
		CostMicros:    outcome.CostMicros,
		DurationMS:    outcome.Duration.Milliseconds(),
		CreatedAt:     time.Now().UTC(),
	})
}

// IsFenced reports whether an error means the caller's lease was superseded.
func IsFenced(err error) bool { return errors.Is(err, repositories.ErrFenced) }
