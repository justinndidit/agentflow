package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/state"
)

// TaskCommit is what a guarded write reports back about the row it changed.
//
// The values come from the statement's RETURNING rather than from the caller's
// copy of the task: the attempt number keys the result row, and the workflow and
// task key drive the dependent decrement, so all three have to be what the
// database currently holds rather than what this node remembers.
type TaskCommit struct {
	WorkflowID uuid.UUID
	TaskKey    string
	Attempt    int
	Status     state.TaskStatus
}

// MarkCompleted records a successful finish, guarded by the fence.
//
// The guard is the state machine. status = ANY(allowed) is built from
// state.AllowedSources rather than hand-written, so a change to the transition
// table changes this statement too.
//
// Zero rows affected means one of three things, and all of them mean the same
// to the caller: the task no longer exists, this node no longer holds the lease,
// or the task is not running any more. In every case this node's result is stale
// and must be discarded, which is what ErrFenced says.
func (p *PostgresTaskStore) MarkCompleted(ctx context.Context, fence Fence) (*TaskCommit, error) {
	stmt := `UPDATE tasks
	    SET status           = 'completed',
	        finished_at      = now(),
	        lease_expires_at = NULL,
	        error_message    = NULL,
	        updated_at       = now()
	  WHERE id = @taskID
	    AND engine_id = @engineID
	    AND lease_epoch = @leaseEpoch
	    AND status = ANY(@allowed::task_status[])
	RETURNING workflow_id, task_key, attempt, status`

	return p.guardedWrite(ctx, stmt, pgx.NamedArgs{
		"taskID":     fence.TaskID,
		"engineID":   fence.EngineID,
		"leaseEpoch": fence.LeaseEpoch,
		"allowed":    state.AllowedSourceNames(state.CompletedTaskStatus),
	})
}

// MarkFailed records a failed attempt, guarded by the fence, and decides in the
// same statement whether the task retries or is finished for good.
//
// The decision is made in SQL rather than in Go because it depends on the row's
// own attempt and max_retries, and reading those first would open a window where
// the reaper changes them underneath the write.
//
// On retry the task returns to pending with not_before pushed out by backoff,
// engine_id cleared, and finished_at left null — it has not finished. Crucially
// remaining_deps is untouched: the dependencies that were satisfied before the
// failure are still satisfied, and decrementing again would corrupt the counter.
func (p *PostgresTaskStore) MarkFailed(
	ctx context.Context,
	fence Fence,
	errorMessage string,
	backoff time.Duration,
) (*TaskCommit, error) {
	stmt := `UPDATE tasks
	    SET status = CASE WHEN attempt <= max_retries
	                      THEN 'pending'::task_status
	                      ELSE 'failed'::task_status END,
	        not_before = CASE WHEN attempt <= max_retries
	                          THEN now() + @backoff::interval
	                          ELSE not_before END,
	        engine_id = CASE WHEN attempt <= max_retries
	                         THEN NULL
	                         ELSE engine_id END,
	        finished_at = CASE WHEN attempt <= max_retries
	                           THEN NULL
	                           ELSE now() END,
	        lease_expires_at = NULL,
	        error_message    = @errorMessage,
	        updated_at       = now()
	  WHERE id = @taskID
	    AND engine_id = @engineID
	    AND lease_epoch = @leaseEpoch
	    AND status = ANY(@allowed::task_status[])
	RETURNING workflow_id, task_key, attempt, status`

	return p.guardedWrite(ctx, stmt, pgx.NamedArgs{
		"taskID":       fence.TaskID,
		"engineID":     fence.EngineID,
		"leaseEpoch":   fence.LeaseEpoch,
		"errorMessage": errorMessage,
		"backoff":      durationToInterval(backoff),
		"allowed":      state.AllowedSourceNames(state.FailedTaskStatus),
	})
}

func (p *PostgresTaskStore) guardedWrite(ctx context.Context, stmt string, args pgx.NamedArgs) (*TaskCommit, error) {
	var commit TaskCommit

	rows, err := p.repo.Query(ctx, stmt, args)
	if err != nil {
		return nil, fmt.Errorf("guarded write on task %v: %w", args["taskID"], err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("guarded write on task %v: %w", args["taskID"], err)
		}
		// The row exists but this caller does not own it any more.
		return nil, fmt.Errorf("task %v: %w", args["taskID"], ErrFenced)
	}

	if err := rows.Scan(&commit.WorkflowID, &commit.TaskKey, &commit.Attempt, &commit.Status); err != nil {
		return nil, fmt.Errorf("scan guarded write on task %v: %w", args["taskID"], err)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("guarded write on task %v: %w", args["taskID"], err)
	}
	return &commit, nil
}

// DecrementDependents reduces remaining_deps on every task that lists taskKey
// as a dependency, and returns the ids of those that reached zero.
//
// The workflow_id predicate is not optional. depends_on holds task keys rather
// than ids, and task keys are unique only within a workflow — the schema says so
// with UNIQUE (workflow_id, task_key). Without the scope, submitting the same
// manifest twice would have each run decrementing the other's counters, which
// surfaces as tasks starting before their dependencies finished.
//
// The GIN index on depends_on serves the @> containment lookup.
//
// There is no status filter, deliberately. A dependent can only leave pending by
// being claimed, which requires remaining_deps = 0, which requires every
// dependency to have already committed — so a non-pending dependent here means a
// counter has gone wrong, and CHECK (remaining_deps >= 0) should fail loudly
// rather than be filtered out of sight.
func (p *PostgresTaskStore) DecrementDependents(
	ctx context.Context,
	workflowID uuid.UUID,
	taskKey string,
) ([]uuid.UUID, error) {
	stmt := `UPDATE tasks
	    SET remaining_deps = remaining_deps - 1,
	        updated_at     = now()
	  WHERE workflow_id = @workflowID
	    AND depends_on @> ARRAY[@taskKey]::text[]
	RETURNING id, remaining_deps`

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{
		"workflowID": workflowID,
		"taskKey":    taskKey,
	})
	if err != nil {
		return nil, fmt.Errorf("decrement dependents of %s in workflow %s: %w", taskKey, workflowID, err)
	}
	defer rows.Close()

	ready := []uuid.UUID{}
	for rows.Next() {
		var (
			id        uuid.UUID
			remaining int
		)
		if err := rows.Scan(&id, &remaining); err != nil {
			return nil, fmt.Errorf("scan dependent of %s: %w", taskKey, err)
		}
		if remaining == 0 {
			ready = append(ready, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("decrement dependents of %s: %w", taskKey, err)
	}
	return ready, nil
}

// CancelDependents cancels every task that transitively depends on taskKey, and
// returns how many were cancelled.
//
// A task that has permanently failed can never satisfy its dependents, so
// without this they sit pending forever and the workflow never terminates.
//
// The recursion walks task keys rather than ids — depends_on holds keys — and is
// scoped to one workflow throughout, both because keys are only unique there and
// because a cascade must never cross into another run.
//
// Only pending tasks are cancelled. Work already running is left to finish:
// killing it would throw away compute that has already been paid for, and it can
// still produce a result worth keeping even though nothing downstream will
// consume it. Independent branches are untouched by construction, since they do
// not appear in the closure.
func (p *PostgresTaskStore) CancelDependents(
	ctx context.Context,
	workflowID uuid.UUID,
	taskKey string,
) (int, error) {
	stmt := `WITH RECURSIVE doomed AS (
	        SELECT @taskKey::text AS task_key
	        UNION
	        SELECT t.task_key
	          FROM tasks t
	          JOIN doomed d ON d.task_key = ANY(t.depends_on)
	         WHERE t.workflow_id = @workflowID
	    )
	    UPDATE tasks
	       SET status      = 'cancelled',
	           finished_at = now(),
	           updated_at  = now()
	     WHERE workflow_id = @workflowID
	       AND status = 'pending'
	       AND task_key IN (SELECT task_key FROM doomed WHERE task_key <> @taskKey)`

	tag, err := p.repo.Exec(ctx, stmt, pgx.NamedArgs{
		"workflowID": workflowID,
		"taskKey":    taskKey,
	})
	if err != nil {
		return 0, fmt.Errorf("cancel dependents of %s in workflow %s: %w", taskKey, workflowID, err)
	}
	return int(tag.RowsAffected()), nil
}

// Notify emits a Postgres notification on channel.
//
// pg_notify is used rather than a literal NOTIFY because the channel and payload
// are parameters; NOTIFY takes only literals. The notification is delivered when
// the surrounding transaction commits, so a rolled-back commit never wakes a
// dispatcher for work that does not exist.
func (p *PostgresTaskStore) Notify(ctx context.Context, channel, payload string) error {
	if _, err := p.repo.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, payload); err != nil {
		return fmt.Errorf("notify %s: %w", channel, err)
	}
	return nil
}

// RescheduleAfter pushes a task's next eligible time out by delay.
//
// The retry decision itself is made inside MarkFailed, which is the only
// statement that can read attempt and max_retries without racing the reaper.
// This applies the backoff for the attempt that actually ran, which MarkFailed
// reports only after the fact.
func (p *PostgresTaskStore) RescheduleAfter(ctx context.Context, taskID uuid.UUID, delay time.Duration) error {
	tag, err := p.repo.Exec(ctx,
		`UPDATE tasks
		    SET not_before = now() + $2::interval, updated_at = now()
		  WHERE id = $1 AND status = 'pending'`,
		taskID, durationToInterval(delay))
	if err != nil {
		return fmt.Errorf("reschedule task %s: %w", taskID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("reschedule task %s: %w", taskID, ErrNotFound)
	}
	return nil
}
