package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/state"
)

// ReclaimReason records why a task was taken back from the node holding it.
type ReclaimReason string

const (
	// ReasonLeaseExpired is a task that overran its own lease on a node that is
	// still alive — a hung container, or work that simply took longer than the
	// lease allowed.
	ReasonLeaseExpired ReclaimReason = "lease expired"

	// ReasonEngineLost is a task on a node that stopped heartbeating or shut
	// down while still holding it.
	ReasonEngineLost ReclaimReason = "engine unavailable"
)

// Reclaimed describes one task the reaper took back and what became of it.
type Reclaimed struct {
	TaskID     uuid.UUID
	WorkflowID uuid.UUID
	TaskKey    string
	Attempt    int

	// Status is what the task became: pending if it had retries left, failed if
	// its budget was exhausted.
	Status state.TaskStatus
	Reason ReclaimReason
}

// Retryable reports whether the task went back to the queue rather than being
// finished for good.
func (r Reclaimed) Retryable() bool { return r.Status == state.PendingTaskStatus }

// ReclaimExpired takes back up to limit tasks whose owner can no longer be
// trusted to finish them, and decides in the same statement whether each one
// retries or is finished for good.
//
// Two conditions reclaim a task, and they are genuinely different failures:
//
//   - lease_expires_at < now() — the node is alive but the work overran its
//     lease. A hung container is the usual cause.
//   - the owning engine stopped heartbeating, or is marked stopped. A node that
//     shut down cleanly releases its work immediately rather than after a
//     timeout, since it is definitely not still running it.
//
// Liveness is read from the engine rather than the task. A per-task heartbeat
// would cost one write per running task per interval on the hottest table in
// the system; this joins to one row per node instead.
//
// Concurrent reaping is safe, which is what lets every node run this loop with
// no leader election. The safety does not come from the row lock, though: it
// comes from the reclaim conditions clearing themselves. A reclaimed task has
// its engine_id and lease_expires_at set to NULL, so it no longer matches either
// arm of the predicate and a second reaper cannot pick it up again — verified by
// removing SKIP LOCKED, which changes nothing about the outcome.
//
// SKIP LOCKED is here for throughput rather than correctness: without it, two
// reapers working the same backlog serialise behind each other's row locks
// instead of taking disjoint slices.
//
// The lease epoch is deliberately NOT bumped here. Returning the task to pending
// clears engine_id, so the previous owner's fenced write already fails its
// guard; the epoch advances on the next claim, which is what neutralises the old
// owner even if the same node claims the task again.
func (p *PostgresTaskStore) ReclaimExpired(
	ctx context.Context,
	leaseTTL time.Duration,
	backoff time.Duration,
	limit int,
) ([]Reclaimed, error) {
	if limit <= 0 {
		return nil, nil
	}

	// Every SET expression reads the pre-update row, so the CASE arms below see
	// the original lease_expires_at even though the same statement nulls it.
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
	        error_message = CASE
	            WHEN lease_expires_at IS NOT NULL AND lease_expires_at < now()
	            THEN @leaseExpired
	            ELSE @engineLost END,
	        updated_at = now()
	  WHERE id IN (
	        SELECT t.id
	          FROM tasks t
	         WHERE t.status = 'running'
	           AND (
	                (t.lease_expires_at IS NOT NULL AND t.lease_expires_at < now())
	                OR t.engine_id IN (
	                    SELECT e.id FROM engines e
	                     WHERE e.heartbeat_at < now() - @leaseTTL::interval
	                        OR e.status = 'stopped'
	                )
	           )
	         ORDER BY t.lease_expires_at NULLS FIRST
	         LIMIT @limit
	         FOR UPDATE SKIP LOCKED
	  )
	RETURNING id, workflow_id, task_key, attempt, status, error_message`

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{
		"leaseTTL":     durationToInterval(leaseTTL),
		"backoff":      durationToInterval(backoff),
		"limit":        limit,
		"leaseExpired": string(ReasonLeaseExpired),
		"engineLost":   string(ReasonEngineLost),
	})
	if err != nil {
		return nil, fmt.Errorf("reclaim up to %d expired tasks: %w", limit, err)
	}
	defer rows.Close()

	reclaimed := []Reclaimed{}
	for rows.Next() {
		var (
			item   Reclaimed
			reason string
		)
		err := rows.Scan(&item.TaskID, &item.WorkflowID, &item.TaskKey,
			&item.Attempt, &item.Status, &reason)
		if err != nil {
			return nil, fmt.Errorf("scan reclaimed task: %w", err)
		}
		item.Reason = ReclaimReason(reason)
		reclaimed = append(reclaimed, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reclaim expired tasks: %w", err)
	}
	return reclaimed, nil
}
