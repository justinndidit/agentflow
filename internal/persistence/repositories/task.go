package repositories

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

const tasksTableName = "tasks"

// taskInsertFields lists the columns written by BulkInsertTask. The values
// returned by the CopyFromSlice callback below MUST stay in this exact order —
// COPY matches them positionally, so a mismatch shifts values into neighbouring
// columns and only fails if the types happen to disagree.
var taskInsertFields = []string{
	"id", "workflow_id", "task_key", "agent_name", "status",
	"depends_on", "remaining_deps", "input_template",
	"priority", "not_before", "attempt", "max_retries", "timeout",
	"created_at", "updated_at",
}

type TaskStore interface {
	BulkInsertTask(context.Context, []*models.TaskRow) error
	UpdateTask(context.Context, *models.TaskRow) error
	GetTaskByID(context.Context, uuid.UUID) (*models.TaskRow, error)
	ListTasksByWorkflow(context.Context, uuid.UUID) ([]*models.TaskRow, error)
	ClaimTasks(context.Context, uuid.UUID, int, time.Duration) ([]*models.TaskRow, error)
}

type PostgresTaskStore struct {
	repo Repository
}

func NewPostgresTaskStore(repo Repository) *PostgresTaskStore {
	return &PostgresTaskStore{
		repo: repo,
	}
}

// taskColumns is declared once and reused by every statement so the SELECT list
// and the scan order cannot drift apart.
const taskColumns = `id, workflow_id, task_key, agent_name, status,
	depends_on, remaining_deps, input_template,
	engine_id, lease_epoch, lease_expires_at,
	priority, not_before, attempt, max_retries, timeout,
	started_at, finished_at, error_message, created_at, updated_at`

func (p *PostgresTaskStore) BulkInsertTask(ctx context.Context, tasks []*models.TaskRow) error {
	if len(tasks) == 0 {
		return nil
	}

	rows := pgx.CopyFromSlice(len(tasks), func(i int) ([]any, error) {
		task := tasks[i]

		// COPY has no SQL expression layer, so there is nowhere to COALESCE.
		// Listing a column and supplying nil writes NULL — column DEFAULTs apply
		// only to columns left out of the copy entirely. Both of these are
		// NOT NULL, so the zero values have to be supplied here.
		dependsOn := task.DependsOn
		if dependsOn == nil {
			dependsOn = []string{}
		}
		notBefore := task.NotBefore
		if notBefore == nil {
			notBefore = &task.CreatedAt
		}

		return []any{
			task.ID, task.WorkflowID, task.TaskKey, task.AgentName, task.Status,
			dependsOn, task.RemainingDeps, task.InputTemplate,
			task.Priority, notBefore, task.Attempt, task.MaxRetries, task.Timeout,
			task.CreatedAt, task.UpdatedAt,
		}, nil
	})

	rowsInserted, err := p.repo.CopyFrom(ctx, pgx.Identifier{tasksTableName}, taskInsertFields, rows)
	if err != nil {
		return fmt.Errorf("bulk insert %d tasks: %w", len(tasks), err)
	}

	if rowsInserted != int64(len(tasks)) {
		return fmt.Errorf("bulk insert tasks: expected %d rows, inserted %d", len(tasks), rowsInserted)
	}

	return nil
}

// UpdateTask writes the mutable fields of a task by id.
//
// Unguarded and last-writer-wins: it matches on id alone. That is correct for
// administrative edits, and wrong for scheduling. Claim, complete, fail and reap
// each need their own statement conditional on (status, engine_id, lease_epoch),
// because for those the WHERE clause is the state machine and a zero-row result
// means "the lease moved on", not "no such task".
func (p *PostgresTaskStore) UpdateTask(ctx context.Context, task *models.TaskRow) error {
	stmt := `UPDATE tasks SET
			status           = @status,
			depends_on       = @dependsOn,
			remaining_deps   = @remainingDeps,
			input_template   = @inputTemplate,
			engine_id        = @engineID,
			lease_epoch      = @leaseEpoch,
			lease_expires_at = @leaseExpiresAt,
			priority         = @priority,
			not_before       = COALESCE(@notBefore, not_before),
			attempt          = @attempt,
			max_retries      = @maxRetries,
			timeout          = @timeout,
			started_at       = @startedAt,
			finished_at      = @finishedAt,
			error_message    = @errorMessage,
			updated_at       = @updatedAt
		WHERE id = @id`

	// depends_on is NOT NULL; a nil slice would be sent as NULL.
	dependsOn := task.DependsOn
	if dependsOn == nil {
		dependsOn = []string{}
	}

	// Exec, not Query. pgx defers a Query until its rows are read, so an UPDATE
	// sent through Query may not run at all, and its error surfaces only on
	// rows.Close(). rows.CommandTag() is likewise empty until the rows have been
	// consumed, so checking RowsAffected on it reports 0 even on success.
	tag, err := p.repo.Exec(ctx, stmt, pgx.NamedArgs{
		"id":             task.ID,
		"status":         task.Status,
		"dependsOn":      dependsOn,
		"remainingDeps":  task.RemainingDeps,
		"inputTemplate":  task.InputTemplate,
		"engineID":       task.EngineID,
		"leaseEpoch":     task.LeaseEpoch,
		"leaseExpiresAt": task.LeaseExpiry,
		"priority":       task.Priority,
		"notBefore":      task.NotBefore,
		"attempt":        task.Attempt,
		"maxRetries":     task.MaxRetries,
		"timeout":        task.Timeout,
		"startedAt":      task.StartedAt,
		"finishedAt":     task.FinishedAt,
		"errorMessage":   task.ErrorMessage,
		"updatedAt":      time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("update task %s: %w", task.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("update task %s: %w", task.ID, ErrNotFound)
	}
	return nil
}

func (p *PostgresTaskStore) GetTaskByID(ctx context.Context, id uuid.UUID) (*models.TaskRow, error) {
	stmt := `SELECT ` + taskColumns + ` FROM tasks WHERE id = @id`

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{
		"id": id,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query to fetch task %s: %w", id, err)
	}

	// CollectOneRow closes rows itself, including on the error paths.
	task, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[models.TaskRow])
	if err != nil {
		// Translate at the boundary so callers can test with errors.Is(err,
		// ErrNotFound) instead of importing pgx to recognise absence.
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get task %s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("failed to collect one row from table:tasks for task %s: %w", id, err)
	}
	return task, nil
}

func (p *PostgresTaskStore) ListTasksByWorkflow(ctx context.Context, workflowID uuid.UUID) ([]*models.TaskRow, error) {
	stmt := `SELECT ` + taskColumns + ` FROM tasks WHERE workflow_id = @workflowID ORDER BY created_at`

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{"workflowID": workflowID})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query to get all tasks attached to workflow %s: %w", workflowID, err)
	}

	tasks, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[models.TaskRow])
	if err != nil {
		return nil, fmt.Errorf("failed to collect rows from table:tasks for workflow %s: %w", workflowID, err)
	}
	return tasks, nil
}

// ClaimTasks atomically takes up to limit ready tasks for engineID and returns
// the rows as claimed, leased for leaseTTL.
//
// This is the hot path of the whole system, and it is one statement on purpose:
// selecting candidates and then updating them in a second round trip leaves a
// window where another node claims the same row.
//
// FOR UPDATE SKIP LOCKED is what makes it contention-free. Two nodes racing for
// the same row do not queue behind one another — the loser skips it and takes
// the next one instead. Fewer than limit rows coming back therefore means
// "nothing more was free right now", not "nothing more exists".
//
// Four conditions define readiness, and each one is load-bearing:
//
//   - status = 'pending'      — not already running, finished or cancelled.
//   - remaining_deps = 0      — every dependency has committed.
//   - not_before <= now()     — past its retry backoff, using the database
//     clock so a node with a fast clock cannot claim work early.
//   - attempt <= max_retries  — the retry budget still has room for the
//     increment below.
//
// That last one is not in the architecture doc and is not optional. The claim
// sets attempt = attempt + 1 across the whole batch in a single statement, and
// the schema enforces CHECK (attempt <= max_retries + 1). A row already at its
// ceiling would abort the entire statement rather than just itself, so one
// exhausted task would stall every claim this node makes until something else
// moved it. Filtering it out here means it is never selected; the reaper is
// responsible for moving it to failed.
//
// The lease is deliberately not conditional on the engine's own view of its
// capacity: limit is the caller's free-slot count, because claiming work you
// cannot start means holding a lease you cannot honour.
//
// max_parallelism is intentionally not enforced here. Doing so would require
// incrementing workflows.running_count inside this statement, which serialises
// every claim for that workflow behind one row lock — fine across many small
// workflows and a hard bottleneck inside one large one.
func (p *PostgresTaskStore) ClaimTasks(
	ctx context.Context,
	engineID uuid.UUID,
	limit int,
	leaseTTL time.Duration,
) ([]*models.TaskRow, error) {
	// A node with no free slots asks for nothing rather than issuing a query
	// that would claim work it cannot run.
	if limit <= 0 {
		return nil, nil
	}

	stmt := `UPDATE tasks
	    SET status           = 'running',
	        engine_id        = @engineID,
	        lease_epoch      = lease_epoch + 1,
	        lease_expires_at = now() + @leaseTTL::interval,
	        attempt          = attempt + 1,
	        started_at       = COALESCE(started_at, now()),
	        updated_at       = now()
	  WHERE id IN (
	        SELECT id
	          FROM tasks
	         WHERE status = 'pending'
	           AND remaining_deps = 0
	           AND not_before <= now()
	           AND attempt <= max_retries
	         ORDER BY priority DESC, created_at
	         LIMIT @limit
	         FOR UPDATE SKIP LOCKED
	  )
	RETURNING ` + taskColumns

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{
		"engineID": engineID,
		"leaseTTL": durationToInterval(leaseTTL),
		"limit":    limit,
	})
	if err != nil {
		return nil, fmt.Errorf("claim up to %d tasks for engine %s: %w", limit, engineID, err)
	}

	claimed, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[models.TaskRow])
	if err != nil {
		return nil, fmt.Errorf("collect claimed tasks for engine %s: %w", engineID, err)
	}
	return claimed, nil
}
