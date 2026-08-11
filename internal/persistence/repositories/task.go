package repositories

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

const tasksTableeName = "tasks"

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

func scanTask(row pgx.Row) (*models.TaskRow, error) {
	var t models.TaskRow

	err := row.Scan(
		&t.ID, &t.WorkflowID, &t.TaskKey, &t.AgentName, &t.Status,
		&t.DependsOn, &t.RemainingDeps, &t.InputTemplate,
		&t.EngineID, &t.LeaseEpoch, &t.LeaseExpiry,
		&t.Priority, &t.NotBefore, &t.Attempt, &t.MaxRetries, &t.Timeout,
		&t.StartedAt, &t.FinishedAt, &t.ErrorMessage, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

// BulkInsertTask writes every task of a workflow using the COPY protocol.
//
// One round trip rather than one per task matters less for speed here than for
// atomicity of intent: the caller runs this inside the same transaction as the
// workflow insert, so a workflow can never exist with a partial task set. COPY
// additionally avoids the 65535-parameter ceiling a multi-row INSERT would hit.
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

	rowsInserted, err := p.repo.CopyFrom(ctx, pgx.Identifier{tasksTableeName}, taskInsertFields, rows)
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
// This is a general-purpose update and deliberately does NOT carry a lease
// guard. Scheduling transitions — claim, complete, fail, reap — must be
// conditional on (engine_id, lease_epoch, status) to be safe under concurrency,
// so they belong in their own methods rather than being expressed through here.
func (p *PostgresTaskStore) UpdateTask(ctx context.Context, task *models.TaskRow) error {
	stmt := `UPDATE tasks SET
			status           = $2::task_status,
			remaining_deps   = $3,
			input_template   = $4::jsonb,
			engine_id        = $5,
			lease_epoch      = $6,
			lease_expires_at = $7,
			priority         = $8,
			not_before       = COALESCE($9, not_before),
			attempt          = $10,
			max_retries      = $11,
			timeout          = $12,
			started_at       = $13,
			finished_at      = $14,
			error_message    = $15,
			updated_at       = now()
		WHERE id = $1`

	tag, err := p.repo.Exec(ctx, stmt,
		task.ID, task.Status, task.RemainingDeps, task.InputTemplate,
		task.EngineID, task.LeaseEpoch, task.LeaseExpiry,
		task.Priority, task.NotBefore, task.Attempt, task.MaxRetries, task.Timeout,
		task.StartedAt, task.FinishedAt, task.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("update task %s: %w", task.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("update task %s: %w", task.ID, ErrNotFound)
	}
	return nil
}

func (p *PostgresTaskStore) GetTaskByID(ctx context.Context, id uuid.UUID) (*models.TaskRow, error) {
	stmt := `SELECT ` + taskColumns + ` FROM tasks WHERE id = $1`

	task, err := scanTask(p.repo.QueryRow(ctx, stmt, id))
	if err != nil {
		return nil, fmt.Errorf("get task %s: %w", id, err)
	}
	return task, nil
}

func (p *PostgresTaskStore) ListTasksByWorkflow(ctx context.Context, workflowID uuid.UUID) ([]*models.TaskRow, error) {
	stmt := `SELECT ` + taskColumns + ` FROM tasks WHERE workflow_id = $1 ORDER BY created_at`

	rows, err := p.repo.Query(ctx, stmt, workflowID)
	if err != nil {
		return nil, fmt.Errorf("list tasks for workflow %s: %w", workflowID, err)
	}
	defer rows.Close()

	tasks := []*models.TaskRow{}
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("list tasks for workflow %s: %w", workflowID, err)
		}
		tasks = append(tasks, task)
	}
	// rows.Err reports a failure that ended iteration early; without it a
	// truncated result set reads as a successful short list.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tasks for workflow %s: %w", workflowID, err)
	}
	return tasks, nil
}
