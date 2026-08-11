package repositories

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/dtos"
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
	UpdateTask(context.Context, uuid.UUID, *dtos.UpdateTaskDTO) error
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

func (p *PostgresTaskStore) UpdateTask(ctx context.Context, taskID uuid.UUID, task *dtos.UpdateTaskDTO) error {
	stmt := "UPDATE tasks SET "

	args := pgx.NamedArgs{
		"id": taskID,
	}
	setClauses := []string{}

	if task.Status != nil {
		setClauses = append(setClauses, "status = @taskStatus")
		args["status"] = *task.Status
	}

	if task.Attempt != nil {
		setClauses = append(setClauses, "attempt = @attempt")
		args["attempt"] = *task.Attempt
	}

	if task.StartedAt != nil {
		setClauses = append(setClauses, "started_at = @startedAt")
		args["startedAt"] = *task.StartedAt
	}
	if task.DependsOn != nil {
		setClauses = append(setClauses, "depends_on = @dependsOn")
		args["dependsOn"] = task.DependsOn
	}
	if task.FinishedAt != nil {
		setClauses = append(setClauses, "finished_at = @finishedAt")
		args["finishedAt"] = *task.FinishedAt
	}
	if task.ErrorMessage != nil {
		setClauses = append(setClauses, "error_message = @errorMessage")
		args["errorMessage"] = *task.ErrorMessage
	}
	stmt += strings.Join(setClauses, ", ")
	stmt += " WHERE id = @id"

	_, err := p.repo.Query(ctx, stmt, args)
	if err != nil {
		return fmt.Errorf("failed to execute update query for task %s", taskID)
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

	task, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[models.TaskRow])
	if err != nil {
		return nil, fmt.Errorf("failed to collect one row from table:tasks for task %s: %w", id, err)
	}
	return task, nil
}

func (p *PostgresTaskStore) ListTasksByWorkflow(ctx context.Context, workflowID uuid.UUID) ([]*models.TaskRow, error) {
	stmt := `SELECT ` + taskColumns + ` FROM tasks WHERE workflow_id = $1 ORDER BY created_at`

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{"workflow_id": workflowID})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query to get all tasks attached to workflow %s: %w", workflowID, err)
	}

	tasks, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[models.TaskRow])
	if err != nil {
		return nil, fmt.Errorf("failed to collect rows from table:tasks for workflow %s: %w", workflowID, err)
	}
	return tasks, nil
}
