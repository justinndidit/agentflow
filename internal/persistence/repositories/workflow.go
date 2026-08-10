package repositories

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

type WorkflowStore interface {
	CreateWorkflow(context.Context, *models.WorkflowRow) (*models.WorkflowRow, error)
	GetWorkflowByName(context.Context, string) (*models.WorkflowRow, error)
	GetWorkflowByID(context.Context, uuid.UUID) (*models.WorkflowRow, error)
	DeleteWorkflow(context.Context, uuid.UUID) error
}

type PostgresWorkflowStore struct {
	repo Repository
}

func NewPostgresWorkflowStore(repo Repository) *PostgresWorkflowStore {
	return &PostgresWorkflowStore{
		repo: repo,
	}
}

// workflowColumns is declared once and reused by every statement so the SELECT
// list and the scan order cannot drift apart.
const workflowColumns = `id, name, namespace, manifest, version, status,
	task_total, task_completed, task_failed, task_cancelled,
	COALESCE(max_parallelism, 0), running_count,
	COALESCE(max_tokens, 0), tokens_used,
	default_timeout, created_at, updated_at`

// scanWorkflow reads one row in the order declared by workflowColumns.
func scanWorkflow(row pgx.Row) (*models.WorkflowRow, error) {
	var w models.WorkflowRow
	var timeout pgtype.Interval

	err := row.Scan(
		&w.ID, &w.WorkflowName, &w.WorkflowNameSpace, &w.Manifest, &w.Version, &w.Status,
		&w.TaskCount, &w.TaskCompleted, &w.TaskFailed, &w.TaskCancelled,
		&w.MaxParallelism, &w.RunningCount,
		&w.MaxTokensPerRun, &w.TokensUsed,
		&timeout, &w.CreatedAt, &w.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	w.DefaultTimeout = intervalToDuration(timeout)
	return &w, nil
}

func (p *PostgresWorkflowStore) CreateWorkflow(ctx context.Context, workflow *models.WorkflowRow) (*models.WorkflowRow, error) {
	// status is cast explicitly because workflow_status is an enum; without the
	// cast Postgres cannot infer the parameter type from a bare string.
	stmt := `INSERT INTO workflows (
		id, name, namespace, manifest, version, status,
		task_total, task_completed, task_failed, task_cancelled,
		max_parallelism, running_count, max_tokens, tokens_used,
		default_timeout, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6::workflow_status,
		$7, $8, $9, $10,
		$11, $12, $13, $14,
		$15, $16, $17
	) RETURNING ` + workflowColumns

	row := p.repo.QueryRow(ctx, stmt,
		workflow.ID, workflow.WorkflowName, workflow.WorkflowNameSpace,
		workflow.Manifest, workflow.Version, workflow.Status,
		workflow.TaskCount, workflow.TaskCompleted, workflow.TaskFailed, workflow.TaskCancelled,
		workflow.MaxParallelism, workflow.RunningCount,
		workflow.MaxTokensPerRun, workflow.TokensUsed,
		durationToInterval(workflow.DefaultTimeout),
		workflow.CreatedAt, workflow.UpdatedAt,
	)

	created, err := scanWorkflow(row)
	if err != nil {
		return nil, fmt.Errorf("create workflow %s: %w", workflow.WorkflowName, err)
	}
	return created, nil
}

// GetWorkflowByName returns the most recently created workflow with this name.
//
// Name is not unique — the same manifest submitted twice produces two rows — so
// this deliberately resolves the ambiguity by recency rather than erroring.
// Callers that need a specific run should use GetWorkflowByID.
func (p *PostgresWorkflowStore) GetWorkflowByName(ctx context.Context, name string) (*models.WorkflowRow, error) {
	stmt := `SELECT ` + workflowColumns + `
		FROM workflows WHERE name = $1
		ORDER BY created_at DESC
		LIMIT 1`

	workflow, err := scanWorkflow(p.repo.QueryRow(ctx, stmt, name))
	if err != nil {
		return nil, fmt.Errorf("get workflow by name %s: %w", name, err)
	}
	return workflow, nil
}

func (p *PostgresWorkflowStore) GetWorkflowByID(ctx context.Context, id uuid.UUID) (*models.WorkflowRow, error) {
	stmt := `SELECT ` + workflowColumns + ` FROM workflows WHERE id = $1`

	workflow, err := scanWorkflow(p.repo.QueryRow(ctx, stmt, id))
	if err != nil {
		return nil, fmt.Errorf("get workflow by id %s: %w", id, err)
	}
	return workflow, nil
}

// DeleteWorkflow removes a workflow. Its tasks go with it via ON DELETE CASCADE,
// and task_results follow from the cascade on tasks.
func (p *PostgresWorkflowStore) DeleteWorkflow(ctx context.Context, id uuid.UUID) error {
	tag, err := p.repo.Exec(ctx, `DELETE FROM workflows WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete workflow %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("delete workflow %s: %w", id, ErrNotFound)
	}
	return nil
}
