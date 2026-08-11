package repositories

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func (p *PostgresWorkflowStore) CreateWorkflow(ctx context.Context, workflow *models.WorkflowRow) (*models.WorkflowRow, error) {
	// status is cast explicitly because workflow_status is an enum; without the
	// cast Postgres cannot infer the parameter type from a bare string.
	stmt := `INSERT INTO workflows (
		id, name, namespace, manifest, version, status,
		task_total, task_completed, task_failed, task_cancelled,
		max_parallelism, running_count, max_tokens, tokens_used,
		default_timeout, created_at, updated_at
	) VALUES (
		@workflowID, @workflowName, @workflowNamespace, @workflowManifest, @workflowVersion, @workflowStatus, @taskTotal,
		@taskCompleted, @taskFailed, @taskCancelled, @maxParallelism,
		@runningCount, @maxTokens, @tokensUsed, @defaultTimeout,
		@createdAt, @updatedAt
	) RETURNING ` + workflowColumns

	args := pgx.NamedArgs{
		"workflowID":        workflow.ID,
		"workflowName":      workflow.WorkflowName,
		"workflowNamespace": workflow.WorkflowNameSpace,
		"workflowManifest":  workflow.Manifest,
		"workflowVersion":   workflow.Version,
		"workflowStatus":    workflow.Status,
		"taskTotal":         workflow.TaskCount,
		"taskCompleted":     workflow.TaskCompleted,
		"taskFailed":        workflow.TaskFailed,
		"taskCancelled":     workflow.TaskCancelled,
		"maxParallelism":    workflow.MaxParallelism,
		"runningCount":      workflow.RunningCount,
		"maxTokens":         workflow.MaxTokensPerRun,
		"tokensUsed":        workflow.TokensUsed,
		"defaultTimeout":    workflow.DefaultTimeout,
		"createdAt":         workflow.CreatedAt,
		"updatedAt":         workflow.UpdatedAt,
	}

	rows, err := p.repo.Query(ctx, stmt, args)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query to create workflow: %w", err)
	}

	created, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[models.WorkflowRow])
	if err != nil {
		return nil, fmt.Errorf("failed to collect from table:workflows: %w", err)
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
		FROM workflows WHERE name = @workflowName
		ORDER BY created_at DESC
		LIMIT 1`

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{
		"workflowName": name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query to get workflow with name %s: %w", name, err)
	}

	workflow, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[models.WorkflowRow])
	if err != nil {
		return nil, fmt.Errorf("failed to collect workflow from table:workflows for workflow %s: %w", name, err)
	}
	return workflow, nil
}

func (p *PostgresWorkflowStore) GetWorkflowByID(ctx context.Context, id uuid.UUID) (*models.WorkflowRow, error) {
	stmt := `SELECT ` + workflowColumns + ` FROM workflows WHERE id = @workflowID`

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{
		"workflowID": id,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute query to get workflow %s: %w", id, err)
	}

	workflow, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[models.WorkflowRow])
	if err != nil {
		return nil, fmt.Errorf("failed to collect workflow from table:workflows for workflow %s: %w", id, err)
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
