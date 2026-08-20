package repositories

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/state"
)

type WorkflowStore interface {
	CreateWorkflow(context.Context, *models.WorkflowRow) (*models.WorkflowRow, error)
	GetWorkflowByName(context.Context, string) (*models.WorkflowRow, error)
	GetWorkflowByID(context.Context, uuid.UUID) (*models.WorkflowRow, error)
	DeleteWorkflow(context.Context, uuid.UUID) error
	RecordTaskOutcome(context.Context, uuid.UUID, state.TaskStatus, int, int64) error
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
	max_parallelism, running_count,
	max_tokens, tokens_used,
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
		// Translate at the boundary so callers can test with errors.Is(err,
		// ErrNotFound) instead of importing pgx to recognise absence.
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get workflow %s: %w", name, ErrNotFound)
		}
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
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get workflow %s: %w", id, ErrNotFound)
		}
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

// RecordTaskOutcome moves a workflow's counters and, when the last task lands,
// its status.
//
// Counters are maintained transactionally rather than derived. The alternative —
// SELECT count(*) ... GROUP BY status on every completion — is correct but turns
// each finish into a scan of that workflow's tasks, so the cost of finishing one
// task grows with how many tasks the workflow has.
//
// cancelled takes a count rather than a flag because a single permanent failure
// cascades to an arbitrary number of dependents, and they are all cancelled in
// the same transaction that failed the task.
//
// A workflow is finished when completed + failed + cancelled reaches task_total.
// It ends 'failed' if anything failed or was cancelled, and 'completed' only if
// every task succeeded — a run that lost a branch did not succeed, even though
// every task that could finish did.
func (p *PostgresWorkflowStore) RecordTaskOutcome(
	ctx context.Context,
	workflowID uuid.UUID,
	outcome state.TaskStatus,
	cancelled int,
	tokensUsed int64,
) error {
	stmt := `UPDATE workflows SET
	        task_completed = task_completed + @completedDelta,
	        task_failed    = task_failed + @failedDelta,
	        task_cancelled = task_cancelled + @cancelledDelta,
	        tokens_used    = tokens_used + @tokensUsed,
	        status = CASE
	            WHEN task_completed + @completedDelta
	               + task_failed + @failedDelta
	               + task_cancelled + @cancelledDelta >= task_total
	            THEN CASE
	                WHEN task_failed + @failedDelta
	                   + task_cancelled + @cancelledDelta > 0
	                THEN 'failed'::workflow_status
	                ELSE 'completed'::workflow_status
	            END
	            ELSE 'running'::workflow_status
	        END,
	        updated_at = now()
	  WHERE id = @workflowID`

	completedDelta, failedDelta := 0, 0
	switch outcome {
	case state.CompletedTaskStatus:
		completedDelta = 1
	case state.FailedTaskStatus:
		failedDelta = 1
	}

	tag, err := p.repo.Exec(ctx, stmt, pgx.NamedArgs{
		"workflowID":     workflowID,
		"completedDelta": completedDelta,
		"failedDelta":    failedDelta,
		"cancelledDelta": cancelled,
		"tokensUsed":     tokensUsed,
	})
	if err != nil {
		return fmt.Errorf("record %s outcome on workflow %s: %w", outcome, workflowID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("record %s outcome on workflow %s: %w", outcome, workflowID, ErrNotFound)
	}
	return nil
}
