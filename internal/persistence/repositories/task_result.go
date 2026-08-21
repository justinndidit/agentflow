package repositories

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

type TaskResultStore interface {
	Insert(context.Context, *models.TaskResult) error
	GetByAttempt(context.Context, uuid.UUID, int) (*models.TaskResult, error)
	ListByTask(context.Context, uuid.UUID) ([]*models.TaskResult, error)
	OutputsByTaskKey(context.Context, uuid.UUID, []string) (map[string][]byte, error)
}

type PostgresTaskResultStore struct {
	repo Repository
}

func NewPostgresTaskResultStore(repo Repository) *PostgresTaskResultStore {
	return &PostgresTaskResultStore{repo: repo}
}

const taskResultColumns = `task_id, attempt, output, artifact_uri, resolved_input,
	tokens_used, cost_micros, duration_ms, created_at`

// Insert records the outcome of one attempt.
//
// The primary key is (task_id, attempt), so a retry adds a row rather than
// overwriting the attempt that failed. That is deliberate: the failed output and
// the resolved input that produced it are usually the only evidence of why a
// workflow went wrong, and overwriting them destroys it at exactly the moment
// someone needs it.
func (p *PostgresTaskResultStore) Insert(ctx context.Context, result *models.TaskResult) error {
	stmt := `INSERT INTO task_results (` + taskResultColumns + `)
	VALUES (@taskID, @attempt, @output, @artifactURI, @resolvedInput,
	        @tokensUsed, @costMicros, @durationMS, @createdAt)`

	_, err := p.repo.Exec(ctx, stmt, pgx.NamedArgs{
		"taskID":        result.TaskID,
		"attempt":       result.Attempt,
		"output":        result.Output,
		"artifactURI":   result.ArtifactURI,
		"resolvedInput": result.ResolvedInput,
		"tokensUsed":    result.TokensUsed,
		"costMicros":    result.CostMicros,
		"durationMS":    result.DurationMS,
		"createdAt":     result.CreatedAt,
	})
	if err != nil {
		return fmt.Errorf("insert result for task %s attempt %d: %w",
			result.TaskID, result.Attempt, err)
	}
	return nil
}

func (p *PostgresTaskResultStore) GetByAttempt(ctx context.Context, taskID uuid.UUID, attempt int) (*models.TaskResult, error) {
	rows, err := p.repo.Query(ctx,
		`SELECT `+taskResultColumns+` FROM task_results
		  WHERE task_id = @taskID AND attempt = @attempt`,
		pgx.NamedArgs{"taskID": taskID, "attempt": attempt})
	if err != nil {
		return nil, fmt.Errorf("get result for task %s attempt %d: %w", taskID, attempt, err)
	}

	result, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[models.TaskResult])
	if err != nil {
		if isNoRows(err) {
			return nil, fmt.Errorf("get result for task %s attempt %d: %w", taskID, attempt, ErrNotFound)
		}
		return nil, fmt.Errorf("collect result for task %s attempt %d: %w", taskID, attempt, err)
	}
	return result, nil
}

// ListByTask returns every attempt's result, oldest first, so a caller
// inspecting a failure can read the sequence that led to it.
func (p *PostgresTaskResultStore) ListByTask(ctx context.Context, taskID uuid.UUID) ([]*models.TaskResult, error) {
	rows, err := p.repo.Query(ctx,
		`SELECT `+taskResultColumns+` FROM task_results
		  WHERE task_id = @taskID ORDER BY attempt`,
		pgx.NamedArgs{"taskID": taskID})
	if err != nil {
		return nil, fmt.Errorf("list results for task %s: %w", taskID, err)
	}

	results, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[models.TaskResult])
	if err != nil {
		return nil, fmt.Errorf("collect results for task %s: %w", taskID, err)
	}
	return results, nil
}

// OutputsByTaskKey returns the output of each named task in a workflow, keyed by
// task key, for resolving a downstream task's input templates.
//
// The join pins r.attempt to t.attempt, so what comes back is the result of the
// attempt that actually completed rather than the newest row for that task. A
// task that failed twice and then succeeded has three result rows, and only the
// third describes the output its dependents were promised.
//
// Scoped by workflow_id because depends_on holds task keys, which the schema
// makes unique only within a workflow. Resolving against another run's output
// would feed a worker data from a workflow it has nothing to do with.
//
// Keys with no completed task are simply absent from the map. The caller
// decides whether that is an error — for a task whose dependencies have all
// committed, it always is.
func (p *PostgresTaskResultStore) OutputsByTaskKey(
	ctx context.Context,
	workflowID uuid.UUID,
	taskKeys []string,
) (map[string][]byte, error) {
	outputs := map[string][]byte{}
	if len(taskKeys) == 0 {
		return outputs, nil
	}

	rows, err := p.repo.Query(ctx,
		`SELECT t.task_key, r.output
		   FROM tasks t
		   JOIN task_results r ON r.task_id = t.id AND r.attempt = t.attempt
		  WHERE t.workflow_id = @workflowID
		    AND t.task_key = ANY(@taskKeys)
		    AND t.status = 'completed'`,
		pgx.NamedArgs{"workflowID": workflowID, "taskKeys": taskKeys})
	if err != nil {
		return nil, fmt.Errorf("load outputs for workflow %s: %w", workflowID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			key    string
			output []byte
		)
		if err := rows.Scan(&key, &output); err != nil {
			return nil, fmt.Errorf("scan output for workflow %s: %w", workflowID, err)
		}
		outputs[key] = output
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load outputs for workflow %s: %w", workflowID, err)
	}
	return outputs, nil
}
