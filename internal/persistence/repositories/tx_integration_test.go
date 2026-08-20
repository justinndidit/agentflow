//go:build integration

package repositories_test

import (
	"context"
	"errors"
	"testing"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

// "Workflow and task persistence, transactionally" is the guarantee the submit
// path is built on: a manifest either lands whole or not at all. These tests
// exercise the rollback paths that claim cannot be checked any other way.

func TestWithTransaction_CommitsOnSuccess(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	txManager := repositories.NewTxManager(pool, nopLogger())

	var workflowID string
	err := txManager.WithTransaction(ctx, func(ctx context.Context, s *repositories.Stores) error {
		workflow, err := s.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow())
		if err != nil {
			return err
		}
		workflowID = workflow.ID.String()

		return s.TaskStore.BulkInsertTask(ctx, []*models.TaskRow{
			newTaskRow(workflow.ID, "a", "research-agent"),
			newTaskRow(workflow.ID, "b", "research-agent"),
		})
	})
	if err != nil {
		t.Fatalf("WithTransaction failed: %v", err)
	}

	if count := countRows(t, pool, "workflows"); count != 1 {
		t.Errorf("workflows = %d, want 1", count)
	}
	if count := countRows(t, pool, "tasks"); count != 2 {
		t.Errorf("tasks = %d, want 2", count)
	}
	if workflowID == "" {
		t.Error("expected the callback to have seen the created workflow")
	}
}

// The case that matters: the workflow insert succeeds, the task insert fails,
// and the workflow must not survive. Without the rollback the database would
// hold a workflow claiming a task count it has no tasks for.
func TestWithTransaction_RollsBackWorkflowWhenTasksFail(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	txManager := repositories.NewTxManager(pool, nopLogger())

	err := txManager.WithTransaction(ctx, func(ctx context.Context, s *repositories.Stores) error {
		workflow, err := s.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow())
		if err != nil {
			return err
		}
		// Fails on the foreign key to agents(name), the same way a manifest
		// naming an unregistered agent does.
		return s.TaskStore.BulkInsertTask(ctx, []*models.TaskRow{
			newTaskRow(workflow.ID, "orphan", "never-registered-agent"),
		})
	})
	if err == nil {
		t.Fatal("expected the transaction to fail")
	}

	if count := countRows(t, pool, "workflows"); count != 0 {
		t.Errorf("workflows = %d after rollback, want 0", count)
	}
	if count := countRows(t, pool, "tasks"); count != 0 {
		t.Errorf("tasks = %d after rollback, want 0", count)
	}
}

// The callback's own error is what the caller needs; a successful rollback
// returns nil and must not be allowed to overwrite it into a silent success.
func TestWithTransaction_ReturnsTheCallbackError(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	sentinel := errors.New("callback decided to abort")
	txManager := repositories.NewTxManager(pool, nopLogger())

	err := txManager.WithTransaction(ctx, func(ctx context.Context, s *repositories.Stores) error {
		if _, err := s.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow()); err != nil {
			return err
		}
		return sentinel
	})

	if !errors.Is(err, sentinel) {
		t.Errorf("error = %v, want the callback's own error", err)
	}
	if count := countRows(t, pool, "workflows"); count != 0 {
		t.Errorf("workflows = %d, want the abort to have rolled back", count)
	}
}

// A panic mid-transaction must roll back and then keep panicking — swallowing it
// would leave the caller believing the work committed.
func TestWithTransaction_RollsBackOnPanic(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	txManager := repositories.NewTxManager(pool, nopLogger())

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Error("expected the panic to propagate to the caller")
			}
		}()

		_ = txManager.WithTransaction(ctx, func(ctx context.Context, s *repositories.Stores) error {
			if _, err := s.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow()); err != nil {
				return err
			}
			panic("something went wrong mid-transaction")
		})
	}()

	if count := countRows(t, pool, "workflows"); count != 0 {
		t.Errorf("workflows = %d after a panic, want the transaction rolled back", count)
	}
}

// Writes inside the transaction must not be visible to a connection outside it
// until commit, which is what makes a partially-built graph unobservable to a
// dispatcher running concurrently.
func TestWithTransaction_WritesAreIsolatedUntilCommit(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	txManager := repositories.NewTxManager(pool, nopLogger())
	observed := -1

	err := txManager.WithTransaction(ctx, func(txCtx context.Context, s *repositories.Stores) error {
		if _, err := s.WorkflowStore.CreateWorkflow(txCtx, newWorkflowRow()); err != nil {
			return err
		}
		// countRows uses the pool, not the transaction, so this is a genuinely
		// separate connection.
		observed = countRows(t, pool, "workflows")
		return nil
	})
	if err != nil {
		t.Fatalf("WithTransaction failed: %v", err)
	}

	if observed != 0 {
		t.Errorf("an outside connection saw %d workflows mid-transaction, want 0", observed)
	}
	if count := countRows(t, pool, "workflows"); count != 1 {
		t.Errorf("workflows = %d after commit, want 1", count)
	}
}

// The repository routes through the pool when no transaction is supplied. That
// is the path any caller outside WithTransaction takes, and it has to autocommit
// rather than silently discard the write.
func TestPostgresRepository_WithoutTransactionAutocommits(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	store := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	if _, err := store.WorkflowStore.CreateWorkflow(ctx, newWorkflowRow()); err != nil {
		t.Fatalf("CreateWorkflow failed: %v", err)
	}

	if count := countRows(t, pool, "workflows"); count != 1 {
		t.Errorf("workflows = %d, want the write to have committed", count)
	}
}
