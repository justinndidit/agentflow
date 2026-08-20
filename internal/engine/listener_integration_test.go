//go:build integration

package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

func TestListener_DeliversNotifications(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	listener := engine.NewListener(dbtest.DSN(t), engine.ReadyChannel, nopLogger())
	wake := make(chan struct{}, 1)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = listener.Run(runCtx, wake) }()

	// Give the subscription time to establish; a notification sent before
	// LISTEN lands is simply lost, which is why the poll floor exists.
	time.Sleep(500 * time.Millisecond)

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	if err := stores.TaskStore.Notify(ctx, engine.ReadyChannel, "hello"); err != nil {
		t.Fatalf("Notify failed: %v", err)
	}

	select {
	case <-wake:
	case <-time.After(10 * time.Second):
		t.Fatal("no wake-up arrived within 10s")
	}
}

// A notification sent inside a transaction that rolls back must never fire —
// otherwise dispatchers wake for a graph that does not exist.
func TestListener_RollbackDoesNotNotify(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	listener := engine.NewListener(dbtest.DSN(t), engine.ReadyChannel, nopLogger())
	wake := make(chan struct{}, 1)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = listener.Run(runCtx, wake) }()
	time.Sleep(500 * time.Millisecond)

	txManager := repositories.NewTxManager(pool, nopLogger())
	err := txManager.WithTransaction(ctx, func(ctx context.Context, stores *repositories.Stores) error {
		if err := stores.TaskStore.Notify(ctx, engine.ReadyChannel, "doomed"); err != nil {
			return err
		}
		return context.Canceled // force a rollback
	})
	if err == nil {
		t.Fatal("expected the transaction to fail")
	}

	select {
	case <-wake:
		t.Fatal("a rolled-back transaction delivered its notification")
	case <-time.After(2 * time.Second):
	}
}

// The fast path, proven by starving the slow one: the poll interval is set far
// beyond the test's patience, so work picked up promptly can only have been
// triggered by a notification.
func TestDispatcher_WakesOnNotificationRatherThanPolling(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	workflow, err := stores.WorkflowStore.CreateWorkflow(ctx, dbtest.NewWorkflowRow())
	if err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	engineRow := models.NewEngineRow("node-a", 4)
	registered, err := stores.EngineStore.Register(ctx, &engineRow)
	if err != nil {
		t.Fatalf("failed to register engine: %v", err)
	}

	listener := engine.NewListener(dbtest.DSN(t), engine.ReadyChannel, nopLogger())
	wake := make(chan struct{}, 1)

	handler := &collector{}
	dispatcher := engine.NewDispatcher(
		stores.TaskStore,
		registered.ID,
		engine.StaticCapacity(4),
		handler,
		fastEngineConfigWithCapacity(4),
		nopLogger(),
		// Ten minutes: if the poll tick is what picks this work up, the test
		// times out long before it fires.
		engine.WithPollInterval(10*time.Minute),
		engine.WithWakeup(wake),
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = listener.Run(runCtx, wake) }()
	go func() { _ = dispatcher.Run(runCtx) }()

	time.Sleep(500 * time.Millisecond)
	if handler.count() != 0 {
		t.Fatalf("handler received %d tasks before any were submitted", handler.count())
	}

	// Insert and notify in one transaction, the way the submit path does.
	err = repositories.NewTxManager(pool, nopLogger()).WithTransaction(ctx,
		func(ctx context.Context, s *repositories.Stores) error {
			err := s.TaskStore.BulkInsertTask(ctx, []*models.TaskRow{
				dbtest.NewTaskRow(workflow.ID, "urgent", "research-agent"),
			})
			if err != nil {
				return err
			}
			return s.TaskStore.Notify(ctx, engine.ReadyChannel, workflow.ID.String())
		})
	if err != nil {
		t.Fatalf("failed to submit work: %v", err)
	}

	deadline := time.After(15 * time.Second)
	for handler.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("work was not picked up; the notification path is not wired through")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// SubmitManifest notifies inside its own transaction, so a node waiting on an
// empty queue starts the moment a manifest lands.
func TestSubmitManifest_NotifiesOnCommit(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent", "matching-agent")

	listener := engine.NewListener(dbtest.DSN(t), engine.ReadyChannel, nopLogger())
	wake := make(chan struct{}, 1)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = listener.Run(runCtx, wake) }()
	time.Sleep(500 * time.Millisecond)

	txManager := repositories.NewTxManager(pool, nopLogger())
	_, err := engine.NewManifestProcessor(nopLogger(), txManager).
		SubmitManifest(ctx, writeManifest(t, diamondManifest))
	if err != nil {
		t.Fatalf("SubmitManifest failed: %v", err)
	}

	select {
	case <-wake:
	case <-time.After(10 * time.Second):
		t.Fatal("submitting a manifest did not notify waiting dispatchers")
	}
}
