//go:build integration

package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"

	"github.com/jackc/pgx/v5/pgxpool"
)

func engineStore(pool *pgxpool.Pool) repositories.EngineStore {
	return repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil)).EngineStore
}

// A one-second heartbeat keeps the loop tests quick; production defaults are in
// config.defaults.
func fastEngineConfig() *config.Engine {
	return fastEngineConfigWithCapacity(4)
}

func fastEngineConfigWithCapacity(capacity int) *config.Engine {
	return &config.Engine{Capacity: capacity, HeartbeatInterval: 1, LeaseTTL: 60}
}

func TestRegistrar_RegistersOnBoot(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	registrar := engine.NewRegistrar(engineStore(pool), fastEngineConfig(), nopLogger())
	if registrar.ID() != uuid.Nil {
		t.Error("ID before Register should be uuid.Nil")
	}

	registered, err := registrar.Register(ctx)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if registrar.ID() != registered.ID {
		t.Errorf("ID = %s, want %s", registrar.ID(), registered.ID)
	}
	if registered.Capacity != 4 {
		t.Errorf("Capacity = %d, want 4", registered.Capacity)
	}
	if registered.HostName == "" {
		t.Error("HostName is empty; the registrar should fall back rather than leave it blank")
	}
	if registered.Status != string(state.ActiveEngineStatus) {
		t.Errorf("Status = %q, want active", registered.Status)
	}
}

// Run heartbeats on a ticker until its context is cancelled.
func TestRegistrar_HeartbeatsUntilCancelled(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := engineStore(pool)

	registrar := engine.NewRegistrar(store, fastEngineConfig(), nopLogger())
	registered, err := registrar.Register(ctx)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- registrar.Run(runCtx) }()

	// Two ticks plus slack, so the assertion does not race the first one.
	time.Sleep(2500 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	got, err := store.GetEngineByID(ctx, registered.ID)
	if err != nil {
		t.Fatalf("GetEngineByID failed: %v", err)
	}
	if !got.HeartBeatAt.After(registered.HeartBeatAt) {
		t.Errorf("heartbeat_at %v did not advance past registration %v",
			got.HeartBeatAt, registered.HeartBeatAt)
	}
}

// Run cannot heartbeat a row that does not exist yet, and silently doing
// nothing would leave the node looking dead to every reaper in the fleet.
func TestRegistrar_RunBeforeRegister(t *testing.T) {
	pool := dbtest.Pool(t)

	registrar := engine.NewRegistrar(engineStore(pool), fastEngineConfig(), nopLogger())
	if err := registrar.Run(context.Background()); err == nil {
		t.Fatal("expected Run before Register to fail")
	}
}

func TestRegistrar_StatusBeforeRegister(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	registrar := engine.NewRegistrar(engineStore(pool), fastEngineConfig(), nopLogger())
	if err := registrar.Drain(ctx); err == nil {
		t.Error("expected Drain before Register to fail")
	}
	if err := registrar.Stop(ctx); err == nil {
		t.Error("expected Stop before Register to fail")
	}
}

// Graceful shutdown goes active -> draining -> stopped. Draining is the window
// where the node has stopped claiming but is still finishing work, so its tasks
// must not become reclaimable until Stop.
func TestRegistrar_GracefulShutdownSequence(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := engineStore(pool)

	registrar := engine.NewRegistrar(store, fastEngineConfig(), nopLogger())
	registered, err := registrar.Register(ctx)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if err := registrar.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	draining, err := store.GetEngineByID(ctx, registered.ID)
	if err != nil {
		t.Fatalf("GetEngineByID failed: %v", err)
	}
	if draining.Status != string(state.DrainingEngineStatus) {
		t.Errorf("Status after Drain = %q, want draining", draining.Status)
	}

	if err := registrar.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	stopped, err := store.GetEngineByID(ctx, registered.ID)
	if err != nil {
		t.Fatalf("GetEngineByID failed: %v", err)
	}
	if stopped.Status != string(state.StoppedEngineStatus) {
		t.Errorf("Status after Stop = %q, want stopped", stopped.Status)
	}
}

// A draining node keeps heartbeating so the reaper leaves the work it is
// finishing alone. Draining must not stop the loop.
func TestRegistrar_KeepsHeartbeatingWhileDraining(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := engineStore(pool)

	registrar := engine.NewRegistrar(store, fastEngineConfig(), nopLogger())
	registered, err := registrar.Register(ctx)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = registrar.Run(runCtx) }()

	if err := registrar.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	before, err := store.GetEngineByID(ctx, registered.ID)
	if err != nil {
		t.Fatalf("GetEngineByID failed: %v", err)
	}
	time.Sleep(2500 * time.Millisecond)
	after, err := store.GetEngineByID(ctx, registered.ID)
	if err != nil {
		t.Fatalf("GetEngineByID failed: %v", err)
	}

	if !after.HeartBeatAt.After(before.HeartBeatAt) {
		t.Errorf("a draining node stopped heartbeating: %v then %v",
			before.HeartBeatAt, after.HeartBeatAt)
	}
	if after.Status != string(state.DrainingEngineStatus) {
		t.Errorf("Status = %q, want the node to stay draining", after.Status)
	}
}

// Two nodes against one database get separate identities and heartbeat
// independently — the base case for everything in the execution path.
func TestRegistrar_TwoNodesAreIndependent(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := engineStore(pool)

	first := engine.NewRegistrar(store, fastEngineConfig(), nopLogger())
	second := engine.NewRegistrar(store, fastEngineConfig(), nopLogger())

	a, err := first.Register(ctx)
	if err != nil {
		t.Fatalf("failed to register the first node: %v", err)
	}
	b, err := second.Register(ctx)
	if err != nil {
		t.Fatalf("failed to register the second node: %v", err)
	}
	if a.ID == b.ID {
		t.Fatal("two nodes registered with the same id")
	}

	// Draining one must not touch the other.
	if err := first.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	other, err := store.GetEngineByID(ctx, b.ID)
	if err != nil {
		t.Fatalf("GetEngineByID failed: %v", err)
	}
	if other.Status != string(state.ActiveEngineStatus) {
		t.Errorf("second node's status = %q, want it untouched at active", other.Status)
	}
}
