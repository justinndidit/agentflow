//go:build integration

package repositories_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
)

func TestRegisterEngine_RoundTrips(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).EngineStore

	row := models.NewEngineRow("node-a.internal", 8)
	registered, err := store.Register(ctx, &row)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if registered.ID != row.ID {
		t.Errorf("ID = %s, want %s", registered.ID, row.ID)
	}
	if registered.HostName != "node-a.internal" {
		t.Errorf("HostName = %q, want node-a.internal", registered.HostName)
	}
	if registered.Capacity != 8 {
		t.Errorf("Capacity = %d, want 8", registered.Capacity)
	}
	// A node is claimable the moment it registers.
	if registered.Status != string(state.ActiveEngineStatus) {
		t.Errorf("Status = %q, want active", registered.Status)
	}
	// Seeded to the same instant so a node that dies before its first heartbeat
	// still ages out of liveness normally.
	if !registered.HeartBeatAt.Equal(registered.StartedAt) {
		t.Errorf("HeartBeatAt %v and StartedAt %v should match on registration",
			registered.HeartBeatAt, registered.StartedAt)
	}
}

func TestRegisterEngine_DistinctIdentities(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).EngineStore

	first := models.NewEngineRow("node-a", 4)
	second := models.NewEngineRow("node-b", 4)

	if _, err := store.Register(ctx, &first); err != nil {
		t.Fatalf("failed to register the first engine: %v", err)
	}
	if _, err := store.Register(ctx, &second); err != nil {
		t.Fatalf("failed to register the second engine: %v", err)
	}

	if first.ID == second.ID {
		t.Fatal("two registrations produced the same engine id")
	}
	if count := countRows(t, pool, "engines"); count != 2 {
		t.Errorf("engines = %d, want 2", count)
	}
}

func TestHeartbeat_AdvancesLiveness(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).EngineStore

	row := models.NewEngineRow("node-a", 4)
	registered, err := store.Register(ctx, &row)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	first, err := store.Heartbeat(ctx, registered.ID)
	if err != nil {
		t.Fatalf("first Heartbeat failed: %v", err)
	}
	if !first.After(registered.HeartBeatAt) {
		t.Errorf("heartbeat %v did not advance past registration %v", first, registered.HeartBeatAt)
	}

	second, err := store.Heartbeat(ctx, registered.ID)
	if err != nil {
		t.Fatalf("second Heartbeat failed: %v", err)
	}
	if !second.After(first) {
		t.Errorf("second heartbeat %v did not advance past the first %v", second, first)
	}
}

// Heartbeats are stamped with the database clock rather than the caller's, so
// liveness does not depend on how well a fleet's clocks agree.
func TestHeartbeat_UsesTheDatabaseClock(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).EngineStore

	row := models.NewEngineRow("node-a", 4)
	registered, err := store.Register(ctx, &row)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	dbNow, err := store.Now(ctx)
	if err != nil {
		t.Fatalf("Now failed: %v", err)
	}
	beat, err := store.Heartbeat(ctx, registered.ID)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	if beat.Before(dbNow) {
		t.Errorf("heartbeat %v predates the database clock reading %v", beat, dbNow)
	}
	if beat.Sub(dbNow) > time.Minute {
		t.Errorf("heartbeat %v is implausibly far ahead of the database clock %v", beat, dbNow)
	}
}

func TestHeartbeat_UnknownEngine(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	_, err := stores(pool).EngineStore.Heartbeat(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected heartbeating an unregistered engine to fail")
	}
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

func TestSetStatus_MovesThroughTheLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).EngineStore

	row := models.NewEngineRow("node-a", 4)
	registered, err := store.Register(ctx, &row)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	for _, status := range []state.EngineStatus{
		state.DrainingEngineStatus,
		state.StoppedEngineStatus,
	} {
		if err := store.SetStatus(ctx, registered.ID, status); err != nil {
			t.Fatalf("SetStatus(%s) failed: %v", status, err)
		}

		got, err := store.GetEngineByID(ctx, registered.ID)
		if err != nil {
			t.Fatalf("GetEngineByID failed: %v", err)
		}
		if got.Status != string(status) {
			t.Errorf("Status = %q, want %q", got.Status, status)
		}
	}
}

func TestSetStatus_UnknownEngine(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	err := stores(pool).EngineStore.SetStatus(ctx, uuid.New(), state.StoppedEngineStatus)
	if err == nil {
		t.Fatal("expected setting status on an unregistered engine to fail")
	}
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

func TestGetEngineByID_NotFound(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	_, err := stores(pool).EngineStore.GetEngineByID(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected an error for a missing engine")
	}
	if !errors.Is(err, repositories.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

// ListStale is the reaper's input. It has to find a node that stopped
// heartbeating and leave a healthy one alone — getting this backwards either
// reclaims live work or strands dead work forever.
func TestListStale_SeparatesDeadFromLive(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).EngineStore

	live := models.NewEngineRow("live-node", 4)
	if _, err := store.Register(ctx, &live); err != nil {
		t.Fatalf("failed to register the live engine: %v", err)
	}

	dead := models.NewEngineRow("dead-node", 4)
	if _, err := store.Register(ctx, &dead); err != nil {
		t.Fatalf("failed to register the dead engine: %v", err)
	}
	// Backdate the heartbeat rather than sleeping through a real TTL.
	if _, err := pool.Exec(ctx,
		`UPDATE engines SET heartbeat_at = now() - interval '10 minutes' WHERE id = $1`,
		dead.ID); err != nil {
		t.Fatalf("failed to backdate the dead engine's heartbeat: %v", err)
	}

	stale, err := store.ListStale(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ListStale failed: %v", err)
	}

	if len(stale) != 1 {
		t.Fatalf("ListStale returned %d engines, want 1", len(stale))
	}
	if stale[0].ID != dead.ID {
		t.Errorf("ListStale returned %s, want the dead node %s", stale[0].ID, dead.ID)
	}
}

// The cutoff is computed from the database clock inside the statement, so the
// boundary is exact rather than approximately right.
func TestListStale_RespectsTheTTLBoundary(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).EngineStore

	engine := models.NewEngineRow("node-a", 4)
	if _, err := store.Register(ctx, &engine); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE engines SET heartbeat_at = now() - interval '30 seconds' WHERE id = $1`,
		engine.ID); err != nil {
		t.Fatalf("failed to backdate the heartbeat: %v", err)
	}

	// Well inside a generous TTL.
	stale, err := store.ListStale(ctx, 5*time.Minute)
	if err != nil {
		t.Fatalf("ListStale failed: %v", err)
	}
	if len(stale) != 0 {
		t.Errorf("a 30s-old heartbeat is stale under a 5m TTL, want it live")
	}

	// Well outside a tight one.
	stale, err = store.ListStale(ctx, 10*time.Second)
	if err != nil {
		t.Fatalf("ListStale failed: %v", err)
	}
	if len(stale) != 1 {
		t.Errorf("a 30s-old heartbeat is live under a 10s TTL, want it stale")
	}
}

// A node that shut down cleanly may still own rows if it was killed between
// setting its status and finishing work, so stopped engines are reclaimable
// rather than exempt.
func TestListStale_IncludesStoppedEngines(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	store := stores(pool).EngineStore

	engine := models.NewEngineRow("node-a", 4)
	if _, err := store.Register(ctx, &engine); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if err := store.SetStatus(ctx, engine.ID, state.StoppedEngineStatus); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE engines SET heartbeat_at = now() - interval '10 minutes' WHERE id = $1`,
		engine.ID); err != nil {
		t.Fatalf("failed to backdate the heartbeat: %v", err)
	}

	stale, err := store.ListStale(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ListStale failed: %v", err)
	}
	if len(stale) != 1 {
		t.Errorf("ListStale returned %d engines, want the stopped node included", len(stale))
	}
}

func TestListStale_NoEngines(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	stale, err := stores(pool).EngineStore.ListStale(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ListStale failed: %v", err)
	}
	if len(stale) != 0 {
		t.Errorf("ListStale returned %d engines, want none", len(stale))
	}
}
