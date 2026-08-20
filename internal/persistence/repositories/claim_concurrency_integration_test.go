//go:build integration

package repositories_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

// dedicatedPool gives one caller its own connection pool, so goroutines in the
// tests below genuinely contend at the database rather than sharing a pool that
// might serialise them.
func dedicatedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), dbtest.DSN(t))
	if err != nil {
		t.Fatalf("failed to open a dedicated pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// This is the test M2 exists for.
//
// Eight simulated nodes, each on its own pool, drain a queue of 200 ready tasks
// as fast as they can. The claim is correct only if the union of what they take
// is exactly the queue and the intersection is empty: every task claimed once,
// none claimed twice, none stranded.
//
// FOR UPDATE SKIP LOCKED is what makes that true without any node blocking on
// another. Without SKIP LOCKED this still passes but serialises; without the
// row lock entirely, two nodes read the same pending row and both claim it.
func TestClaimTasks_ConcurrentNodesClaimDisjointSets(t *testing.T) {
	const (
		nodes     = 8
		taskCount = 200
		batchSize = 5
	)

	ctx := context.Background()
	setup := dbtest.Pool(t)
	workflow := seedWorkflow(t, setup, "research-agent")

	tasks := make([]*models.TaskRow, 0, taskCount)
	for i := range taskCount {
		tasks = append(tasks, newTaskRow(workflow.ID, "task-"+strconv.Itoa(i), "research-agent"))
	}
	if err := stores(setup).TaskStore.BulkInsertTask(ctx, tasks); err != nil {
		t.Fatalf("failed to seed tasks: %v", err)
	}

	type claim struct {
		taskID   uuid.UUID
		engineID uuid.UUID
	}

	var (
		mu        sync.Mutex
		allClaims []claim
		wg        sync.WaitGroup
		start     = make(chan struct{})
	)

	for range nodes {
		pool := dedicatedPool(t)
		engineID := registerEngine(t, setup, "node-"+uuid.New().String()[:8])
		store := stores(pool).TaskStore

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release everyone at once to maximise contention

			for {
				claimed, err := store.ClaimTasks(ctx, engineID, batchSize, testLeaseTTL)
				if err != nil {
					t.Errorf("ClaimTasks failed: %v", err)
					return
				}
				if len(claimed) == 0 {
					return
				}

				mu.Lock()
				for _, task := range claimed {
					allClaims = append(allClaims, claim{taskID: task.ID, engineID: engineID})
				}
				mu.Unlock()
			}
		}()
	}

	close(start)
	wg.Wait()

	// Every task exactly once.
	seen := map[uuid.UUID]uuid.UUID{}
	for _, c := range allClaims {
		if previous, duplicate := seen[c.taskID]; duplicate {
			t.Errorf("task %s was claimed twice: by %s and %s", c.taskID, previous, c.engineID)
			continue
		}
		seen[c.taskID] = c.engineID
	}

	if len(allClaims) != taskCount {
		t.Errorf("nodes claimed %d tasks in total, want %d", len(allClaims), taskCount)
	}
	if len(seen) != taskCount {
		t.Errorf("%d distinct tasks were claimed, want %d", len(seen), taskCount)
	}

	// Nothing stranded, and the database agrees with what the nodes think.
	var pending int
	if err := setup.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE status = 'pending'`).Scan(&pending); err != nil {
		t.Fatalf("failed to count pending tasks: %v", err)
	}
	if pending != 0 {
		t.Errorf("%d tasks left pending, want the queue fully drained", pending)
	}

	// Each row's engine_id matches the node that reported claiming it — proof
	// that no claim was overwritten by a later one.
	rows, err := setup.Query(ctx, `SELECT id, engine_id, attempt, lease_epoch FROM tasks`)
	if err != nil {
		t.Fatalf("failed to read tasks back: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id       uuid.UUID
			engineID *uuid.UUID
			attempt  int
			epoch    int64
		)
		if err := rows.Scan(&id, &engineID, &attempt, &epoch); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if engineID == nil {
			t.Errorf("task %s has no engine_id", id)
			continue
		}
		if *engineID != seen[id] {
			t.Errorf("task %s is assigned to %s but %s reported claiming it", id, engineID, seen[id])
		}
		// Claimed exactly once, so exactly one increment of each.
		if attempt != 1 {
			t.Errorf("task %s has attempt = %d, want 1", id, attempt)
		}
		if epoch != 1 {
			t.Errorf("task %s has lease_epoch = %d, want 1", id, epoch)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration failed: %v", err)
	}
}

// The narrowest version of the same race: many nodes, one task. Exactly one
// wins and the rest come back empty rather than blocking or erroring.
func TestClaimTasks_SingleTaskHasExactlyOneWinner(t *testing.T) {
	const nodes = 12

	ctx := context.Background()
	setup := dbtest.Pool(t)
	workflow := seedWorkflow(t, setup, "research-agent")
	seedReadyTasks(t, setup, workflow.ID, "the-only-one")

	var (
		mu      sync.Mutex
		winners []uuid.UUID
		wg      sync.WaitGroup
		start   = make(chan struct{})
	)

	for range nodes {
		pool := dedicatedPool(t)
		engineID := registerEngine(t, setup, "node-"+uuid.New().String()[:8])
		store := stores(pool).TaskStore

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			claimed, err := store.ClaimTasks(ctx, engineID, 1, testLeaseTTL)
			if err != nil {
				t.Errorf("ClaimTasks failed: %v", err)
				return
			}
			if len(claimed) > 1 {
				t.Errorf("a node claimed %d tasks when only 1 exists", len(claimed))
			}
			if len(claimed) == 1 {
				mu.Lock()
				winners = append(winners, engineID)
				mu.Unlock()
			}
		}()
	}

	close(start)
	wg.Wait()

	if len(winners) != 1 {
		t.Errorf("%d nodes claimed the task, want exactly 1", len(winners))
	}
}

// Nodes racing while the queue is refilled must still never double-claim. This
// is closer to the steady state than a single drain: the committer will be
// making tasks ready underneath the dispatchers.
func TestClaimTasks_NoDoubleClaimWhileQueueGrows(t *testing.T) {
	const (
		nodes    = 4
		rounds   = 10
		perRound = 20
	)

	ctx := context.Background()
	setup := dbtest.Pool(t)
	workflow := seedWorkflow(t, setup, "research-agent")

	var (
		mu      sync.Mutex
		claimed = map[uuid.UUID]bool{}
		wg      sync.WaitGroup
		done    = make(chan struct{})
	)

	for range nodes {
		pool := dedicatedPool(t)
		engineID := registerEngine(t, setup, "node-"+uuid.New().String()[:8])
		store := stores(pool).TaskStore

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					// One last sweep so nothing inserted just before the stop
					// signal is left behind.
					batch, err := store.ClaimTasks(ctx, engineID, 50, testLeaseTTL)
					if err != nil {
						t.Errorf("final ClaimTasks failed: %v", err)
						return
					}
					record(t, &mu, claimed, batch)
					return
				default:
				}

				batch, err := store.ClaimTasks(ctx, engineID, 3, testLeaseTTL)
				if err != nil {
					t.Errorf("ClaimTasks failed: %v", err)
					return
				}
				record(t, &mu, claimed, batch)
				if len(batch) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	total := 0
	for round := range rounds {
		tasks := make([]*models.TaskRow, 0, perRound)
		for i := range perRound {
			tasks = append(tasks, newTaskRow(workflow.ID,
				"r"+strconv.Itoa(round)+"-t"+strconv.Itoa(i), "research-agent"))
		}
		if err := stores(setup).TaskStore.BulkInsertTask(ctx, tasks); err != nil {
			t.Fatalf("failed to insert round %d: %v", round, err)
		}
		total += perRound
		time.Sleep(5 * time.Millisecond)
	}

	// Let the dispatchers catch up before signalling the stop.
	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()

	mu.Lock()
	distinct := len(claimed)
	mu.Unlock()

	if distinct != total {
		t.Errorf("claimed %d distinct tasks, want %d", distinct, total)
	}

	var pending int
	if err := setup.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE status = 'pending'`).Scan(&pending); err != nil {
		t.Fatalf("failed to count pending tasks: %v", err)
	}
	if pending != 0 {
		t.Errorf("%d tasks left pending", pending)
	}
}

// record fails the test on any task seen twice, which is the property under
// examination rather than a bookkeeping detail.
func record(t *testing.T, mu *sync.Mutex, seen map[uuid.UUID]bool, batch []*models.TaskRow) {
	t.Helper()

	mu.Lock()
	defer mu.Unlock()
	for _, task := range batch {
		if seen[task.ID] {
			t.Errorf("task %s was claimed twice", task.ID)
		}
		seen[task.ID] = true
	}
}
