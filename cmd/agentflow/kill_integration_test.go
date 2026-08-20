//go:build integration

// The phase gate for the execution path.
//
// Two engine processes against one Postgres, one of them killed outright while
// it holds work. This is the test the architecture doc says not to skip, because
// it is the only one that exercises the failure the whole design is built
// around: a node that stops existing without getting to run any cleanup.
//
// SIGKILL, not SIGTERM. A graceful stop drains the pool, marks the node stopped
// and releases its leases — which proves the shutdown path works and says
// nothing at all about crash recovery.
package main_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
)

func nopLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

// buildBinary compiles the real command once per test.
func buildBinary(t *testing.T) string {
	t.Helper()

	binary := filepath.Join(t.TempDir(), "agentflow")
	cmd := exec.Command("go", "build", "-o", binary, "./cmd/agentflow")
	cmd.Dir = dbtest.RepoRoot()

	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build agentflow: %v\n%s", err, output)
	}
	return binary
}

// engineEnv points a subprocess at the test database with intervals tight
// enough that reclaim happens in test time rather than in production time.
func engineEnv(t *testing.T, host string, port int) []string {
	t.Helper()

	return append(os.Environ(),
		"AGENTFLOW__DATABASE__HOST="+host,
		"AGENTFLOW__DATABASE__PORT="+strconv.Itoa(port),
		"AGENTFLOW__DATABASE__USER=postgres",
		"AGENTFLOW__DATABASE__PASSWORD=password",
		"AGENTFLOW__DATABASE__NAME=agentflow",
		"AGENTFLOW__DATABASE__SSL_MODE=disable",
		"AGENTFLOW__MIGRATIONS__PATH=file://"+filepath.Join(dbtest.RepoRoot(), "migrations"),
		"AGENTFLOW__ENGINE__CAPACITY=3",
		"AGENTFLOW__ENGINE__HEARTBEAT_INTERVAL=1",
		// Short, so a killed node's work becomes reclaimable in seconds.
		"AGENTFLOW__ENGINE__LEASE_TTL=4",
		"AGENTFLOW__ENGINE__POLL_INTERVAL=1",
		"AGENTFLOW__ENGINE__REAP_INTERVAL=1",
	)
}

// startEngine launches an engine subprocess. Setpgid so a kill reaches only
// this process, and so a leaked child cannot outlive the test.
func startEngine(t *testing.T, binary string, env []string, echoDelay time.Duration) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(binary, "engine", "-echo-delay", echoDelay.String())
	cmd.Env = env
	cmd.Dir = dbtest.RepoRoot()
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start engine: %v", err)
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	return cmd
}

func waitFor(t *testing.T, what string, within time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.After(within)
	for {
		if condition() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out after %s waiting for %s", within, what)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func engineIDs(t *testing.T, pool *pgxpool.Pool) []uuid.UUID {
	t.Helper()

	rows, err := pool.Query(context.Background(),
		`SELECT id FROM engines ORDER BY created_at`)
	if err != nil {
		t.Fatalf("failed to list engines: %v", err)
	}
	defer rows.Close()

	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("failed to scan engine id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to read engines: %v", err)
	}
	return ids
}

func runningOn(t *testing.T, pool *pgxpool.Pool, engineID uuid.UUID) []uuid.UUID {
	t.Helper()

	rows, err := pool.Query(context.Background(),
		`SELECT id FROM tasks WHERE status = 'running' AND engine_id = $1`, engineID)
	if err != nil {
		t.Fatalf("failed to list running tasks: %v", err)
	}
	defer rows.Close()

	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("failed to scan task id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to read running tasks: %v", err)
	}
	return ids
}

func TestTwoNodesSurviveOneBeingKilled(t *testing.T) {
	const taskCount = 24

	ctx := context.Background()
	pool := dbtest.Pool(t)
	binary := buildBinary(t)

	host, port := dbtest.HostPort(t)
	env := engineEnv(t, host, port)

	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil))
	dbtest.SeedAgents(t, pool, "research-agent")

	// Node A first, so its engine row is identifiable before B exists. Nothing
	// else distinguishes two processes on the same host.
	startEngine(t, binary, env, 900*time.Millisecond)
	waitFor(t, "node A to register", 60*time.Second, func() bool {
		return len(engineIDs(t, pool)) == 1
	})
	victim := engineIDs(t, pool)[0]

	startEngine(t, binary, env, 900*time.Millisecond)
	waitFor(t, "node B to register", 60*time.Second, func() bool {
		return len(engineIDs(t, pool)) == 2
	})

	var survivor uuid.UUID
	for _, id := range engineIDs(t, pool) {
		if id != victim {
			survivor = id
		}
	}

	// Enough independent work that both nodes are busy at once.
	workflowRow := dbtest.NewWorkflowRow()
	workflowRow.TaskCount = taskCount
	workflow, err := stores.WorkflowStore.CreateWorkflow(ctx, workflowRow)
	if err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	tasks := make([]*models.TaskRow, 0, taskCount)
	for i := range taskCount {
		tasks = append(tasks, dbtest.NewTaskRow(workflow.ID, "task-"+strconv.Itoa(i), "research-agent"))
	}
	if err := stores.TaskStore.BulkInsertTask(ctx, tasks); err != nil {
		t.Fatalf("failed to seed tasks: %v", err)
	}

	// Both nodes have to be working, or killing one proves nothing.
	waitFor(t, "both nodes to be running work", 60*time.Second, func() bool {
		return len(runningOn(t, pool, victim)) > 0 && len(runningOn(t, pool, survivor)) > 0
	})

	held := runningOn(t, pool, victim)
	if len(held) == 0 {
		t.Fatal("node A held nothing at the moment of the kill")
	}
	t.Logf("killing node A while it holds %d tasks", len(held))

	// SIGKILL: no signal handler runs, no drain, no status update. The row stays
	// active with a heartbeat that simply stops.
	killVictim(t, binary)

	// The survivor must finish the whole workflow on its own.
	waitFor(t, "the workflow to complete", 180*time.Second, func() bool {
		row, err := stores.WorkflowStore.GetWorkflowByID(ctx, workflow.ID)
		if err != nil {
			t.Fatalf("GetWorkflowByID failed: %v", err)
		}
		return row.Status == string(state.CompletedWorkflowStatus)
	})

	final, err := stores.WorkflowStore.GetWorkflowByID(ctx, workflow.ID)
	if err != nil {
		t.Fatalf("GetWorkflowByID failed: %v", err)
	}
	if final.TaskCompleted != taskCount {
		t.Errorf("task_completed = %d, want %d", final.TaskCompleted, taskCount)
	}
	if final.TaskFailed != 0 || final.TaskCancelled != 0 {
		t.Errorf("failed=%d cancelled=%d, want a clean completion despite the kill",
			final.TaskFailed, final.TaskCancelled)
	}

	// Every task the dead node held was reclaimed and rerun by the survivor.
	for _, taskID := range held {
		task, err := stores.TaskStore.GetTaskByID(ctx, taskID)
		if err != nil {
			t.Fatalf("GetTaskByID failed: %v", err)
		}
		if task.Status != string(state.CompletedTaskStatus) {
			t.Errorf("task %s held by the dead node ended %q, want completed", taskID, task.Status)
		}
		if task.Attempt < 2 {
			t.Errorf("task %s has attempt = %d, want at least 2 — it was never rerun",
				taskID, task.Attempt)
		}
		if task.EngineID == nil || *task.EngineID != survivor {
			t.Errorf("task %s finished on %v, want the surviving node %s",
				taskID, task.EngineID, survivor)
		}

		// The killed node committed nothing, so only the successful rerun left
		// a result. Two rows here would mean the dead node's work was recorded
		// as well, which is the double-commit the fence exists to prevent.
		results, err := stores.TaskResultStore.ListByTask(ctx, taskID)
		if err != nil {
			t.Fatalf("ListByTask failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("task %s has %d result rows, want exactly the rerun's", taskID, len(results))
		}
	}

	// No task committed twice, anywhere in the run.
	var completedTasks, resultRows int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE status = 'completed'`).Scan(&completedTasks)
	if err != nil {
		t.Fatalf("failed to count completed tasks: %v", err)
	}
	err = pool.QueryRow(ctx, `SELECT count(*) FROM task_results`).Scan(&resultRows)
	if err != nil {
		t.Fatalf("failed to count results: %v", err)
	}
	if completedTasks != taskCount {
		t.Errorf("completed tasks = %d, want %d", completedTasks, taskCount)
	}
	if resultRows != taskCount {
		t.Errorf("result rows = %d, want %d — a task was committed more than once",
			resultRows, taskCount)
	}

	// The killed node never got to update its own row: it is still 'active'
	// with a stale heartbeat, which is exactly what the reaper keys off.
	var victimStatus string
	err = pool.QueryRow(ctx, `SELECT status FROM engines WHERE id = $1`, victim).Scan(&victimStatus)
	if err != nil {
		t.Fatalf("failed to read the victim's status: %v", err)
	}
	if victimStatus != string(state.ActiveEngineStatus) {
		t.Errorf("the killed node's status is %q; a SIGKILL should leave no trace of cleanup",
			victimStatus)
	}
}

func killVictim(t *testing.T, binary string) {
	t.Helper()

	processes, err := listEngineProcesses(binary)
	if err != nil {
		t.Fatalf("failed to list engine processes: %v", err)
	}
	if len(processes) != 2 {
		t.Fatalf("found %d engine processes, want 2", len(processes))
	}

	// Node A was started first, so it holds the lower pid.
	pid := processes[0]
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("failed to SIGKILL pid %d: %v", pid, err)
	}

	waitFor(t, "the killed process to disappear", 30*time.Second, func() bool {
		remaining, err := listEngineProcesses(binary)
		if err != nil {
			return false
		}
		return len(remaining) == 1
	})
}

// listEngineProcesses returns the pids of running engine subprocesses, oldest
// first, by scanning /proc rather than shelling out.
func listEngineProcesses(binary string) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}

	type process struct {
		pid   int
		start string
	}
	found := []process{}

	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		target, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil || target != binary {
			continue
		}
		found = append(found, process{pid: pid})
	}

	pids := make([]int, 0, len(found))
	for _, p := range found {
		pids = append(pids, p.pid)
	}
	// Ascending pid: the process started first has the lower number, barring
	// wraparound, which is not a concern across a few seconds of a test.
	for i := range pids {
		for j := i + 1; j < len(pids); j++ {
			if pids[j] < pids[i] {
				pids[i], pids[j] = pids[j], pids[i]
			}
		}
	}
	return pids, nil
}
