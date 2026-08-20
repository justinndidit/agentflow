//go:build integration

package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/database"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

func nopLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

func writeManifest(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "workflow.yml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}
	return path
}

// submit runs the real pipeline: parse, validate, build the graph, persist.
func submit(t *testing.T, pool *pgxpool.Pool, manifestPath string) (*repositories.Stores, string) {
	t.Helper()

	txManager := repositories.NewTxManager(pool, nopLogger())
	workflow, err := engine.NewManifestProcessor(nopLogger(), txManager).
		SubmitManifest(context.Background(), manifestPath)
	if err != nil {
		t.Fatalf("SubmitManifest failed: %v", err)
	}
	return repositories.NewStore(repositories.NewPostgresRepository(pool, nopLogger(), nil)),
		workflow.ID.String()
}

const diamondManifest = `
name: diamond-pipeline
namespace: default
workflow_version: 2
workers: 8
timeout: 2m
max_tokens: 250000

tasks:
  - task_key: fetch
    agent: research-agent
    priority: 4
    max_retries: 3
    timeout: 300
    input:
      roles: ["backend engineer"]

  - task_key: rank
    agent: matching-agent
    priority: 2
    max_retries: 2
    timeout: 120
    depends_on:
      - fetch
    input:
      jobs: "{{ tasks.fetch.output.jobs }}"

  - task_key: score
    agent: matching-agent
    priority: 2
    max_retries: 2
    timeout: 120
    depends_on:
      - fetch

  - task_key: report
    agent: research-agent
    priority: 1
    max_retries: 0
    timeout: 60
    depends_on:
      - rank
      - score
`

// The whole submit path, end to end: a manifest on disk becomes a runnable graph
// in Postgres. remaining_deps is the assertion that matters — it is the only
// thing that will make a task claimable, and it has to equal the number of
// dependencies the task was declared with.
func TestSubmitManifest_PersistsRunnableGraph(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent", "matching-agent")

	stores, workflowID := submit(t, pool, writeManifest(t, diamondManifest))

	workflow, err := stores.WorkflowStore.GetWorkflowByName(ctx, "diamond-pipeline")
	if err != nil {
		t.Fatalf("GetWorkflowByName failed: %v", err)
	}
	if workflow.ID.String() != workflowID {
		t.Errorf("stored workflow %s, want %s", workflow.ID, workflowID)
	}
	if workflow.WorkflowNameSpace != "default" {
		t.Errorf("namespace = %q, want default", workflow.WorkflowNameSpace)
	}
	if workflow.Version != 2 {
		t.Errorf("version = %d, want 2", workflow.Version)
	}
	if workflow.TaskCount != 4 {
		t.Errorf("task_total = %d, want 4", workflow.TaskCount)
	}
	if workflow.Status != "pending" {
		t.Errorf("status = %q, want pending", workflow.Status)
	}
	if workflow.MaxParallelism != 8 {
		t.Errorf("max_parallelism = %d, want 8", workflow.MaxParallelism)
	}
	// The manifest is stored verbatim so a run can be reproduced from the row.
	if string(workflow.Manifest) != diamondManifest {
		t.Error("stored manifest does not match the submitted file")
	}

	tasks, err := stores.TaskStore.ListTasksByWorkflow(ctx, workflow.ID)
	if err != nil {
		t.Fatalf("ListTasksByWorkflow failed: %v", err)
	}
	if len(tasks) != 4 {
		t.Fatalf("stored %d tasks, want 4", len(tasks))
	}

	wantRemaining := map[string]int{"fetch": 0, "rank": 1, "score": 1, "report": 2}
	for _, task := range tasks {
		want, ok := wantRemaining[task.TaskKey]
		if !ok {
			t.Errorf("unexpected task %q", task.TaskKey)
			continue
		}
		if task.RemainingDeps != want {
			t.Errorf("task %q remaining_deps = %d, want %d", task.TaskKey, task.RemainingDeps, want)
		}
		if task.Status != "pending" {
			t.Errorf("task %q status = %q, want pending", task.TaskKey, task.Status)
		}
		// Nothing has claimed anything: the execution path does not exist yet.
		if task.EngineID != nil || task.LeaseExpiry != nil || task.LeaseEpoch != 0 {
			t.Errorf("task %q was submitted already leased", task.TaskKey)
		}
		if task.Attempt != 0 {
			t.Errorf("task %q attempt = %d, want 0", task.TaskKey, task.Attempt)
		}
	}
}

// Only the roots are claimable at submit; everything else waits on the counter.
func TestSubmitManifest_OnlyRootsAreReady(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent", "matching-agent")

	submit(t, pool, writeManifest(t, diamondManifest))

	rows, err := pool.Query(ctx,
		`SELECT task_key FROM tasks WHERE status = 'pending' AND remaining_deps = 0`)
	if err != nil {
		t.Fatalf("failed to query ready tasks: %v", err)
	}
	defer rows.Close()

	ready := []string{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		ready = append(ready, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration failed: %v", err)
	}

	if len(ready) != 1 || ready[0] != "fetch" {
		t.Errorf("ready tasks = %v, want [fetch]", ready)
	}
}

// Template expressions are stored unresolved: the upstream output they reference
// does not exist until dispatch.
func TestSubmitManifest_StoresTemplatesUnresolved(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent", "matching-agent")

	submit(t, pool, writeManifest(t, diamondManifest))

	var input string
	err := pool.QueryRow(ctx,
		`SELECT input_template::text FROM tasks WHERE task_key = 'rank'`).Scan(&input)
	if err != nil {
		t.Fatalf("failed to read input_template: %v", err)
	}
	if !strings.Contains(input, "{{ tasks.fetch.output.jobs }}") {
		t.Errorf("input_template = %s, want the template stored unresolved", input)
	}
}

// Each submission is an independent run, so the same manifest twice gives two
// graphs rather than colliding on (workflow_id, task_key).
func TestSubmitManifest_TwiceCreatesTwoIndependentRuns(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent", "matching-agent")

	path := writeManifest(t, diamondManifest)
	_, firstID := submit(t, pool, path)
	_, secondID := submit(t, pool, path)

	if firstID == secondID {
		t.Fatal("expected two submissions to produce two workflows")
	}

	var workflows, tasks int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM workflows`).Scan(&workflows); err != nil {
		t.Fatalf("failed to count workflows: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks`).Scan(&tasks); err != nil {
		t.Fatalf("failed to count tasks: %v", err)
	}
	if workflows != 2 {
		t.Errorf("workflows = %d, want 2", workflows)
	}
	if tasks != 8 {
		t.Errorf("tasks = %d, want 8", tasks)
	}
}

// A manifest rejected at validation must leave nothing behind — the workflow row
// is written before the tasks, so a cycle discovered late would otherwise strand
// it. The cycle is caught before any write here, and this pins that.
func TestSubmitManifest_RejectedManifestWritesNothing(t *testing.T) {
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	cyclic := `
name: cyclic
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    agent: research-agent
    max_retries: 0
    timeout: 10
    depends_on: [b]
  - task_key: b
    agent: research-agent
    max_retries: 0
    timeout: 10
    depends_on: [a]
`
	txManager := repositories.NewTxManager(pool, nopLogger())
	_, err := engine.NewManifestProcessor(nopLogger(), txManager).
		SubmitManifest(context.Background(), writeManifest(t, cyclic))
	if err == nil {
		t.Fatal("expected a cyclic manifest to be rejected")
	}

	assertEmpty(t, pool, "workflows")
	assertEmpty(t, pool, "tasks")
}

// An agent that was never registered fails on the foreign key, after the
// workflow row has already been inserted inside the transaction. This is the
// case that proves the rollback covers the submit path specifically.
func TestSubmitManifest_UnknownAgentRollsBackTheWorkflow(t *testing.T) {
	pool := dbtest.Pool(t)
	dbtest.SeedAgents(t, pool, "research-agent")

	unknownAgent := `
name: unknown-agent
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    agent: research-agent
    max_retries: 0
    timeout: 10
  - task_key: b
    agent: never-registered-agent
    max_retries: 0
    timeout: 10
`
	txManager := repositories.NewTxManager(pool, nopLogger())
	_, err := engine.NewManifestProcessor(nopLogger(), txManager).
		SubmitManifest(context.Background(), writeManifest(t, unknownAgent))
	if err == nil {
		t.Fatal("expected a manifest naming an unregistered agent to fail")
	}

	assertEmpty(t, pool, "workflows")
	assertEmpty(t, pool, "tasks")
}

func TestSubmitManifest_MissingFile(t *testing.T) {
	pool := dbtest.Pool(t)

	txManager := repositories.NewTxManager(pool, nopLogger())
	_, err := engine.NewManifestProcessor(nopLogger(), txManager).
		SubmitManifest(context.Background(), filepath.Join(t.TempDir(), "absent.yml"))
	if err == nil {
		t.Fatal("expected a missing manifest to fail")
	}

	assertEmpty(t, pool, "workflows")
}

// The manifest the getting-started path actually runs, submitted against the
// agents the dev seed inserts.
func TestSubmitManifest_ExampleWorkflow(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.Pool(t)

	if err := database.SeedDevAgents(ctx, pool, nopLogger()); err != nil {
		t.Fatalf("failed to seed dev agents: %v", err)
	}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to resolve repo root: %v", err)
	}

	stores, _ := submit(t, pool, filepath.Join(root, "example-workflow.yml"))

	workflow, err := stores.WorkflowStore.GetWorkflowByName(ctx, "global-hiring-pipeline")
	if err != nil {
		t.Fatalf("GetWorkflowByName failed: %v", err)
	}

	tasks, err := stores.TaskStore.ListTasksByWorkflow(ctx, workflow.ID)
	if err != nil {
		t.Fatalf("ListTasksByWorkflow failed: %v", err)
	}
	if len(tasks) != workflow.TaskCount {
		t.Errorf("stored %d tasks but task_total is %d", len(tasks), workflow.TaskCount)
	}

	// Every dependency named by a task must exist in the same workflow, or the
	// counter can never reach zero.
	keys := map[string]bool{}
	for _, task := range tasks {
		keys[task.TaskKey] = true
	}
	for _, task := range tasks {
		if task.RemainingDeps != len(task.DependsOn) {
			t.Errorf("task %q remaining_deps = %d, want %d",
				task.TaskKey, task.RemainingDeps, len(task.DependsOn))
		}
		for _, dependency := range task.DependsOn {
			if !keys[dependency] {
				t.Errorf("task %q depends on %q, which was not stored", task.TaskKey, dependency)
			}
		}
	}
}

func assertEmpty(t *testing.T, pool *pgxpool.Pool, table string) {
	t.Helper()

	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&count); err != nil {
		t.Fatalf("failed to count %s: %v", table, err)
	}
	if count != 0 {
		t.Errorf("%s = %d, want 0", table, count)
	}
}
