# Agentflow

Agentflow is a distributed execution engine for AI workers. You bring the worker
as a container; Agentflow handles scheduling, dependency resolution, leasing,
retries, and failure recovery across a fleet of machines.

Postgres is the single source of truth. Every scheduling decision is a
transaction against it, so engine nodes stay stateless and interchangeable.

## Status

Under development. Workflows run end to end across multiple nodes, but the only
worker available is a placeholder that echoes its input — no containers are
executed yet, so Agentflow cannot run real agents.

What works today:

- **Submit** — YAML manifest parsing and schema validation, cycle detection,
  and template reference checking, persisted as a runnable graph in one
  transaction
- **Claim** — nodes take ready work atomically under `FOR UPDATE SKIP LOCKED`,
  bounded by their own free capacity
- **Execute** — node-local bounded concurrency, with each attempt capped by the
  shorter of the task's timeout and its lease
- **Commit** — results, dependent decrements and workflow counters in one
  transaction, fenced by `lease_epoch` so a superseded node cannot overwrite a
  newer result
- **Reap** — work is reclaimed from nodes that stopped heartbeating or overran
  their lease, retried with exponential backoff and jitter, and permanently
  failed tasks cancel their transitive dependents
- **Run** — `agentflow engine` registers, heartbeats, claims, runs, commits and
  reaps until interrupted, woken by `LISTEN`/`NOTIFY` with a poll interval as
  the floor

Two engine nodes against one Postgres survive one of them being `SIGKILL`ed
mid-workflow: the work it held is reclaimed and rerun, the workflow completes,
and the dead node's late writes are fenced out. That test lives in
[`cmd/agentflow/kill_integration_test.go`](cmd/agentflow/kill_integration_test.go).

Not built yet: the Docker runtime, template resolution at dispatch, blob storage
for large outputs, and observability.

See [docs/agentflow_architecture.md](docs/agentflow_architecture.md) for the full
design, and [docs/execution_path_plan.md](docs/execution_path_plan.md) for how
the execution path was built and what remains.

## Concepts

**Agent** — an AI worker packaged as a container image with a typed input/output
contract. Language agnostic.

**Task** — one dispatch to an agent. Lifecycle:
`pending → running → completed | failed | cancelled`, with retry back to
`pending` and a backoff window.

**Workflow** — a DAG of tasks, submitted as a YAML manifest. Independent branches
run in parallel.

**Engine** — a node that claims and executes tasks. Engines register themselves,
heartbeat, and hold time-bounded leases on the work they claim.

## How Scheduling Works

There is no coordinator and no leader election. Each task row carries a
`remaining_deps` counter; a task is runnable when that counter reaches zero. When
a task completes, the same transaction that records its result decrements its
dependents. Readiness propagates through data rather than through a scheduler.

The DAG is a validation artifact, not an execution plan — checked once at submit
time for cycles, then discarded.

## Manifest Format

```yaml
name: global-hiring-pipeline
namespace: default
workflow_version: 1
workers: 8
timeout: 2m
max_tokens: 250000

tasks:
  - task_key: collect-market-data
    agent: research-agent
    priority: 4          # 4 high, 2 medium, 1 low
    max_retries: 3
    timeout: 300         # seconds
    input:
      roles: ["backend engineer", "platform engineer"]

  - task_key: rank-jobs
    agent: matching-agent
    priority: 2
    max_retries: 2
    timeout: 120
    depends_on:
      - collect-market-data
    input:
      # resolved at dispatch from the upstream task's output
      jobs: "{{ tasks.collect-market-data.output.jobs }}"
```

A full example is in [`example-workflow.yml`](example-workflow.yml).

Template expressions are stored unresolved and resolved when the task is
dispatched, since upstream output does not exist at submit time. Referencing a
task you do not depend on is rejected at submit.

## Getting Started

Requires Go 1.25+ and Docker.

```bash
git clone https://github.com/justinndidit/agentflow
cd agentflow
go mod tidy

cp .env.sample .env

# start Postgres
docker compose -f docker-compose.dev.yml up -d
```

Configuration comes from built-in defaults, then `.env`, then real environment
variables, each layer overriding the one before. The defaults match
`docker-compose.dev.yml`, so `.env` is optional locally and a deployment can
override a single value without shipping a file:

```bash
AGENTFLOW__DATABASE__HOST=db.internal go run ./cmd/agentflow engine
```

Application variables are namespaced `AGENTFLOW__<SECTION>__<KEY>`; the double
underscore separates the section from the key, since key names contain single
underscores of their own. `.env.sample` lists every setting with its default.

Migrations are applied automatically on boot. On a fresh database, pass `-seed`
the first time — `tasks.agent_name` is a foreign key to `agents(name)`, so a
manifest cannot be submitted until the agents it names exist. The seed runs after
the migrations and is idempotent, so repeating it is harmless.

Submitting persists a graph; it does not run anything. Start an engine node to
pick the work up — in a second terminal, since it runs until interrupted:

```bash
go run ./cmd/agentflow engine -seed     # first run
go run ./cmd/agentflow engine           # afterwards
```

Then submit:

```bash
go run ./cmd/agentflow submit
go run ./cmd/agentflow submit -manifest path/to/workflow.yml
```

The node is notified on commit, so it starts immediately rather than waiting for
its next poll. Every task is run by the echo runtime, which returns its input as
its output — `-echo-delay` controls how long it pretends to take.

Run more than one engine and they share the work; kill one and the other picks
up whatever it was holding.

Watch a run:

```bash
docker exec agentflow_db psql -U postgres -d agentflow -c \
  "SELECT task_key, status, remaining_deps, attempt FROM tasks
    WHERE workflow_id = (SELECT id FROM workflows ORDER BY created_at DESC LIMIT 1)
    ORDER BY remaining_deps, task_key"
```

Each submission creates a new workflow run, so submitting the same manifest twice
gives you two independent graphs — hence the scoping by workflow above.

## Testing

```bash
task test              # unit tests, no Docker needed
task test:integration  # against a throwaway Postgres, needs Docker
task test:all
```

The suite is split by build tag. Unit tests cover the pure logic — manifest
validation, cycle detection, the state machine, backoff — and run in about a
second. Integration tests are tagged `integration` and start a real Postgres per
package via testcontainers, because most of what the repository layer does is a
Postgres semantic rather than a Go one: positional `COPY`, `INTERVAL` encoding,
foreign keys, enum casts, `SKIP LOCKED`. Faking the database there would only
assert that the code calls the methods it was written to call.

## Project Layout

```
cmd/agentflow/         submit and engine commands
internal/
  manifest/            YAML schema, parsing, template validation
  engine/              submit pipeline, and the five node loops:
                       registrar, dispatcher, pool, committer, reaper
  runtime/             worker execution; echo only, Docker not implemented
  state/               task and workflow state machines, and the SQL guards
                       generated from them
  persistence/         connection pool, migrations, models, repositories
  dbtest/              throwaway Postgres for integration tests
  dtos/                API response types
  config/
migrations/            schema, applied by golang-migrate
docs/                  architecture and the execution path plan
pkg/                   logger, set, json helpers
```

## Built With

- [Go](https://golang.org) — engine
- PostgreSQL — source of truth, task queue, coordination, and `LISTEN`/`NOTIFY`
- [pgx](https://github.com/jackc/pgx) — Postgres driver
- [golang-migrate](https://github.com/golang-migrate/migrate) — schema migrations
- [koanf](https://github.com/knadh/koanf) — layered configuration
- [testcontainers](https://github.com/testcontainers/testcontainers-go) —
  throwaway Postgres for the integration suite

## License

MIT
