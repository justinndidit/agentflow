# Agentflow

Agentflow is a distributed execution engine for AI workers. You bring the worker
as a container; Agentflow handles scheduling, dependency resolution, leasing,
retries, and failure recovery across a fleet of machines.

Postgres is the single source of truth. Every scheduling decision is a
transaction against it, so engine nodes stay stateless and interchangeable.

## Status

Early development. The submit path works end to end — a YAML manifest is parsed,
validated, and persisted as a runnable task graph. The execution path is
designed and has schema behind it, but nothing claims or runs tasks yet.

What works today:

- YAML manifest parsing with schema validation
- DAG validation at submit time — cycle detection, unknown dependencies
- Template reference validation — a task may only read the output of a task it
  declares a dependency on
- Postgres schema with migrations applied on boot
- Transactional workflow and task persistence

See [docs/agentflow_architecture.md](docs/agentflow_architecture.md) for the full
design, including lease fencing and failure semantics.

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
AGENTFLOW__DATABASE__HOST=db.internal go run ./cmd/agentflow
```

Application variables are namespaced `AGENTFLOW__<SECTION>__<KEY>`; the double
underscore separates the section from the key, since key names contain single
underscores of their own. `.env.sample` lists every setting with its default.

Migrations are applied automatically on boot. On a fresh database, pass `-seed`
the first time — `tasks.agent_name` is a foreign key to `agents(name)`, so a
manifest cannot be submitted until the agents it names exist. The seed runs after
the migrations and is idempotent, so repeating it is harmless.

```bash
go run ./cmd/agentflow -seed     # first run
go run ./cmd/agentflow           # afterwards
go run ./cmd/agentflow -manifest path/to/workflow.yml
```

Inspect what landed:

```bash
docker exec agentflow_db psql -U postgres -d agentflow -c \
  "SELECT task_key, status, remaining_deps FROM tasks
    WHERE workflow_id = (SELECT id FROM workflows ORDER BY created_at DESC LIMIT 1)
    ORDER BY remaining_deps, task_key"
```

Tasks with `remaining_deps = 0` are the ones a dispatcher would claim first.
Each submission creates a new workflow run, so submitting the same manifest twice
gives you two independent graphs — hence the scoping by workflow above.

## Project Layout

```
cmd/agentflow/         entry point: config, migrate, submit
internal/
  manifest/            YAML schema, parsing, template validation
  engine/              cycle detection, submit pipeline
  state/               task and workflow state machines
  persistence/         connection pool, migrations, models, repositories
  dtos/                API response types
  runtime/             Docker agent runtime (not implemented)
  config/
migrations/            schema, applied by golang-migrate
docs/                  architecture
pkg/                   logger, set, json helpers
```

## Built With

- [Go](https://golang.org) — engine
- PostgreSQL — source of truth, task queue, coordination
- [pgx](https://github.com/jackc/pgx) — Postgres driver
- [golang-migrate](https://github.com/golang-migrate/migrate) — schema migrations

## License

MIT
