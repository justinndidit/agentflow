# Agentflow

Agentflow is a distributed execution engine for AI workers.

Think of it as Kubernetes for AI agents — you bring the worker, Agentflow handles
scheduling, dependency resolution, leasing, retries, and failure recovery across
a fleet of machines.

> "The valuable layer is not discovering agents. It is running them safely,
> composing them reliably, and scaling them predictably."

---

## The Problem

Teams building AI systems hit the same infrastructure problems repeatedly:

- No standard deployment unit for AI workers
- Brittle glue code stitching agents together
- No reliable retry, cancellation, or failure semantics
- Poor observability into what agents are actually doing
- Tight coupling to Python frameworks and specific SDK ecosystems
- No governance — cost controls, audit trails, access policies

Agentflow solves the infrastructure layer so you can focus on worker logic.

---

## Status

Agentflow is under active development and **not yet usable for running
workloads**. The submit path works end to end; the execution path does not exist
yet.

**Working today**

- [x] YAML manifest parsing with schema validation
- [x] DAG validation at submit time — cycle detection, unknown dependencies
- [x] Template reference validation — a task may only read the output of a task
      it declares a dependency on
- [x] Postgres schema with migrations applied on boot
- [x] Workflow and task persistence, transactionally
- [x] Submit a manifest and have it land in the database as a runnable graph

**Designed, schema in place, not yet implemented**

- [ ] Dispatcher — claim ready tasks with `FOR UPDATE SKIP LOCKED`
- [ ] Committer — record results, decrement dependents, update counters
- [ ] Reaper — reclaim tasks from dead engines and expired leases
- [ ] Lease fencing via `lease_epoch` (columns exist, no loop uses them yet)
- [ ] Engine registration and heartbeating

**Not started**

- [ ] Docker agent runtime — the worker contract is specified, nothing executes
- [ ] Template resolution at dispatch
- [ ] Blob storage for large outputs
- [ ] Observability — traces, cost tracking

The v0.1 in-memory wave executor has been removed. It could not survive
distribution; see [the architecture doc](docs/agentflow_architecture.md) for why.

---

## Architecture

**Postgres is the single source of truth.** Every scheduling decision is a
transaction against it. Engine nodes are stateless and interchangeable — any node
can run any ready task, and losing a node loses no work.

There is no coordinator and no leader election. Each task row carries a
`remaining_deps` counter; a task is runnable when that counter reaches zero. When
a task completes, the same transaction that records its result decrements its
dependents. Readiness propagates through data rather than through a scheduler.

The DAG is a **validation artifact**, not an execution plan — checked once at
submit time for cycles, then discarded.

Full design, including lease fencing, failure semantics, and the trade-offs
behind them: **[docs/agentflow_architecture.md](docs/agentflow_architecture.md)**

---

## Core Concepts

**Agent** — an AI worker packaged as a container image with a typed input/output
contract. Language agnostic.

**Task** — one dispatch to an agent. Lifecycle:
`pending → running → completed | failed | cancelled`, with retry back to
`pending` and a backoff window.

**Workflow** — a DAG of tasks, submitted as a YAML manifest. Independent branches
run in parallel automatically.

**Engine** — a node that claims and executes tasks. Engines register themselves,
heartbeat, and hold time-bounded leases on the work they claim.

---

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

---

## Getting Started

Requires Go 1.25+ and Docker.

```bash
git clone https://github.com/justinndidit/agentflow
cd agentflow
go mod tidy

# start Postgres
docker compose -f docker-compose.dev.yml up -d
```

Migrations are applied automatically on boot. Run once to create the schema —
this will fail at submit, because `tasks.agent_name` is a foreign key to
`agents(name)` and no agents exist yet:

```bash
go run ./cmd/agentflow
```

Seed the agents referenced by the example manifest:

```bash
docker exec -i agentflow_db psql -U postgres -d agentflow < scripts/seed_agents.sql
```

Then submit:

```bash
go run ./cmd/agentflow
```

```
INF starting db migration... func=Migrate
INF no schema change func=Migrate
INF db migrated successfully func=Migrate
INF submitManifest request func=SubmitManifest
workflow submitted successfully!
```

Inspect what landed:

```bash
docker exec agentflow_db psql -U postgres -d agentflow -c \
  "SELECT task_key, status, remaining_deps FROM tasks
    WHERE workflow_id = (SELECT id FROM workflows ORDER BY created_at DESC LIMIT 1)
    ORDER BY remaining_deps, task_key"
```

Tasks with `remaining_deps = 0` are the ones a dispatcher would claim first.
Nothing claims them yet.

Each submission creates a new workflow run, so submitting the same manifest twice
gives you two independent graphs — hence the scoping by workflow above.

Use a different manifest with `-manifest`:

```bash
go run ./cmd/agentflow -manifest path/to/workflow.yml
```

---

## Project Layout

```
cmd/agentflow/
    main.go                  — entry point: config, migrate, submit

internal/
    manifest/
        parser.go            — YAML schema and parsing
        validate.go          — template reference validation
    engine/
        dag.go               — cycle detection at submit time
        manifest.go          — submit pipeline
        pool.go              — node-local concurrency (being rewritten)
    state/
        task.go              — task state machine and transition rules
        workflow.go          — workflow state
    persistence/
        database/            — connection pool, migrations
        models/              — database row types
        repositories/        — stores, transaction manager
    dtos/                    — API response types
    runtime/                 — Docker agent runtime (not implemented)
    config/

migrations/                  — schema, applied by golang-migrate
scripts/seed_agents.sql      — development seed data
docs/                        — architecture
pkg/                         — logger, set, json helpers
```

---

## Design Principles

**Determinism at the orchestration layer** — AI workers are probabilistic;
execution control is not. The engine behaves predictably.

**Reliability over intelligence** — Agentflow does not make AI workers smarter.
It makes them operationally reliable.

**Exactly-once state, at-least-once execution** — Postgres gives transactional
certainty about state. It cannot give it about an agent's side effects, so
workers are required to be idempotent under a supplied key. The guarantee is
stated where it can actually be kept.

**Language agnostic** — workers are containers. The engine does not care what is
inside.

**Infrastructure first** — registry, marketplace, and discovery are emergent
layers. The execution engine is the product.

---

## Built With

- [Go](https://golang.org) — engine
- PostgreSQL — source of truth, task queue, coordination
- [pgx](https://github.com/jackc/pgx) — Postgres driver
- [golang-migrate](https://github.com/golang-migrate/migrate) — schema migrations
- Docker — worker sandboxing (planned)
- OpenTelemetry — distributed tracing (planned)

---

## License

MIT
