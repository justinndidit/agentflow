# Agentflow

![Agentflow executing a dependency-aware workflow](demo.gif)

Agentflow is an open-source execution engine for AI workers.

Think of it as Kubernetes for AI agents — you bring the worker, Agentflow handles
scheduling, lifecycle, cancellation, observability, and scale.

> "The valuable layer is not discovering agents. It is running them safely,
> composing them reliably, and scaling them predictably."

---

## The Problem

Teams building AI systems today face the same infrastructure problems repeatedly:

- No standard deployment unit for AI workers
- Brittle glue code stitching agents together
- No reliable retry, cancellation, or failure semantics
- Poor observability into what agents are actually doing
- Tight coupling to Python frameworks and specific SDK ecosystems
- No governance — cost controls, audit trails, access policies

Agentflow solves the infrastructure layer so you can focus on the worker logic.

---

## Core Concepts

**Worker** — an AI agent packaged as a Docker container with a typed
input/output contract. Language agnostic. Bring Go, Python, Node, Rust — anything.

**Task** — a unit of work dispatched to a worker. Has a lifecycle:
`pending → running → completed / failed / cancelled`.

**Workflow** — a directed acyclic graph (DAG) of tasks. Agentflow resolves
dependencies and schedules tasks in the correct order, running independent
tasks in parallel automatically.

**Worker Pool** — a bounded pool of concurrent executors. Provides
backpressure — when the pool is at capacity, new submissions are rejected
immediately rather than blocking indefinitely.

---

## Architecture

[Full architecture blueprint](docs/agentflow_architecture.md)

AgentFlow is moving from an in-memory, single-process engine toward a
PostgreSQL-backed distributed coordination model — the same pattern
Kubernetes' controller-manager and Temporal.io use: a durable state store
as the single source of truth, with a reconciliation loop claiming and
driving work forward instead of in-process channels.

```
cmd/agentflow/
    main.go                      — entry point

internal/
    config/
        config.go                — app configuration

    engine/
        dag.go                   — Task Graph: topological sorting (Kahn's algorithm)
        executor.go               — Executor: initializes worker pool, runs waves of tasks
        manifest.go               — manifest → execution graph wiring
        pool.go                   — WorkerPool: Submit, Start, drain on cancellation
        worker.go                 — worker: run with context-aware cancellation

    manifest/
        parser.go                 — YAML workflow manifest parser, schema validation

    persistence/
        database/
            database.go           — DB connection handling
            migrator.go            — migration runner
        domain/
            agent.go               — Agent domain struct
            base.go                 — shared base fields
            task.go                  — Task domain struct
            workflow.go              — Workflow domain struct
        repositories/
            repository.go           — shared repository interfaces
            task.go                   — Task persistence
            workflow.go                — Workflow persistence

    runtime/
        docker.go                  — Docker agent runtime (in progress)

    state/
        task.go                     — Task, TaskStatus, state machine with transition guards

migrations/
    000001_init_db.up.sql / .down.sql   — schema migrations (workflows, tasks, enums, indexes)

pkg/
    logger/
        logger.go                  — structured transition logging
    set/
        set.go                      — generic set utility
```

---

## What's Built

### v0.1 — In-Memory Concurrent Engine

- [x] Concurrent worker pool with bounded concurrency
- [x] Context-aware cancellation — workers stop cleanly mid-execution
- [x] Enforced state machine — invalid transitions are rejected
- [x] Per-task mutex — tasks transition safely under concurrent access
- [x] Backpressure via non-blocking Submit — pool rejects when at capacity
- [x] Clean shutdown — no leaked goroutines, resultChan closes deterministically
- [x] Structured execution logging

### v0.2 — Dependency-Aware Scheduling

- [x] Resolve task dependencies via `DependsOn []string`
- [x] Topological sort for execution ordering
- [x] Parallel execution of independent tasks
- [x] Block tasks until all dependencies complete

### v0.3 — Manifest Parser

- [x] YAML workflow definitions
- [x] Template expressions (`{{ trigger.topic }}`)
- [x] Schema validation

### v0.4 — Durable State Store (in progress)

- [x] PostgreSQL schema: `workflows` and `tasks` tables, `task_status` enum
- [x] Migration tooling (`golang-migrate`-style up/down files)
- [x] Partial indexes for the reconciliation hot path (`idx_tasks_ready`, `idx_tasks_orphan`)
- [x] Domain structs separated from DB row structs (`persistence/domain`)
- [x] Repository layer for workflows and tasks
- [ ] Task dependency resolution: mapping manifest `task_key` strings to
      database UUIDs before building domain objects
- [ ] Atomic `TransitionTask(ctx, taskID, from, to)` with invariant guards
- [ ] `SELECT FOR UPDATE SKIP LOCKED` reconciliation loop

---

## Roadmap

**v0.5 — Reconciliation Engine**
- Decoupled reconciliation loop (poll → claim → mark running → execute)
- Heartbeat writer + orphan task sweeper (self-healing on crash)
- Graceful shutdown — in-flight tasks drain before exit

**v0.6 — Docker Sandbox Runtime**
- Spawn Docker containers as workers via the Docker SDK
- Hard resource enforcement: CPU/memory limits, read-only rootfs
- A2A protocol I/O broker: stdout/stdin contracts, output schema validation

**v0.7 — Observability**
- Full execution traces per workflow (OpenTelemetry)
- Prometheus metrics: queue depth, execution latency, orphan reclamations
- Failure root-cause analysis

**v0.8 — Chaos Verification**
- Integration test harness against real PostgreSQL (testcontainers-go)
- Fault injection: mid-task crash, dual-engine race, container OOM, cyclic manifest rejection

**Forge — Managed Cloud**
- Hosted Agentflow clusters
- Agent registry
- Workflow marketplace
- Usage-based billing

---

## Getting Started

```bash
git clone https://github.com/justinndidit/agentflow
cd agentflow
go mod tidy
go run cmd/agentflow/main.go
```

Expected output:

```
[t-1] [pending] -> [running]    (worker 1)
[t-2] [pending] -> [running]    (worker 2)
[t-3] [pending] -> [running]    (worker 3)
[t-1] [running] -> [completed]  (worker 1)
...
All done!
```

---

## Design Principles

**Determinism at the orchestration layer** — even though AI workers are
probabilistic, execution control is deterministic. The engine always
behaves predictably.

**Databases coordinate systems, channels coordinate goroutines** — as
AgentFlow moves from in-process channels to a PostgreSQL-backed
reconciliation loop, this is the guiding line: channels are the wrong
primitive for work that must survive process restarts and scale across
nodes.

**Reliability over intelligence** — Agentflow does not make AI workers
smarter. It makes them operationally reliable.

**Language agnostic** — workers are Docker containers. The engine does
not care what is inside.

**Infrastructure first** — marketplace, registry, and discovery are
emergent layers. The execution engine is the product.

---

## Built With

- [Go](https://golang.org) — runtime and engine
- PostgreSQL — durable state store, `SELECT FOR UPDATE SKIP LOCKED` coordination
- Docker — worker sandboxing (v0.6)
- A2A Protocol — agent communication standard (v0.6)
- OpenTelemetry — distributed tracing (v0.7)

---

## License

MIT