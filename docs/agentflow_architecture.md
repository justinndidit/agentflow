# Agentflow Architecture

**Status:** v2 — distributed. Supersedes the single-process wave design.

---

## 0. What changed and why

Agentflow began as a single-process engine. One machine held the DAG in memory,
computed execution levels ("waves"), submitted each wave to a goroutine pool,
and blocked until every task in the wave reached a terminal state. Mutexes on
in-memory task structs were the source of truth.

That model does not survive distribution. The question "which node runs the wave
loop?" has no good answer — if every node runs it, every node submits every
task; if one node runs it, distribution is cosmetic and that node is a single
point of failure. And an in-memory mutex now guards a *local copy* of a row that
another node may already have changed, which makes races invisible rather than
impossible.

**Postgres is now the single source of truth.** Every scheduling decision is a
transaction against it. Engine nodes are stateless and interchangeable: any node
can execute any ready task, and losing a node loses no work.

### The central reframe: waves → readiness counters

The DAG is no longer an execution plan. It is a **validation artifact**, checked
once at submit time for cycles and unknown references, then discarded.

At runtime there is no graph traversal and no global coordinator. Each task row
carries `remaining_deps`. A task is **ready** iff:

```
status = 'pending' AND remaining_deps = 0 AND not_before <= now()
```

When a task completes, the same transaction that records its result decrements
`remaining_deps` on its dependents. Readiness propagates through the data, not
through a scheduler.

This also removes head-of-line blocking that waves imposed even on one machine.
Under waves, `build-skill-gap-report` waited on the slowest task in every
intervening level. Under readiness counters it waits on its own three parents and
nothing else.

| Component | v1 | v2 |
|---|---|---|
| `engine/executor.go` | wave orchestration | **deleted** |
| `engine/dag.go` | produced execution waves | submit-time cycle validation only |
| `engine/pool.go` | bounded workflow concurrency | bounded **node-local** container concurrency |
| `state.Task.mu` | source of truth | **deleted** — the row is the truth |
| `internal/state` | in-memory state machine | transition table that generates SQL guards |

---

## 1. System overview

```text
┌──────────────────────────────────────────────────────────────────────┐
│                        SUBMIT PATH (once per run)                    │
└──────────────────────────────────────────────────────────────────────┘

   CLI / API
      │  agentflow submit workflow.yml
      ▼
 ┌─────────────────┐
 │ Manifest Parser │  schema validation, unknown-dependency check
 └────────┬────────┘
          ▼
 ┌─────────────────────┐
 │ Identity Assignment │  task_key ──► UUID,  agent name ──► agent_id
 │                     │  depends_on keys resolved to UUIDs here
 └────────┬────────────┘
          ▼
 ┌─────────────────┐
 │ DAG Validation  │  topological sort — cycle detection ONLY
 └────────┬────────┘   (result is thrown away)
          ▼
 ┌──────────────────────────────────────────┐
 │        Single Transaction                │
 │  INSERT workflow                         │
 │  INSERT tasks (remaining_deps = |deps|)  │
 │  NOTIFY agentflow_ready                  │
 └──────────────────────────────────────────┘
          ▼
      PostgreSQL  ◄──────── single source of truth


┌──────────────────────────────────────────────────────────────────────┐
│                   EXECUTION PATH (continuous, N nodes)               │
└──────────────────────────────────────────────────────────────────────┘

      PostgreSQL
          ▲  ▲  ▲
          │  │  └───────────────────────────────┐
          │  └──────────────┐                   │
          │                 │                   │
   ┌──────┴──────┐   ┌──────┴──────┐     ┌──────┴──────┐
   │  Engine A   │   │  Engine B   │ ... │  Engine N   │
   │─────────────│   │─────────────│     │─────────────│
   │ Registrar   │   │ Registrar   │     │ Registrar   │
   │ Dispatcher  │   │ Dispatcher  │     │ Dispatcher  │
   │ Worker Pool │   │ Worker Pool │     │ Worker Pool │
   │ Committer   │   │ Committer   │     │ Committer   │
   │ Reaper      │   │ Reaper      │     │ Reaper      │
   │─────────────│   │─────────────│     │─────────────│
   │   Docker    │   │   Docker    │     │   Docker    │
   │  ┌───┐┌───┐ │   │  ┌───┐      │     │  ┌───┐┌───┐ │
   │  │ct ││ct │ │   │  │ct │      │     │  │ct ││ct │ │
   │  └───┘└───┘ │   │  └───┘      │     │  └───┘└───┘ │
   └─────────────┘   └─────────────┘     └─────────────┘

Nodes never talk to each other. All coordination is through Postgres.
```

---

## 2. Engine node anatomy

Every node runs the same five loops. There is no leader, no election, no
special node.

### 2.1 Registrar

On boot, inserts a row into `engines` with a fresh UUID and the node's declared
capacity. Then updates `heartbeat_at` every `heartbeat_interval` (default 5s).

Liveness lives **here**, not on tasks. A per-task heartbeat costs one write per
running task per interval — write load proportional to cluster concurrency, on
the hottest table in the system. A per-engine heartbeat costs one write per node
per interval. Task liveness is derived by joining to the owning engine.

On graceful shutdown: set `status='draining'`, stop claiming, finish in-flight
work, then `status='stopped'`.

### 2.2 Dispatcher (claim loop)

Wakes on `NOTIFY agentflow_ready` or on a poll tick (default 2s), whichever
comes first, then claims a batch:

```sql
UPDATE tasks t
   SET status           = 'running',
       engine_id        = $1,
       lease_epoch      = t.lease_epoch + 1,
       lease_expires_at = now() + $2::interval,
       attempt          = t.attempt + 1,
       started_at       = COALESCE(t.started_at, now()),
       updated_at       = now()
 WHERE t.id IN (
     SELECT id FROM tasks
      WHERE status = 'pending'
        AND remaining_deps = 0
        AND not_before <= now()
      ORDER BY priority DESC, created_at
      FOR UPDATE SKIP LOCKED
      LIMIT $3
 )
RETURNING t.*;
```

`SKIP LOCKED` is what makes this contention-free at scale — two nodes racing for
the same row don't block, the loser simply takes a different row.

**Batch, don't single-claim.** One round trip per task caps throughput at
`1 / rtt` per node. Batch size should track free pool capacity.

**Never claim past capacity.** `LIMIT` is `min(batch_size, pool_free_slots)`.
Claiming work you can't start means holding a lease you can't honour.

After claiming, the dispatcher **resolves input templates** (§5) and hands each
task to the pool.

### 2.3 Worker pool

Node-local bounded concurrency, backpressure, clean shutdown — this is the one
component that survives v1 largely intact. It now bounds *containers on this
host*, sized to the node's CPU/memory rather than to a workflow's declared
worker count.

### 2.4 Committer

Writes terminal outcomes. Every write is **fenced** (§4.1) and every write is a
single transaction that does all of:

1. Guarded update of the task row.
2. Insert into `task_results`.
3. Decrement `remaining_deps` on dependents.
4. Update workflow counters.
5. `NOTIFY` if any dependent became ready.

If any step fails, none of it happened, and the lease expiry will cause a clean
retry.

### 2.5 Reaper

Every node runs the reaper on a tick. It is idempotent under `SKIP LOCKED`, so
concurrent reaping is safe — which lets us skip leader election entirely, a
large complexity saving for one of the fussier parts of any distributed system.

Two reclaim conditions:

```sql
-- 1. Task on a dead engine (engine stopped heartbeating)
status = 'running' AND engine_id IN (
    SELECT id FROM engines WHERE heartbeat_at < now() - $lease_ttl
)

-- 2. Task overran its own lease on a live engine (hung container)
status = 'running' AND lease_expires_at < now()
```

Reclaimed tasks return to `pending` if `attempt <= max_retries`, else to
`failed` with cascade (§6.2). The `lease_epoch` bump on the next claim is what
neutralises the original owner if it ever comes back.

---

## 3. Data model

### 3.1 `engines`

```sql
CREATE TYPE engine_status AS ENUM ('active', 'draining', 'stopped');

CREATE TABLE engines (
    id            UUID PRIMARY KEY,
    hostname      TEXT NOT NULL,
    status        engine_status NOT NULL DEFAULT 'active',
    capacity      INTEGER NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    heartbeat_at  TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_engines_liveness ON engines (heartbeat_at)
    WHERE status = 'active';
```

### 3.2 `tasks` — kept deliberately narrow

```sql
CREATE TYPE task_status AS ENUM (
    'pending', 'running', 'completed', 'failed', 'cancelled'
);

CREATE TABLE tasks (
    id               UUID PRIMARY KEY,
    workflow_id      UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    task_key         VARCHAR(256) NOT NULL,
    agent_id         UUID NOT NULL REFERENCES agents(id),
    status           task_status NOT NULL DEFAULT 'pending',

    depends_on       UUID[] NOT NULL DEFAULT '{}',
    remaining_deps   INTEGER NOT NULL,

    input_template   JSONB NOT NULL,

    engine_id        UUID REFERENCES engines(id),
    lease_epoch      BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,

    priority         SMALLINT NOT NULL DEFAULT 0,
    not_before       TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_retries      INTEGER NOT NULL DEFAULT 0,
    timeout          INTERVAL,

    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,

    UNIQUE (workflow_id, task_key),
    CHECK (remaining_deps >= 0),
    CHECK (attempt <= max_retries + 1)
);
```

Two details that are easy to get wrong and expensive to debug:

**`lease_epoch` must be `NOT NULL DEFAULT 0`.** If it is nullable, the fencing
predicate `WHERE lease_epoch = $3` never matches a `NULL` — SQL three-valued
logic makes the comparison `UNKNOWN`, not `TRUE`. Every fenced write would affect
zero rows, so every worker would classify itself as a zombie and discard valid
results. The system would appear to run and silently complete nothing.

**Table constraints need commas between them.** `UNIQUE (...) CHECK (...)` with
no separator is a syntax error in Postgres, and because migrations are not
implicitly transactional per statement in all configurations, it can leave the
schema half-created and golang-migrate marked dirty.

Note `input_template`, not `input_payload`. What is stored is the manifest as
authored, including unresolved `{{ }}` expressions. The resolved input is
computed at dispatch and is a different object (§5).

### 3.3 Indexes — the partial index is the important one

```sql
-- HOT PATH. Partial index contains only ready rows, so it stays small
-- and fully cached regardless of total table size.
CREATE INDEX idx_tasks_ready ON tasks (priority DESC, created_at)
    WHERE status = 'pending' AND remaining_deps = 0;

-- Reverse edges: "who depends on me?" for the completion decrement.
CREATE INDEX idx_tasks_depends_on ON tasks USING GIN (depends_on);

-- Reaper scan.
CREATE INDEX idx_tasks_leases ON tasks (lease_expires_at)
    WHERE status = 'running';

CREATE INDEX idx_tasks_workflow ON tasks (workflow_id, status);
```

A partial index on the ready predicate is the single highest-leverage decision
in this schema. A cluster with ten million historical task rows and forty ready
ones scans an index containing forty entries.

**On reverse edges:** the alternative is a denormalised `dependents UUID[]`
column. It is marginally faster to read but requires a dual write kept
consistent with `depends_on`. GIN costs one line and cannot drift. Take GIN.

### 3.4 `task_results` — split out on purpose

```sql
CREATE TABLE task_results (
    task_id      UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt      INTEGER NOT NULL,
    output       JSONB,
    artifact_uri TEXT,
    resolved_input JSONB,
    tokens_used  BIGINT NOT NULL DEFAULT 0,
    cost_micros  BIGINT NOT NULL DEFAULT 0,
    duration_ms  BIGINT,
    created_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (task_id, attempt)
);
```

Outputs are large and cold; the scheduling predicate is small and hot. Keeping
them in one table means every claim scan drags TOAST pointers and inflated rows
through shared buffers. Keyed by `(task_id, attempt)` so failed attempts stay
inspectable rather than being overwritten.

### 3.5 `workflows` — with counters, not aggregate queries

```sql
CREATE TYPE workflow_status AS ENUM (
    'pending', 'running', 'completed', 'failed', 'cancelled'
);

CREATE TABLE workflows (
    id              UUID PRIMARY KEY,
    name            VARCHAR(256) NOT NULL,
    namespace       VARCHAR(256) NOT NULL,
    manifest        BYTEA NOT NULL,
    version         INTEGER NOT NULL,
    status          workflow_status NOT NULL DEFAULT 'pending',

    task_total      INTEGER NOT NULL,
    task_completed  INTEGER NOT NULL DEFAULT 0,
    task_failed     INTEGER NOT NULL DEFAULT 0,
    task_cancelled  INTEGER NOT NULL DEFAULT 0,

    max_parallelism INTEGER,
    running_count   INTEGER NOT NULL DEFAULT 0,

    max_tokens      BIGINT,
    tokens_used     BIGINT NOT NULL DEFAULT 0,
    default_timeout INTERVAL NOT NULL,

    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_workflows_name_ns ON workflows (namespace, name);
```

Workflow completion is `completed + failed + cancelled = task_total`, maintained
transactionally by the committer. The alternative — `SELECT count(*) ... GROUP BY
status` on every task completion — is correct but turns every finish into a scan
of that workflow's tasks.

Workflows get their own status enum. Reusing `task_status` for both worked by
coincidence and would block adding workflow-only states later.

**`max_parallelism` is nullable and should usually stay NULL.** Enforcing a
per-workflow concurrency cap requires incrementing `running_count` in the claim
transaction, which serialises all claims for that workflow behind one row lock.
That is fine across many small workflows and a hard bottleneck inside one large
one. Pay for it only where it's actually wanted.

Note that `workers: 8` from the v1 manifest no longer means anything — pool size
is a property of a *node*, not a workflow. It is replaced by `max_parallelism`
with different semantics: a global cap on concurrently running tasks, not a
local goroutine count.

---

## 4. Execution semantics

**Agentflow guarantees exactly-once state transitions and at-least-once
execution.** These are different things and the distinction is load-bearing.

Postgres gives transactional certainty about *state*. It gives nothing about the
agent's actual work. A node claims a task, the container spends $2 on LLM calls
and sends an email, then the kernel panics before commit. The lease expires,
another node reclaims, and it happens again. No design closes this — the side
effects are outside any transaction we control. So the honest architecture puts
the guarantee where it can actually be kept, and states the rest plainly.

### 4.1 Fencing — `lease_epoch`

Without fencing, a stalled node is a data-corruption bug:

```
t0   Engine A claims task T, begins work
t1   Engine A stalls (GC pause / network partition / VM freeze)
t2   Lease expires. Reaper returns T to pending.
t3   Engine B claims T, runs it, writes result. T is completed.
t4   Engine A wakes up, finishes, writes ITS result over B's.
```

`lease_epoch` is a monotonically increasing counter bumped on every claim. Every
write from a worker carries the epoch it was issued:

```sql
UPDATE tasks
   SET status = 'completed', finished_at = now(), updated_at = now()
 WHERE id = $1 AND engine_id = $2 AND lease_epoch = $3
   AND status = 'running';
```

Zero rows affected means "you are a zombie." The node discards its result and
logs a fencing event. At `t4` above, A's epoch is stale and its write is a
no-op.

This is cheap and it is not optional — **the reaper is unsafe without it.** Any
system with lease reclamation and no fencing has this race live.

### 4.2 The worker idempotency contract

Because execution is at-least-once, a worker may be invoked more than once for
the same task. Every invocation receives a stable idempotency key:

```
AGENTFLOW_IDEMPOTENCY_KEY = <task_id>
```

Stable across retries **by design** — a retry after a crash should let the
worker recognise work it already did rather than redo it. Workers that perform
external side effects are required to honour this key.

This is a product decision as much as a technical one. It is a constraint on
everyone who ever writes an agent for Agentflow, so it belongs in the worker
contract from the first version rather than being retrofitted once someone has
been double-billed. `attempt` is also passed, for logging and for workers that
genuinely want to distinguish retries.

### 4.3 Cost accounting is best-effort

`tokens_used` is written when a task completes. A crash between spend and commit
undercounts, permanently. Trying to make this exact means a distributed
transaction with the model provider, which does not exist.

So `max_tokens` is a **soft ceiling**: checked before dispatch, enforced by
killing in-flight containers when the workflow crosses it. Budget overrun is
detected, not prevented. This is worth stating loudly in user-facing docs.

---

## 5. Template resolution happens at dispatch, never at submit

The manifest supports references to upstream outputs:

```yaml
input:
  jobs: "{{ tasks.filter-top-opportunities.output.jobs }}"
```

At submit time that value does not exist. So:

- `tasks.input_template` stores the expression **unresolved**, as authored.
- At claim time the dispatcher reads dependency rows from `task_results`,
  resolves the expressions, and passes the concrete input to the container.
- The resolved input is recorded on `task_results.resolved_input` for
  debuggability.

Resolution failure (missing key, wrong shape) is a **task failure**, not an
engine error — it means the upstream agent produced something the manifest did
not expect, which is a workflow bug and should surface as one.

### 5.1 Large outputs go to blob storage

Postgres is the right bus for small JSON. It is the wrong bus for the PDFs,
embeddings, and reports these workflows actually produce.

Rule: outputs under a threshold (default 256 KB) are stored inline in
`task_results.output`. Larger outputs go to S3/MinIO with `artifact_uri`
holding the reference. Workers write blobs directly using credentials supplied
by the engine; the engine never proxies bytes.

---

## 6. Failure semantics

### 6.1 Retry with backoff

Failure with `attempt <= max_retries` returns the task to `pending` with
`not_before = now() + backoff(attempt)`, exponential with jitter. Jitter matters:
without it, a downstream provider outage produces a synchronised retry stampede
across every node.

`remaining_deps` is **not** touched on retry — dependencies are still satisfied.

### 6.2 Permanent failure cascades

When a task exhausts retries, its dependents can never become ready. Left alone
they sit `pending` forever and the workflow never terminates.

So permanent failure **recursively cancels all transitive dependents**:

```sql
WITH RECURSIVE doomed AS (
    SELECT id FROM tasks WHERE id = $1
    UNION
    SELECT t.id FROM tasks t
      JOIN doomed d ON d.id = ANY(t.depends_on)
)
UPDATE tasks SET status = 'cancelled', finished_at = now()
 WHERE id IN (SELECT id FROM doomed) AND status = 'pending';
```

Independent branches keep running — cancelling the whole workflow on one failure
would throw away completed, paid-for work. The workflow ends `failed`, but every
task that *could* finish did.

### 6.3 Task state machine

```text
                    ┌──────────────┐
       ┌───────────►│   pending    │◄────────────┐
       │            └──────┬───────┘             │
       │                   │ claim (fenced)      │ retry
       │                   ▼                     │ (attempt <= max_retries,
       │            ┌──────────────┐             │  not_before = +backoff)
       │            │   running    │─────────────┘
       │            └──┬────┬───┬──┘
       │               │    │   │
       │       success │    │   │ lease expired / engine dead
       │               │    │   └──────► back to pending (epoch bumped)
       │               ▼    ▼
       │      ┌───────────┐ ┌──────────┐
       │      │ completed │ │  failed  │ (retries exhausted)
       │      └───────────┘ └────┬─────┘
       │                         │ cascade
       │                         ▼
       │                  ┌─────────────┐
       └──────────────────│  cancelled  │  (transitive dependents)
                          └─────────────┘

Every transition is a guarded UPDATE:
  WHERE id = $1 AND engine_id = $2 AND lease_epoch = $3 AND status = $4
The WHERE clause IS the state machine. There is no mutex.
```

---

## 7. Scale considerations

Single-tenant, but built to not fall over.

**Claim contention.** `SKIP LOCKED` plus the partial ready-index is good for
thousands of claims/sec on modest hardware. The first thing to break will be the
dispatcher poll interval creating a thundering herd at low task counts — hence
`NOTIFY` as the fast path, poll as the floor.

**Connection pooling vs. `LISTEN/NOTIFY`.** Beyond a few dozen nodes,
`max_connections` forces PgBouncer. Transaction-mode pooling **breaks `LISTEN`**
(and session advisory locks). Therefore: each node holds one *direct* connection
for `LISTEN` and routes all transactional work through the pooler. Designing this
in now costs nothing; retrofitting it means reworking the dispatcher.

**Write amplification.** The three amplifiers, in order: per-task heartbeats
(eliminated by §2.1), workflow counter updates (one hot row per workflow —
acceptable, and the reason `max_parallelism` is opt-in), and result payloads in
the hot table (eliminated by §3.4).

**Table growth.** `tasks` and `task_results` grow without bound. Retention or
partition-by-month on `created_at` is needed before this matters — noted, not
yet built.

**Postgres is the ceiling.** That is an accepted trade. It buys transactional
correctness that would otherwise require consensus machinery, and the ceiling is
far above the target. Sharding by workflow is the escape hatch if ever needed.

---

## 8. Worker contract (Docker runtime)

Containers are executed by the node that claimed the task. `internal/runtime`
exposes a `Runtime` interface so a Kubernetes backend can be added later without
touching the scheduler; Docker is the only implementation for now.

**Input** — resolved JSON on `stdin`, plus environment:

```
AGENTFLOW_TASK_ID           uuid
AGENTFLOW_WORKFLOW_ID       uuid
AGENTFLOW_TASK_KEY          string
AGENTFLOW_ATTEMPT           integer
AGENTFLOW_IDEMPOTENCY_KEY   stable across retries
AGENTFLOW_ARTIFACT_URI      pre-signed blob destination
```

**Output** — JSON on `stdout`. Exit 0 = success, non-zero = failure, `stderr`
captured as logs and truncated into `error_message` on failure.

**Enforced by the runtime:** memory and CPU limits, wall-clock timeout matching
the lease, network policy, read-only root filesystem, non-root user, no Docker
socket mount.

The engine does not know or care what runs inside. That is the point.

---

## 9. Explicitly out of scope for v1

**`foreach` / dynamic fan-out.** The example manifest uses it; the engine will
reject it. Runtime task-count expansion makes the DAG mutable — rows inserted
mid-workflow, `remaining_deps` recomputed on join points after a fan-out
resolves, and a barrier primitive that does not exist yet. It is the single
largest piece of complexity available and it should not be half-built alongside
a scheduler that has never run in production. The schema does not preclude it.

**Multi-tenancy.** Single entity. No `tenant_id`, no RBAC, no per-tenant quotas.
Namespaces exist for organisation, not isolation.

**`schedule:` / cron triggers, `approval_required:`.** Present in the example
manifest, not implemented. Both should be removed from the example until they
are, so the docs stop promising them.

---

## 10. Build order

1. **Identity and schema.** Fix `task_key` ↔ UUID mapping in the submit path,
   land the v2 schema, call the migrator from `main`.
2. **Persistence for real.** Implement the store methods that currently return
   `nil, nil`.
3. **Claim / commit / reap loops.** Delete `executor.go`. Prove correctness with
   the existing sleep-worker before any container is involved.
4. **Multi-node test.** Two engines against one Postgres, `SIGKILL` one
   mid-workflow, assert the workflow still completes and that fencing rejects
   the dead node's late write.
5. **Docker runtime.** Swap the sleep worker for real containers.
6. **Templates and blob storage.**
7. **Observability** — OpenTelemetry traces per workflow, cost aggregation.

Steps 3 and 4 are the ones that decide whether this architecture is real. Do not
skip 4.
