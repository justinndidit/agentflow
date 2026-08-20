# Execution Path — Build Plan

Companion to [agentflow_architecture.md](agentflow_architecture.md). That document
says what the system is; this one says what to build next and in what order.

**Status:** steps 1 and 2 of the architecture doc's build order (§10) are done.
The schema is landed, the migrator runs from `main`, and the submit path
persists a validated graph transactionally. Step 3 — claim, commit, reap — is
this phase.

At the end of it, a submitted workflow runs to completion on its own, across
more than one node, with no containers involved yet.

---

## Read this first: three schema facts

The architecture doc was written before the schema was implemented, and the
implementation deliberately diverged in one place. That divergence changes three
queries the doc gives verbatim. Getting these wrong produces bugs that look like
scheduling flakiness rather than SQL errors.

### 1. `depends_on` holds task keys, not UUIDs

The doc's §3.2 declares `depends_on UUID[]` and `agent_id UUID REFERENCES
agents(id)`. The migration actually shipped `depends_on TEXT[]` holding
`task_key` values, and `agent_name TEXT REFERENCES agents(name)`.

Task keys are only unique **per workflow** — the constraint is
`UNIQUE (workflow_id, task_key)`. So every query that walks the dependency edge
must be scoped by `workflow_id`, or two concurrent runs of the same manifest
will decrement each other's counters.

The doc's cascade query (§6.2) joins on `d.id = ANY(t.depends_on)` and does not
work as written. The corrected form:

```sql
WITH RECURSIVE doomed AS (
    SELECT task_key FROM tasks WHERE id = $1
    UNION
    SELECT t.task_key
      FROM tasks t
      JOIN doomed d ON d.task_key = ANY(t.depends_on)
     WHERE t.workflow_id = $2
)
UPDATE tasks
   SET status = 'cancelled', finished_at = now(), updated_at = now()
 WHERE workflow_id = $2
   AND task_key IN (SELECT task_key FROM doomed)
   AND status = 'pending';
```

And the committer's decrement:

```sql
UPDATE tasks
   SET remaining_deps = remaining_deps - 1, updated_at = now()
 WHERE workflow_id = $1
   AND depends_on @> ARRAY[$2]::text[];
```

`idx_tasks_depends_on` is a GIN index, so `@>` uses it. The `workflow_id`
predicate is not optional.

### 2. `CHECK (attempt <= max_retries + 1)` can poison a whole claim batch

The claim sets `attempt = attempt + 1` across a batch in one statement. If any
row in that batch is already at its ceiling, the CHECK fails, and Postgres
aborts **the entire statement** — not just that row. The dispatcher then retries,
selects the same poisoned row, and fails again. One bad row stalls the node.

Two defences, and take both:

- The claim's inner `SELECT` filters `attempt <= max_retries`, so an exhausted
  task is never selected in the first place.
- The reaper never returns an exhausted task to `pending`. It goes to `failed`
  and cascades.

### 3. `idx_tasks_ready` does not cover `not_before`

The index predicate is `status = 'pending' AND remaining_deps = 0`. The claim
predicate adds `not_before <= now()`. Rows sitting in retry backoff are in the
index and get filtered after the scan.

This is fine at low retry volume and degrades during a provider outage, which is
exactly when every task in a workflow is in backoff at once. Decide deliberately:
extend the index to include `not_before`, or accept it and revisit under load.
Note the decision either way.

### Also true, less subtle

- **The submit transaction does not `NOTIFY`.** The doc's §1 diagram shows it.
  Needed in M5 for the fast path; until then the dispatcher polls.
- **`UpdateTask` is not usable for scheduling**, and says so in its own comment.
  It matches on `id` alone and is last-writer-wins. Claim, complete, fail and
  reap each need their own guarded statement. `UpdateTask` stays for
  administrative edits.
- **`workers:` in the manifest no longer means anything.** Pool size is a
  property of a node, not a workflow. `NewWorkflowRowFromDefinition` currently
  maps it to `max_parallelism`, which the doc (§3.5) says should usually stay
  NULL because enforcing it serialises every claim for that workflow behind one
  row lock. Resolve this in M2.

---

## M0 — Clear the ground

`executor.go`, `worker.go` and `pool.go` are fully commented out, along with
their three test files. The architecture doc calls for deleting `executor.go`
outright, and `pool.go` is being rebuilt with different semantics — it bounded a
workflow's declared worker count, and will now bound containers on a host.

Delete all six files. Nothing imports them.

**Done when:** `internal/engine` contains no commented-out code and the build is
unchanged.

---

## M1 — Engine identity

Nothing can claim a task until engines exist as rows. The `engines` table and
`models.Engine` are already there; the store and the loop are not.

**Build**

- `repositories/engine.go` — `EngineStore`: `Register`, `Heartbeat`, `Drain`,
  `Stop`, `ListStale(olderThan)`.
- `engine/registrar.go` — insert on boot with a fresh UUID, hostname and
  capacity; tick `heartbeat_at` every interval; on `SIGTERM` set `draining`,
  stop claiming, finish in-flight work, then `stopped`.
- Config: `AGENTFLOW__ENGINE__CAPACITY`, `__HEARTBEAT_INTERVAL`, `__LEASE_TTL`.
  Defaults 5s heartbeat, 60s lease.

Liveness lives on the engine, not the task — one write per node per interval
instead of one per running task per interval.

**Prove**

- Register inserts an `active` row; two registrars get distinct ids.
- Heartbeat advances `heartbeat_at`; a second tick advances it again.
- `ListStale` returns an engine whose heartbeat is older than the TTL and
  ignores a fresh one — this is the reaper's input in M4.
- Graceful shutdown leaves the row `stopped`, not `active`.

**Done when:** a process registers, visibly heartbeats, and marks itself stopped
on `SIGTERM`.

---

## M2 — The claim

The single most important query in the system.

**Build**

`TaskStore.ClaimTasks(ctx, engineID, limit, leaseTTL) ([]*TaskRow, error)`,
following the doc's §2.2 statement with the `attempt <= max_retries` filter from
fact 2 added to the inner select. `LIMIT` is `min(batchSize, poolFreeSlots)` —
never claim past capacity, because a lease you cannot honour is worse than an
unclaimed task.

Resolve the `max_parallelism` question here.

**Prove** — this milestone's tests are the ones that matter most in the project.

- **Concurrent claim.** N goroutines on N *separate connections* claim against a
  pool of M ready tasks. The union of what they claim is exactly M and the
  intersection is empty. This is the proof that `FOR UPDATE SKIP LOCKED` does
  its job, and it cannot be written without the integration harness.
- A claim sets `status='running'`, `engine_id`, `started_at`, and increments
  both `lease_epoch` and `attempt`.
- Tasks with `remaining_deps > 0` are never claimed.
- Tasks with `not_before` in the future are never claimed.
- Ordering is `priority DESC, created_at`.
- A batch containing a retry-exhausted task still succeeds — the regression test
  for fact 2.

**Done when:** two processes against one database claim disjoint sets under
sustained load.

---

## M3 — The committer

Every write from a worker is fenced. This is not optional: without it the reaper
in M4 is a data-corruption bug.

**Build**

Introduce a `Fence{TaskID, EngineID, LeaseEpoch}` value so no call site can
forget the guard, and a distinct `ErrFenced` — a zero-row result here means "you
are a zombie", which is categorically different from `ErrNotFound`.

`CompleteTask` is one transaction doing all five steps from doc §2.4: guarded
update, insert `task_results`, decrement dependents (workflow-scoped, per fact
1), update workflow counters, `NOTIFY` if anything became ready.

This is where `internal/state` earns its keep. The transition table stops being
an in-memory helper and starts generating the `WHERE` clause. The `WHERE` clause
is the state machine.

**Prove**

- A write with the correct epoch succeeds; a write with a stale epoch affects
  zero rows and returns `ErrFenced`.
- Completing a task decrements exactly the tasks that name it — **and nothing in
  another workflow that happens to use the same task key.** That is fact 1's
  bug, written as a test.
- **Concurrent fan-in.** Three upstreams complete simultaneously on separate
  connections; the shared downstream reaches exactly 0 and never goes negative.
- `task_results` is keyed `(task_id, attempt)`, so a retry adds a row rather than
  overwriting the failed attempt.
- Workflow counters sum to `task_total` when the run finishes.
- Failure with retries remaining returns the task to `pending` with
  `not_before = now() + backoff`, and does **not** touch `remaining_deps` —
  dependencies are still satisfied.

**Done when:** a test drives a two-task chain to completion by calling claim and
commit directly, with no engine process in the picture.

---

## M4 — The reaper

**Build**

Both reclaim conditions from doc §2.5 — task on a dead engine, and task that
overran its lease on a live one. Reclaimed tasks return to `pending` when
`attempt <= max_retries`, otherwise to `failed` followed by the cascade from
fact 1. Backoff is exponential **with jitter**; without jitter a provider outage
produces a synchronised retry stampede across every node.

Every node runs the reaper. It is idempotent under `SKIP LOCKED`, which is what
lets the whole system skip leader election.

**Prove**

- A task on an engine that stopped heartbeating is reclaimed.
- A task whose lease expired on a *live* engine is reclaimed — the hung-container
  case.
- **The zombie scenario, written literally.** Engine A claims, stalls; the lease
  expires; the reaper reclaims; engine B claims and completes; engine A wakes and
  writes. A's write must affect zero rows. This is doc §4.1's `t0`–`t4` sequence,
  and it is the test that decides whether the reaper is safe.
- Exhausted retries produce `failed` plus transitively cancelled dependents,
  while an independent branch keeps running to completion.
- Two reapers running concurrently reclaim the same backlog without
  double-processing anything.

**Done when:** killing a claim mid-flight results in another engine retrying the
task and the workflow still finishing.

---

## M5 — The engine process

Everything above is library code that tests drive. This turns it into a program.

**Build**

- Rewrite `pool.go`: node-local bounded concurrency, backpressure, clean drain on
  shutdown. Sized to the host, not to the manifest.
- `runtime.Runtime` interface plus a **sleep/echo implementation** — no Docker.
  The doc is explicit that correctness should be proven with a fake worker before
  containers enter the picture, and it means the whole loop is testable in CI.
- Split `cmd/`: `agentflow submit` (what `main` does today) and
  `agentflow engine` (long-running: registrar, dispatcher, pool, committer,
  reaper).
- `LISTEN/NOTIFY`: add the `NOTIFY` to the submit transaction, and give each node
  one **direct** connection for `LISTEN` while routing transactional work through
  the pool. Transaction-mode pooling breaks `LISTEN`, and designing around it now
  costs nothing.

**Done when:** `agentflow engine` picks up a submitted workflow and runs it to
completion with the sleep runtime, driven only by `NOTIFY` and the poll floor.

---

## M6 — Two nodes, one kill

The architecture doc says of this step: *do not skip 4.* It is the phase gate.

Two engine processes against one Postgres. Submit a workflow with enough tasks
that both nodes are working. `SIGKILL` one mid-flight — not `SIGTERM`, which
would drain gracefully and prove nothing.

Assert:

- The workflow still completes.
- Every task the dead node held was reclaimed and rerun.
- No task ran to a *committed* result twice.
- If the killed process is somehow resumed, its writes are fenced out.

**Done when:** that runs green repeatably. If it does not, the architecture is
not real yet, and nothing after this matters.

---

## After the gate

In order, and each one is a phase of its own:

**M7 — Template resolution at dispatch.** The parser already validates that a
task only references a task it depends on, so the hard correctness work is done.
This is reading `task_results` for the dependencies, substituting, and recording
the result on `resolved_input`. Resolution failure is a *task* failure, not an
engine error — it means the upstream agent produced a shape the manifest did not
expect.

**M8 — Docker runtime.** Swap the sleep worker for containers. Memory and CPU
limits, wall-clock timeout matching the lease, read-only root, non-root user, no
socket mount. The worker contract (doc §8) is already specified, including the
idempotency key.

**Then:** blob storage for large outputs, and observability — traces per
workflow, cost aggregation.

---

## Sequencing notes

M1 through M4 are library code with no process around them, which means every
one of them is fully provable with the integration harness before any of it runs
for real. That is deliberate: the failure modes here — a lost decrement, an
unfenced write, a poisoned batch — are the kind that look like flakiness in
production and are nearly free to catch in a test.

M0 is half a day. M2 and M3 are the substantial ones. M6 is short to write and
the only one that can invalidate the design.
