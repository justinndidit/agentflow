# Agentflow Architecture Diagrams

---

# 1. High-Level System Overview

**Description:**  
End-to-end architecture showing how a workflow request enters Agentflow, gets parsed, scheduled, executed by AI worker containers, and observed/governed.

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         AGENTFLOW ARCHITECTURE                      │
└─────────────────────────────────────────────────────────────────────┘

                         ┌──────────────────┐
                         │   CLI / SDK API  │
                         │  (submit run)    │
                         └────────┬─────────┘
                                  │
                                  ▼
                     ┌────────────────────────┐
                     │   Workflow Manifest    │
                     │     Parser (YAML)      │
                     └────────┬───────────────┘
                              │
                              ▼
                  ┌────────────────────────────┐
                  │      Workflow Builder      │
                  │  - Validate schema         │
                  │  - Build task objects      │
                  │  - Resolve dependencies    │
                  └────────┬───────────────────┘
                           │
                           ▼
              ┌────────────────────────────────┐
              │         DAG Scheduler          │
              │  - Build dependency graph      │
              │  - Detect cycles               │
              │  - Topological sort            │
              │  - Produce execution waves     │
              └────────┬───────────────────────┘
                       │
                       ▼
             ┌─────────────────────────────────┐
             │         Wave Executor           │
             │  Sequential wave orchestration  │
             │  Parallel task execution        │
             └────────┬────────────────────────┘
                      │
                      ▼
         ┌───────────────────────────────────────┐
         │              Worker Pool              │
         │  - Bounded concurrency                │
         │  - Backpressure                       │
         │  - Task queue                         │
         └───────┬───────────────┬───────────-───┘
                 │               │
        ┌────────▼──────┐ ┌──────▼────────┐
        │   Worker #1   │ │   Worker #N   │
        └────────┬──────┘ └──────┬────────┘
                 │               │
                 ▼               ▼
       ┌─────────────────────────────────┐
       │     Docker Agent Runtime        │
       │  - Spawn agent containers       │
       │  - Pass input via A2A protocol  │
       │  - Stream logs/output           │
       └──────────────┬──────────────────┘
                      │
                      ▼
          ┌─────────────────────────────┐
          │      AI Agent Container     │
          │  (developer-provided image) │
          └──────────────┬──────────────┘
                         │
                         ▼
              ┌────────────────────────┐
              │     Task Result        │
              │  output / artifacts    │
              └────────┬───────────────┘
                       │
                       ▼
        ┌──────────────────────────────────────┐
        │        State Machine Layer           │
        │ pending → running → terminal state   │
        └────────┬─────────────────────────────┘
                 │
                 ▼
     ┌────────────────────────────────────────────┐
     │         Observability & Governance         │
     │--------------------------------------------│
     │ OpenTelemetry traces                       │
     │ Structured logs                            │
     │ Cost tracking                              │
     │ Retry metrics                              │
     │ Audit logs                                 │
     │ RBAC / quotas / resource limits            │
     └────────────────────────────────────────────┘
```

---

# 2. Execution Flow Diagram

**Description:**  
Illustrates how a workflow moves through scheduling, wave execution, worker dispatch, and result collection.

```text
┌─────────────────────────────────────────────────────────────┐
│                  WORKFLOW EXECUTION FLOW                    │
└─────────────────────────────────────────────────────────────┘

        Workflow Request
                │
                ▼
     ┌─────────────────────┐
     │     Task Builder    │
     └─────────┬───────────┘
               │
               ▼
     ┌─────────────────────┐
     │    DAG Scheduler    │
     │---------------------│
     │ Build graph         │
     │ Detect cycles       │
     │ Topological sort    │
     │ Generate waves      │
     └─────────┬───────────┘
               │
               ▼
     Example Output Waves:

       Wave 1: [t1]
       Wave 2: [t2, t3]
       Wave 3: [t4]

               │
               ▼
     ┌─────────────────────┐
     │    Wave Executor    │
     └─────────┬───────────┘
               │
     ┌─────────▼───────────────┐
     │ Execute Wave Sequentially│
     └─────────┬───────────────┘
               │
      ┌────────▼────────┐
      │ Current Wave    │
      │ [t2, t3]        │
      └────────┬────────┘
               │
     ┌─────────▼─────────┐
     │    Worker Pool    │
     │-------------------│
     │ Submit(task)      │
     │ Queue management  │
     │ Backpressure      │
     └──────┬──────┬─────┘
            │      │
            ▼      ▼
     ┌─────────┐ ┌─────────┐
     │ Worker1 │ │ Worker2 │
     └────┬────┘ └────┬────┘
          │           │
          ▼           ▼
      Execute      Execute
        t2            t3
          │           │
          ▼           ▼
     ┌─────────────────────┐
     │ State Transitions   │
     │ running → completed │
     └─────────┬───────────┘
               │
               ▼
     ┌─────────────────────┐
     │ Wait For All Tasks  │
     │ In Wave To Finish   │
     └─────────┬───────────┘
               │
         all terminal?
               │
         ┌─────┴─────┐
         │   yes     │
         ▼           │ no
 Next Wave           │
 Execution           │
                     │
                     ▼
             Continue Waiting
```

---

# 3. Task Lifecycle State Machine

```text
┌───────────────────────────────────────────────────────┐
│                 TASK LIFECYCLE STATE MACHINE          │
└───────────────────────────────────────────────────────┘

                    ┌─────────────┐
                    │   pending   │
                    └──────┬──────┘
                           │
                           │ start execution
                           ▼
                    ┌─────────────┐
                    │   running   │
                    └───┬───┬─────┘
                        │   │
         success        │   │ failure
                        │   │
                        ▼   ▼
               ┌──────────┐ ┌──────────┐
               │completed │ │  failed  │
               └──────────┘ └────┬─────┘
                                  │
                                  │ retry available
                                  ▼
                            ┌─────────────┐
                            │   running   │
                            └─────────────┘

Additional Transition:

     running ───────────────► cancelled
           cancellation request
```

---

# 4. Component Dependency Graph

```text
┌────────────────────────────────────────────────────────────┐
│                 COMPONENT DEPENDENCY GRAPH                 │
└────────────────────────────────────────────────────────────┘

                     ┌────────────────┐
                     │     cmd/       │
                     │ CLI Entrypoint │
                     └───────┬────────┘
                             │
                             ▼
                 ┌─────────────────────┐
                 │      engine/        │
                 │ Workflow Orchestrat │
                 └─────┬─────┬─────────┘
                       │     │
        ┌──────────────┘     └──────────────┐
        ▼                                   ▼
┌────────────────┐                 ┌────────────────┐
│  scheduler/    │                 │   executor/    │
│ DAG Scheduler  │                 │ Wave Executor  │
└──────┬─────────┘                 └──────┬─────────┘
       │                                  │
       ▼                                  ▼
┌────────────────┐               ┌──────────────────┐
│    graph/      │               │   workerpool/    │
│ DAG utilities  │               │ concurrency mgmt │
└────────────────┘               └────────┬─────────┘
                                          │
                                          ▼
                                ┌──────────────────┐
                                │     worker/      │
                                │ task execution   │
                                └────────┬─────────┘
                                         │
                                         ▼
                               ┌────────────────────┐
                               │ docker-runtime/    │
                               │ container adapter  │
                               └────────────────────┘
```

---

# 5. Wave Execution Timeline

```text
┌──────────────────────────────────────────────────────────────┐
│                     WAVE EXECUTION TIMELINE                  │
└──────────────────────────────────────────────────────────────┘

Time ─────────────────────────────────────────────────────────►

          t0        t1        t2        t3        t4

Worker 1  ┌─────────┐
          │   t1    │
          └─────────┘
                                 ┌─────────┐
                                 │   t2    │
                                 └─────────┘

Worker 2
                                 ┌─────────┐
                                 │   t3    │
                                 └─────────┘

Worker 3
                                                       ┌─────────┐
                                                       │   t4    │
                                                       └─────────┘
```

---

# 6. Forge Two-Tier Architecture

```text
┌──────────────────────────────────────────────────────────────────────┐
│                      FORGE TWO-TIER ARCHITECTURE                     │
└──────────────────────────────────────────────────────────────────────┘

                         PUBLIC CONTROL PLANE
                (Hosted SaaS operated by Forge)

┌──────────────────────────────────────────────────────────────────────┐
│                               FORGE CLOUD                            │
│----------------------------------------------------------------------│
│                                                                      │
│  ┌────────────────┐    ┌──────────────────┐                          │
│  │ Agent Registry │    │ Workflow Market  │                          │
│  │ Versioned      │    │ Shared templates │                          │
│  │ agent images   │    │ Community flows  │                          │
│  └──────┬─────────┘    └────────┬─────────┘                          │
│         │                       │                                    │
│         └──────────┬────────────┘                                    │
│                    ▼                                                 │
│         ┌───────────────────────────┐                                │
│         │     Forge Control API     │                                │
│         │ Cluster coordination      │                                │
│         │ AuthN/AuthZ               │                                │
│         │ Deployment management     │                                │
│         │ Fleet orchestration       │                                │
│         └────────────┬──────────────┘                                │
│                      │                                               │
│          ┌───────────▼────────────┐                                  │
│          │ Billing & Metering API │                                  │
│          │ Usage aggregation      │                                  │
│          │ GPU/runtime billing    │                                  │
│          │ Cost analytics         │                                  │
│          └───────────┬────────────┘                                  │
│                      │                                               │
│          ┌───────────▼────────────┐                                  │
│          │ Observability Platform │                                  │
│          │ Traces                 │                                  │
│          │ Logs                   │                                  │
│          │ Metrics                │                                  │
│          │ Audit events           │                                  │
│          └────────────────────────┘                                  │
└──────────────────────────────────────────────────────────────────────┘
```
