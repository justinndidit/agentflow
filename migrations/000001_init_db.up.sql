DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_status') THEN
        CREATE TYPE task_status AS ENUM (
            'pending', 'running', 'completed', 'failed', 'cancelled'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'engine_status') THEN
        CREATE TYPE engine_status AS ENUM (
            'active', 'draining', 'stopped'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'workflow_status') THEN
        CREATE TYPE workflow_status AS ENUM (
            'pending', 'running', 'completed', 'failed', 'cancelled'
        );
    END IF;


END$$;

CREATE TABLE IF NOT EXISTS agents (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    agent_image TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS engines (
    id UUID PRIMARY KEY,
    hostname TEXT NOT NULL,
    status engine_status NOT NULL DEFAULT 'active',
    capacity INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS workflows (
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

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    task_key VARCHAR(256) NOT NULL,
    agent_name TEXT NOT NULL REFERENCES agents(name),
    status task_status NOT NULL DEFAULT 'pending',

    depends_on TEXT[] NOT NULL DEFAULT '{}', -- use task key instead o id both are unique
    remaining_deps INTEGER NOT NULL,

    input_template JSONB NOT NULL,

    engine_id UUID REFERENCES engines(id),
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,

    priority SMALLINT NOT NULL DEFAULT 0,
    not_before TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 0,
    timeout INTERVAL,

    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    UNIQUE (workflow_id, task_key),
    CHECK(attempt <= max_retries + 1),
    CHECK(remaining_deps >= 0)
);


CREATE TABLE IF NOT EXISTS task_results (
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

CREATE INDEX IF NOT EXISTS idx_tasks_ready ON tasks (priority DESC, created_at)
    WHERE status = 'pending' AND remaining_deps = 0;

CREATE INDEX IF NOT EXISTS idx_tasks_depends_on ON tasks USING GIN (depends_on);

CREATE INDEX IF NOT EXISTS idx_tasks_leases ON tasks (lease_expires_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_tasks_scheduling ON tasks (workflow_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_agent_name ON tasks (agent_name);

CREATE INDEX IF NOT EXISTS idx_workflows_name_namespace ON workflows (name, namespace);
CREATE INDEX IF NOT EXISTS idx_engines_liveness ON engines (heartbeat_at) WHERE status = 'active';