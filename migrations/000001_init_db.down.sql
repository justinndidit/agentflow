CREATE TYPE task_status AS ENUM (
  'pending', 'running', 'completed', 'failed', 'cancelled'
);

CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    namespace VARCHAR(256) NOT NULL,
    manifest BYTEA NOT NULL,
    version INTEGER NOT NULL,
    status task_status NOT NULL DEFAULT 'pending',
    worker_count INTEGER NOT NULL,
    default_timeout TEXT NOT NULL,
    max_tokens BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE agents (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    agent_image TEXT NOT NULL
);

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    task_key VARCHAR(256) NOT NULL,
    agent_id UUID NOT NULL REFERENCES agents(id),
    status task_status NOT NULL DEFAULT 'pending',
    depends_on UUID[] NOT NULL DEFAULT '{}',
    remaining_deps INTEGER NOT NULL,
    input_payload BYTEA NOT NULL,
    output_payload BYTEA,
    engine_id TEXT,
    engine_heartbeat TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    max_retries INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (workflow_id, task_key)
    CHECK(retry_count <= max_retries)
);

CREATE INDEX idx_tasks_scheduling ON tasks (workflow_id, status, remaining_deps);
CREATE INDEX idx_tasks_agent_id ON tasks (agent_id);
CREATE INDEX idx_workflows_name_namespace ON workflows (name, namespace);