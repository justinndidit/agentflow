-- agents.sql
-- Development seed data: the agents referenced by example-workflow.yml.
--
-- tasks.agent_name is a foreign key to agents(name), so a manifest cannot be
-- submitted until the agents it names exist. This is a stand-in for a real agent
-- registry — the images below are placeholders and are not published anywhere.
--
-- Embedded into the binary and applied by SeedDevAgents after migrations have
-- run. It cannot be applied by the container's docker-entrypoint-initdb.d hook,
-- because the agents table does not exist until the application migrates.
--
-- Idempotent: safe to run repeatedly.
--
--   go run ./cmd/agentflow -seed
INSERT INTO agents (id, name, agent_image) VALUES
    (gen_random_uuid(), 'research-agent',      'agentflow/research-agent:example'),
    (gen_random_uuid(), 'scraper-agent',       'agentflow/scraper-agent:example'),
    (gen_random_uuid(), 'data-agent',          'agentflow/data-agent:example'),
    (gen_random_uuid(), 'intelligence-agent',  'agentflow/intelligence-agent:example'),
    (gen_random_uuid(), 'resume-agent',        'agentflow/resume-agent:example'),
    (gen_random_uuid(), 'github-agent',        'agentflow/github-agent:example'),
    (gen_random_uuid(), 'career-agent',        'agentflow/career-agent:example'),
    (gen_random_uuid(), 'matching-agent',      'agentflow/matching-agent:example'),
    (gen_random_uuid(), 'ranking-agent',       'agentflow/ranking-agent:example'),
    (gen_random_uuid(), 'writer-agent',        'agentflow/writer-agent:example'),
    (gen_random_uuid(), 'networking-agent',    'agentflow/networking-agent:example'),
    (gen_random_uuid(), 'interview-agent',     'agentflow/interview-agent:example'),
    (gen_random_uuid(), 'mentor-agent',        'agentflow/mentor-agent:example'),
    (gen_random_uuid(), 'automation-agent',    'agentflow/automation-agent:example'),
    (gen_random_uuid(), 'tracking-agent',      'agentflow/tracking-agent:example'),
    (gen_random_uuid(), 'analytics-agent',     'agentflow/analytics-agent:example'),
    (gen_random_uuid(), 'notification-agent',  'agentflow/notification-agent:example')
ON CONFLICT (name) DO NOTHING;
