-- An agent is an image plus, optionally, the command to run inside it.
--
-- The worker contract makes the image the unit of deployment, and most agents
-- will leave this null so their own ENTRYPOINT runs. It exists because one
-- image frequently implements several agents — a single Python worker
-- distinguishing roles by argument is the common shape — and forcing a separate
-- image per agent for that would be a build pipeline's worth of ceremony to
-- express one flag.
ALTER TABLE agents ADD COLUMN IF NOT EXISTS agent_command TEXT[];
