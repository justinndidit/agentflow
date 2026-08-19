package database

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// agentSeed is embedded rather than read from disk so the seed travels with the
// binary and does not depend on the working directory.
//
//go:embed seeds/agents.sql
var agentSeed string

// SeedDevAgents inserts the agents that example-workflow.yml refers to.
//
// It has to run after Migrate — the agents table does not exist before then,
// which is why this cannot live in the container's docker-entrypoint-initdb.d
// hook — and before any manifest is submitted, because tasks.agent_name is a
// foreign key to agents(name).
//
// The statement is idempotent (ON CONFLICT (name) DO NOTHING), so re-running it
// against a seeded database is a no-op.
func SeedDevAgents(ctx context.Context, pool *pgxpool.Pool, logger *zerolog.Logger) error {
	logger.Info().Str("func", "SeedDevAgents").Msg("seeding development agents...")

	tag, err := pool.Exec(ctx, agentSeed)
	if err != nil {
		logger.Error().Err(err).Str("func", "SeedDevAgents").Msg("failed to seed development agents")
		return err
	}

	logger.Info().Int64("inserted", tag.RowsAffected()).Str("func", "SeedDevAgents").Msg("development agents seeded")
	return nil
}
