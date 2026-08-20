package repositories

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

type AgentStore interface {
	GetByName(context.Context, string) (*models.AgentRow, error)
	ListAgents(context.Context) ([]*models.AgentRow, error)
}

type PostgresAgentStore struct {
	repo Repository
}

func NewPostgresAgentStore(repo Repository) *PostgresAgentStore {
	return &PostgresAgentStore{repo: repo}
}

const agentColumns = `id, name, agent_image, created_at, updated_at`

// GetByName resolves the image a task's agent_name refers to.
//
// tasks.agent_name is a foreign key to agents(name), so a task that reached
// dispatch always has a row here — a miss means the agent was deleted between
// submit and dispatch, which is worth reporting rather than defaulting.
func (p *PostgresAgentStore) GetByName(ctx context.Context, name string) (*models.AgentRow, error) {
	rows, err := p.repo.Query(ctx,
		`SELECT `+agentColumns+` FROM agents WHERE name = @name`,
		pgx.NamedArgs{"name": name})
	if err != nil {
		return nil, fmt.Errorf("get agent %s: %w", name, err)
	}

	agent, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[models.AgentRow])
	if err != nil {
		if isNoRows(err) {
			return nil, fmt.Errorf("get agent %s: %w", name, ErrNotFound)
		}
		return nil, fmt.Errorf("collect agent %s: %w", name, err)
	}
	return agent, nil
}

func (p *PostgresAgentStore) ListAgents(ctx context.Context) ([]*models.AgentRow, error) {
	rows, err := p.repo.Query(ctx, `SELECT `+agentColumns+` FROM agents ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}

	agents, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[models.AgentRow])
	if err != nil {
		return nil, fmt.Errorf("collect agents: %w", err)
	}
	return agents, nil
}
