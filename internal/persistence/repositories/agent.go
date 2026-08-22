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
	UpsertAgent(context.Context, *models.AgentRow) (*models.AgentRow, error)
	DeleteAgent(context.Context, string) error
}

type PostgresAgentStore struct {
	repo Repository
}

func NewPostgresAgentStore(repo Repository) *PostgresAgentStore {
	return &PostgresAgentStore{repo: repo}
}

const agentColumns = `id, name, agent_image, agent_command, created_at, updated_at`

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

// UpsertAgent registers an agent, or re-points an existing one.
//
// Upsert rather than insert because registering is how an image is rolled
// forward, and making that a delete-then-insert would break the foreign key
// from every task that names the agent — including tasks in workflows that are
// still running.
func (p *PostgresAgentStore) UpsertAgent(ctx context.Context, agent *models.AgentRow) (*models.AgentRow, error) {
	stmt := `INSERT INTO agents (id, name, agent_image, agent_command, created_at, updated_at)
	VALUES (@id, @name, @image, @command, @createdAt, @updatedAt)
	ON CONFLICT (name) DO UPDATE SET
	    agent_image   = EXCLUDED.agent_image,
	    agent_command = EXCLUDED.agent_command,
	    updated_at    = EXCLUDED.updated_at
	RETURNING ` + agentColumns

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{
		"id":        agent.ID,
		"name":      agent.Name,
		"image":     agent.AgentImage,
		"command":   agent.AgentCommand,
		"createdAt": agent.CreatedAt,
		"updatedAt": agent.UpdatedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("upsert agent %s: %w", agent.Name, err)
	}

	saved, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[models.AgentRow])
	if err != nil {
		return nil, fmt.Errorf("collect upserted agent %s: %w", agent.Name, err)
	}
	return saved, nil
}

// DeleteAgent removes an agent.
//
// Fails while any task still references it, by the foreign key on
// tasks.agent_name. That is the intended behaviour: removing an agent out from
// under a workflow that has not finished would leave tasks that can never be
// dispatched.
func (p *PostgresAgentStore) DeleteAgent(ctx context.Context, name string) error {
	tag, err := p.repo.Exec(ctx, `DELETE FROM agents WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete agent %s: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("delete agent %s: %w", name, ErrNotFound)
	}
	return nil
}
