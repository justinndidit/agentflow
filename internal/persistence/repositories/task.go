package repositories

import (
	"context"

	"github.com/justinndidit/agentflow/internal/manifest"
	"github.com/justinndidit/agentflow/internal/persistence/domain"
)

type TaskStore interface {
	BulkInsertTask(context.Context, []*domain.Task) error
	UpdateTask(context.Context, *manifest.TaskDefinition) error
}

type PostgresTaskStore struct {
	repo Repository
}

func NewPostgresTaskStore(repo Repository) *PostgresTaskStore {
	return &PostgresTaskStore{
		repo: repo,
	}
}

func (p *PostgresTaskStore) BulkInsertTask(ctx context.Context, tasks []*domain.Task) error {
	return nil
}

func (p *PostgresTaskStore) UpdateTask(ctx context.Context, task *manifest.TaskDefinition) error {
	return nil
}
