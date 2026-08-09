package repositories

import (
	"context"

	"github.com/justinndidit/agentflow/internal/manifest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

type TaskStore interface {
	BulkInsertTask(context.Context, []*models.TaskRow) error
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

func (p *PostgresTaskStore) BulkInsertTask(ctx context.Context, tasks []*models.TaskRow) error {
	return nil
}

func (p *PostgresTaskStore) UpdateTask(ctx context.Context, task *manifest.TaskDefinition) error {
	return nil
}
