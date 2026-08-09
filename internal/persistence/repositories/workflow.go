package repositories

import (
	"context"

	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

type WorkflowStore interface {
	CreateWorkflow(context.Context, *models.WorkflowRow) (*models.WorkflowRow, error)
	GetWorkflowByName(context.Context, string) (*models.WorkflowRow, error)
	GetWorkflowByID(context.Context, uuid.UUID) (*models.WorkflowRow, error)
	DeleteWorkflow(context.Context, uuid.UUID) error
}

type PostgresWorkflowStore struct {
	repo Repository
}

func NewPostgresWorkflowStore(repo Repository) *PostgresWorkflowStore {
	return &PostgresWorkflowStore{
		repo: repo,
	}
}

func (p *PostgresWorkflowStore) CreateWorkflow(context.Context, *models.WorkflowRow) (*models.WorkflowRow, error) {
	return nil, nil
}

func (p *PostgresWorkflowStore) GetWorkflowByName(context.Context, string) (*models.WorkflowRow, error) {
	return nil, nil
}

func (p *PostgresWorkflowStore) GetWorkflowByID(context.Context, uuid.UUID) (*models.WorkflowRow, error) {
	return nil, nil
}

func (p *PostgresWorkflowStore) DeleteWorkflow(context.Context, uuid.UUID) error {
	return nil
}
