package repositories

import (
	"context"

	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/persistence/domain"
)

type WorkflowStore interface {
	CreateWorkflow(context.Context, *domain.Workflow) (*domain.Workflow, error)
	GetWorkflowByName(context.Context, string) (*domain.Workflow, error)
	GetWorkflowByID(context.Context, uuid.UUID) (*domain.Workflow, error)
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

func (p *PostgresWorkflowStore) CreateWorkflow(context.Context, *domain.Workflow) (*domain.Workflow, error) {
	return nil, nil
}

func (p *PostgresWorkflowStore) GetWorkflowByName(context.Context, string) (*domain.Workflow, error) {
	return nil, nil
}

func (p *PostgresWorkflowStore) GetWorkflowByID(context.Context, uuid.UUID) (*domain.Workflow, error) {
	return nil, nil
}

func (p *PostgresWorkflowStore) DeleteWorkflow(context.Context, uuid.UUID) error {
	return nil
}
