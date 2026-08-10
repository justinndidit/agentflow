package engine

import (
	"context"

	"github.com/justinndidit/agentflow/internal/manifest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/rs/zerolog"
)

type manifestProcessor struct {
	logger    *zerolog.Logger
	txManager *repositories.TxManager
}

func NewManifestProcessor(
	logger *zerolog.Logger,
	txManager *repositories.TxManager) *manifestProcessor {
	return &manifestProcessor{
		logger:    logger,
		txManager: txManager,
	}
}

func (p *manifestProcessor) SubmitManifest(ctx context.Context, manifestFileLocation string) (*models.WorkflowRow, error) {
	p.logger.Info().Str("func", "SubmitManifest").Msg("submitManifest request")
	workflow, definitionByte, err := manifest.Parse(manifestFileLocation)
	if err != nil {
		return nil, err
	}
	graph, err := NewGraph(workflow.Tasks)
	if err != nil {
		p.logger.Error().Err(err).Str("func", "SubmitManifest").Msg("error building task graph")
		return nil, err
	}

	//detect cycle in task dependence
	_, err = graph.TopologicalSort()
	if err != nil {
		p.logger.Error().Err(err).Str("func", "SubmitManifest").Msg("failed to sort task execution order")
		return nil, err
	}

	wf, err := models.NewWorkflowRowFromDefinition(*workflow, definitionByte)
	if err != nil {
		p.logger.Error().Err(err).Str("func", "SubmitManifest").Msg("failed to create models workflow")
		return nil, err
	}

	newTasks := []*models.TaskRow{}
	for _, t := range workflow.Tasks {
		newTask, err := models.NewTaskFromDefinition(*t, wf.ID)
		if err != nil {
			p.logger.Error().Err(err).Str("func", "SubmitManifest").Msg("failed to create models task")
			return nil, err
		}
		newTasks = append(newTasks, &newTask)
	}

	var workflowDB *models.WorkflowRow
	err = p.txManager.WithTransaction(ctx, func(ctx context.Context, stores *repositories.Stores) error {
		workflowDB, err = stores.WorkflowStore.CreateWorkflow(ctx, &wf)
		if err != nil {
			p.logger.Error().Err(err).Str("func", "SubmitManifest").Msg("failed to create workflow")
			return err
		}

		err = stores.TaskStore.BulkInsertTask(ctx, newTasks)
		if err != nil {
			p.logger.Error().Err(err).Str("func", "SubmitManifest").Msg("failed to bulk insert tasks")
			return err
		}
		return nil
	})

	if err != nil {
		p.logger.Error().Err(err).Str("func", "SubmitManifest").Msg("transaction failed")
		return nil, err
	}

	return workflowDB, nil
}
