package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/manifest"
	"github.com/justinndidit/agentflow/internal/state"
)

type BaseModel struct {
	ID        uuid.UUID `json:"id" db:"id"`
	CreatedAt time.Time `json:"-" db:"created_at"`
	UpdatedAt time.Time `json:"-" db:"updated_at"`
}

func NewBaseModel() BaseModel {
	now := time.Now().UTC()

	return BaseModel{
		ID:        uuid.New(),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func NewTask(workflowID uuid.UUID, task *state.Task) (*Task, error) {
	baseModel := NewBaseModel()
	outputData, err := marshalData(task.Result)
	if err != nil {
		return nil, err
	}
	inputPayload, err := marshalData(task.Payload)
	if err != nil {
		return nil, err
	}
	dependsOn := []uuid.UUID{}

	for _, id := range task.DependsOn {
		dependsOn = append(dependsOn, uuid.MustParse(id))
	}

	return &Task{
		BaseModel:     baseModel,
		WorkflowID:    workflowID,
		TaskKey:       task.TaskKey,
		AgentID:       uuid.MustParse(task.AgentID),
		Status:        string(task.Status),
		DependsOn:     dependsOn,
		RemainingDeps: task.RemainingDeps,
		InputPayload:  inputPayload,
		OutputPayload: outputData,
		MaxRetries:    task.MaxRetries,
	}, nil
}

func NewWorkflow(workflow *manifest.Workflow) (*Workflow, error) {
	baseModel := NewBaseModel()
	return &Workflow{
		BaseModel:          baseModel,
		WorkflowName:       workflow.WorkflowName,
		WorkflowNameSpace:  workflow.WorkflowNameSpace,
		Manifest:           workflow.Manifest,
		Version:            workflow.Version,
		Status:             string(workflow.Status),
		DefaultWorkerCount: workflow.DefaultWorkerCount,
		DefaultTimeout:     workflow.DefaultTimeout,
		MaxTokensPerRun:    workflow.MaxTokensPerRun,
	}, nil
}

func marshalData(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
