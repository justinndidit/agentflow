// Package manifest - handles workflow deployments
package manifest

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	validator "github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/state"
	yaml "go.yaml.in/yaml/v3"
)

type WorkflowStatus string

const (
	PendingWorkflowStatus   WorkflowStatus = "pending"
	RunningWorkflowStatus   WorkflowStatus = "running"
	CompletedWorkflowStatus WorkflowStatus = "completed"
	FailedWorkflowStatus    WorkflowStatus = "failed"
	CancelledWorkflowStatus WorkflowStatus = "cancelled"
)

type Workflow struct {
	WorkflowName       string           `yaml:"name" validate:"required"`
	WorkflowNameSpace  string           `yaml:"namespace" validate:"required"`
	DefaultWorkerCount int              `yaml:"workers" validate:"required"`
	DefaultTimeout     string           `yaml:"timeout" validate:"required"`
	MaxTokensPerRun    int64            `yaml:"max_tokens" validate:"required"`
	Tasks              []TaskDefinition `yaml:"tasks" validate:"required"`

	//internal tracking
	Manifest []byte
	Version  int
	//TODO: create in app primitive
	Status WorkflowStatus
}

type TaskDefinition struct {
	TaskKey   string         `yaml:"task_key" validate:"required"`
	AgentID   string         `yaml:"agent" validate:"required"`
	Input     map[string]any `yaml:"input"`
	DependsOn []string       `yaml:"depends_on"`

	//internal tracking
	ID         *uuid.UUID
	WorkflowID *uuid.UUID
}

func Parse(fileLocation string) (*Workflow, error) {
	validate := validator.New()
	seen := map[string]bool{}

	filePath := fileLocation
	//check that the current passed file location is not absolute before trying to build the directory
	if !filepath.IsAbs(fileLocation) {
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		filePath = filepath.Join(wd, fileLocation)
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var workflow Workflow
	if err = yaml.Unmarshal(data, &workflow); err != nil {
		return nil, err
	}

	if err = validate.Struct(workflow); err != nil {
		return nil, err
	}

	workflow.Manifest = data
	workflow.Version = 0
	workflow.Status = PendingWorkflowStatus

	for _, task := range workflow.Tasks {
		if seen[task.TaskKey] {
			return nil, fmt.Errorf("duplicate task id: %s", task.TaskKey)
		}
		seen[task.TaskKey] = true
	}

	for _, task := range workflow.Tasks {
		for _, dependency := range task.DependsOn {
			if !seen[dependency] {
				return nil, fmt.Errorf("task %s depends on unknown task with task key %s", task.TaskKey, dependency)
			}
		}
	}

	return &workflow, nil
}

func (wf *Workflow) ToTasks() []*state.Task {
	tasks := []*state.Task{}
	for _, taskDefinition := range wf.Tasks {
		task := &state.Task{
			TaskKey:    taskDefinition.TaskKey,
			WorkflowID: wf.WorkflowName, //TODO: update workflow to include ID
			AgentID:    taskDefinition.AgentID,
			DependsOn:  taskDefinition.DependsOn,
			Status:     state.PendingTaskStatus,
			Payload:    taskDefinition.Input,
			CreatedAt:  time.Now(),
		}
		tasks = append(tasks, task)
	}

	return tasks
}
