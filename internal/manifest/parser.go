// Package manifest - handles workflow deployments
package manifest

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	validator "github.com/go-playground/validator/v10"
	"github.com/justinndidit/agentflow/internal/state"
	yaml "go.yaml.in/yaml/v3"
)

type Workflow struct {
	WorkflowName       string           `yaml:"name" validate:"required"`
	DefaultWorkerCount int              `yaml:"workers" validate:"required"`
	DefaultTimeout     string           `yaml:"timeout" validate:"required"`
	MaxTokensPerRun    int64            `yaml:"max_tokens" validate:"required"`
	Tasks              []TaskDefinition `yaml:"tasks" validate:"required"`
}

type TaskDefinition struct {
	TaskID    string         `yaml:"id" validate:"required"`
	AgentID   string         `yaml:"agent" validate:"required"`
	Input     map[string]any `yaml:"input"`
	DependsOn []string       `yaml:"depends_on"`
}

// Parse - fileLocation is relative to the working directory
func Parse(fileLocation string) (*Workflow, error) {
	validate := validator.New()
	seen := map[string]bool{}

	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	filePath := filepath.Join(wd, fileLocation)

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

	for _, task := range workflow.Tasks {
		if seen[task.TaskID] {
			return nil, fmt.Errorf("duplicate task id: %s", task.TaskID)
		}
		seen[task.TaskID] = true
	}

	for _, task := range workflow.Tasks {
		for _, dependency := range task.DependsOn {
			if !seen[dependency] {
				return nil, fmt.Errorf("task %s depends on unknown task %s", task.TaskID, dependency)
			}
		}
	}

	return &workflow, nil
}

func (wf *Workflow) ToTasks() []*state.Task {
	tasks := []*state.Task{}
	for _, taskDefinition := range wf.Tasks {
		task := &state.Task{
			ID:         taskDefinition.TaskID,
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
