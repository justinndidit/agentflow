// Package manifest - handles workflow deployments
package manifest

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	validator "github.com/go-playground/validator/v10"
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

type WorkflowDefinition struct {
	WorkflowName       string            `yaml:"name" validate:"required"`
	WorkflowNameSpace  string            `yaml:"namespace" validate:"required"`
	DefaultWorkerCount int               `yaml:"workers" validate:"required"`
	DefaultTimeout     string            `yaml:"timeout" validate:"required"`
	MaxTokensPerRun    int64             `yaml:"max_tokens" validate:"required"`
	Tasks              []*TaskDefinition `yaml:"tasks" validate:"required"`
	Version            int               `yaml:"workflow_version"`
}

type TaskDefinition struct {
	TaskKey    string         `yaml:"task_key" validate:"required"`
	AgentName  string         `yaml:"agent" validate:"required"`
	Input      map[string]any `yaml:"input"`
	Output     map[string]any `yaml:"output"`
	DependsOn  []string       `yaml:"depends_on"`
	Priority   int8           `yaml:"priority"`
	NotBefore  *time.Time     `yaml:"not_before"`
	MaxRetries int            `yaml:"max_retries"`
	Timeout    *int64         `yaml:"timeout"`
}

func Parse(fileLocation string) (*WorkflowDefinition, []byte, error) {
	validate := validator.New()
	seen := map[string]bool{}

	filePath := fileLocation
	//check that the current passed file location is not absolute before trying to build the directory
	if !filepath.IsAbs(fileLocation) {
		wd, err := os.Getwd()
		if err != nil {
			return nil, nil, err
		}
		filePath = filepath.Join(wd, fileLocation)
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}

	var workflow WorkflowDefinition
	if err = yaml.Unmarshal(data, &workflow); err != nil {
		return nil, data, err
	}

	if err = validate.Struct(workflow); err != nil {
		return nil, data, err
	}

	for _, task := range workflow.Tasks {
		if seen[task.TaskKey] {
			return nil, data, fmt.Errorf("duplicate task id: %s", task.TaskKey)
		}
		seen[task.TaskKey] = true
	}

	for _, task := range workflow.Tasks {
		for _, dependency := range task.DependsOn {
			if !seen[dependency] {
				return nil, data, fmt.Errorf("task %s depends on unknown task with task key %s", task.TaskKey, dependency)
			}
		}
	}

	return &workflow, data, nil
}
