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
	Tasks              []*TaskDefinition `yaml:"tasks" validate:"required,dive"`
	Version            int               `yaml:"workflow_version"`
}

type TaskDefinition struct {
	TaskKey   string         `yaml:"task_key" validate:"required"`
	AgentName string         `yaml:"agent" validate:"required"`
	Input     map[string]any `yaml:"input"`
	// Output           map[string]any `yaml:"output"`
	DependsOn        []string   `yaml:"depends_on"`
	Priority         int8       `yaml:"priority"`
	NotBefore        *time.Time `yaml:"not_before"`
	MaxRetries       int        `yaml:"max_retries" validate:"gte=0"`
	TimeoutInSeconds int64      `yaml:"timeout" validate:"required"`
}

// Parse reads a manifest from disk and validates it. The raw bytes are
// returned alongside the decoded workflow because the submit path stores the
// original definition verbatim.
func Parse(fileLocation string) (*WorkflowDefinition, []byte, error) {
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

	workflow, err := ParseBytes(data)
	if err != nil {
		// data is returned even on failure so a caller can report the input it
		// rejected.
		return nil, data, err
	}

	return workflow, data, nil
}

// ParseBytes decodes and validates a manifest that has already been read.
//
// Every rule that makes a manifest well formed lives here rather than in Parse,
// so validation is exercisable without touching the filesystem. The rules run in
// widening order: schema, then task keys, then edges between them, because each
// later check assumes the earlier one held.
func ParseBytes(data []byte) (*WorkflowDefinition, error) {
	var workflow WorkflowDefinition
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		return nil, err
	}

	if err := validator.New().Struct(workflow); err != nil {
		return nil, err
	}

	// seen doubles as the set of known task keys for the two checks below.
	seen := map[string]bool{}
	for _, task := range workflow.Tasks {
		if seen[task.TaskKey] {
			return nil, fmt.Errorf("duplicate task id: %s", task.TaskKey)
		}
		seen[task.TaskKey] = true
	}

	// Deferred to its own loop: a task may legally depend on one declared later
	// in the file, so every key has to be collected before any edge is resolved.
	for _, task := range workflow.Tasks {
		for _, dependency := range task.DependsOn {
			if !seen[dependency] {
				return nil, fmt.Errorf("task %s depends on unknown task with task key %s", task.TaskKey, dependency)
			}
		}
	}

	if err := validateTemplateReferences(workflow.Tasks, seen); err != nil {
		return nil, err
	}

	return &workflow, nil
}
