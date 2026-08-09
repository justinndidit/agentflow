package models

import (
	"time"

	"github.com/justinndidit/agentflow/internal/manifest"
	"github.com/justinndidit/agentflow/internal/state"
)

type WorkflowRow struct {
	BaseModel
	WorkflowName      string        `db:"name"`
	WorkflowNameSpace string        `db:"namespace"`
	Manifest          []byte        `db:"manifest"`
	Version           int           `db:"version"`
	Status            string        `db:"status"`
	TaskCount         int           `db:"task_total"`
	TaskCompleted     int           `db:"task_completed"`
	TaskFailed        int           `db:"task_failed"`
	TaskCancelled     int           `db:"task_cancelled"`
	MaxParallelism    int           `db:"max_parallelism"` //worker count
	RunningCount      int           `db:"running_count"`
	MaxTokensPerRun   int64         `db:"max_tokens"`
	DefaultTimeout    time.Duration `db:"default_timeout"`
}

func (w WorkflowRow) ToDomain() (state.Workflow, error) {
	return state.Workflow{
		ID:                w.ID,
		WorkflowName:      w.WorkflowName,
		WorkflowNameSpace: w.WorkflowNameSpace,
		Manifest:          w.Manifest,
		Version:           w.Version,
		Status:            state.WorkflowStatus(w.Status),
		TaskCount:         w.TaskCount,
		TaskCompleted:     w.TaskCompleted,
		TaskFailed:        w.TaskFailed,
		TaskCancelled:     w.TaskCancelled,
		MaxParallelism:    w.MaxParallelism,
		RunningCount:      w.RunningCount,
		MaxTokensPerRun:   w.MaxTokensPerRun,
		DefaultTimeout:    w.DefaultTimeout,
	}, nil
}

func WorkflowRowFromDomain(w state.Workflow) WorkflowRow {
	return WorkflowRow{
		BaseModel:         NewBaseModel(),
		WorkflowName:      w.WorkflowName,
		WorkflowNameSpace: w.WorkflowNameSpace,
		Manifest:          w.Manifest,
		Version:           w.Version,
		Status:            string(w.Status),
		TaskCount:         w.TaskCount,
		TaskCompleted:     w.TaskCompleted,
		TaskFailed:        w.TaskFailed,
		TaskCancelled:     w.TaskCancelled,
		MaxParallelism:    w.MaxParallelism,
		RunningCount:      w.RunningCount,
		MaxTokensPerRun:   w.MaxTokensPerRun,
		DefaultTimeout:    w.DefaultTimeout,
	}
}

func NewWorkflowRowFromDefinition(w manifest.WorkflowDefinition, data []byte) (WorkflowRow, error) {
	timeout, err := time.ParseDuration(w.DefaultTimeout)
	if err != nil {
		return WorkflowRow{}, err
	}
	return WorkflowRow{
		BaseModel:         NewBaseModel(),
		WorkflowName:      w.WorkflowName,
		WorkflowNameSpace: w.WorkflowNameSpace,
		Manifest:          data,
		Version:           w.Version,
		Status:            string(state.PendingWorkflowStatus),
		TaskCount:         int(len(w.Tasks)),
		MaxParallelism:    w.DefaultWorkerCount,
		MaxTokensPerRun:   w.MaxTokensPerRun,
		DefaultTimeout:    timeout,
	}, nil
}
