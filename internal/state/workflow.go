package state

import (
	"time"

	"github.com/google/uuid"
)

type WorkflowStatus string

const (
	PendingWorkflowStatus   WorkflowStatus = "pending"
	CancelledWorkflowStatus WorkflowStatus = "cancelled"
	RunningWorkflowStatus   WorkflowStatus = "running"
	FailedWorkflowStatus    WorkflowStatus = "failed"
	CompletedWorkflowStatus WorkflowStatus = "completed"
)

type Workflow struct {
	ID                uuid.UUID
	WorkflowName      string
	WorkflowNameSpace string
	Manifest          []byte
	Version           int
	Status            WorkflowStatus
	TaskCount         int
	TaskCompleted     int
	TaskFailed        int
	TaskCancelled     int
	MaxParallelism    int //worker count
	RunningCount      int
	MaxTokensPerRun   int64
	DefaultTimeout    time.Duration
}
