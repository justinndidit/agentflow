package dtos

import (
	"time"

	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/state"
)

type TaskResponse struct {
	ID           uuid.UUID  `json:"id"`
	WorkflowID   uuid.UUID  `json:"workflow_id"`
	TaskKey      string     `json:"task_key"`
	Status       string     `json:"status"`
	DependsOn    []string   `json:"depends_on"`
	Priority     int8       `json:"priority"`
	Attempt      int        `json:"attempt"`
	MaxRetries   int        `json:"max_retries"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	ErrorMessage *string    `json:"error_message,omitempty"`
}

func NewTaskResponse(t *state.Task) TaskResponse {
	return TaskResponse{
		ID:           t.ID,
		WorkflowID:   t.WorkflowID,
		TaskKey:      t.TaskKey,
		Status:       string(t.Status),
		DependsOn:    t.DependsOn,
		Priority:     t.Priority,
		Attempt:      t.Attempt,
		MaxRetries:   t.MaxRetries,
		StartedAt:    t.StartedAt,
		FinishedAt:   t.FinishedAt,
		ErrorMessage: t.ErrorMessage,
	}
}
