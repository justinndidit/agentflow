package domain

import (
	"time"

	"github.com/google/uuid"
)

type Task struct {
	BaseModel
	WorkflowID      uuid.UUID   `json:"workflow_id" db:"workflow_id"`
	TaskKey         string      `json:"task_key" db:"task_key"`
	AgentID         uuid.UUID   `json:"agent_id" db:"agent_id"`
	Status          string      `json:"status" db:"status"`
	DependsOn       []uuid.UUID `json:"depends_on" db:"depends_on"`
	RemainingDeps   int         `json:"remaining_deps" db:"remaining_deps"`
	InputPayload    []byte      `json:"input_payload" db:"input_payload"`
	OutputPayload   []byte      `json:"output_payload" db:"output_payload"`
	EngineID        *string     `json:"engine_id,omitempty" db:"engine_id"`
	EngineHeartBeat *time.Time  `json:"-" db:"engine_heartbeat"`
	Lease_Epoch     int         `json:"-" db:"lease_epoch"`
	StartedAt       *time.Time  `json:"started_at" db:"started_at"`
	FinishedAt      *time.Time  `json:"finished_at" db:"finished_at"`
	ErrorMessage    *string     `json:"error_message,omitempty" db:"error_message"`
	MaxRetries      int         `json:"max_retries" db:"max_retries"`
	RetryCount      int         `json:"retry_count" db:"retry_count"`
}
