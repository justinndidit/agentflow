package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/justinndidit/agentflow/internal/manifest"
	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/pkg/data"
)

type TaskRow struct {
	BaseModel

	WorkflowID    uuid.UUID        `db:"workflow_id"`
	TaskKey       string           `db:"task_key"`
	AgentName     string           `db:"agent_name"`
	Status        string           `db:"status"`
	DependsOn     []string         `db:"depends_on"` //task_key
	RemainingDeps int              `db:"remaining_deps"`
	InputTemplate []byte           `db:"input_template"`
	EngineID      *uuid.UUID       `db:"engine_id"`
	LeaseEpoch    int64            `db:"lease_epoch"`
	LeaseExpiry   *time.Time       `db:"lease_expires_at"`
	Priority      int8             `db:"priority"`
	NotBefore     *time.Time       `db:"not_before"`
	Attempt       int              `db:"attempt"`
	MaxRetries    int              `db:"max_retries"`
	Timeout       *pgtype.Interval `db:"timeout"`
	StartedAt     *time.Time       `db:"started_at"`
	FinishedAt    *time.Time       `db:"finished_at"`
	ErrorMessage  *string          `db:"error_message"`
}

func (r TaskRow) ToDomain() (state.Task, error) {
	return state.Task{
		ID:            r.ID,
		WorkflowID:    r.WorkflowID,
		TaskKey:       r.TaskKey,
		AgentName:     r.AgentName,
		Status:        state.TaskStatus(r.Status),
		DependsOn:     r.DependsOn,
		RemainingDeps: r.RemainingDeps,
		InputTemplate: r.InputTemplate,
		EngineID:      r.EngineID,
		LeaseEpoch:    r.LeaseEpoch,
		LeaseExpiry:   r.LeaseExpiry,
		Priority:      r.Priority,
		NotBefore:     r.NotBefore,
		Attempt:       r.Attempt,
		MaxRetries:    r.MaxRetries,
		Timeout:       r.Timeout,
		StartedAt:     r.StartedAt,
		FinishedAt:    r.FinishedAt,
		ErrorMessage:  r.ErrorMessage,
	}, nil
}

func FromDomain(t state.Task) TaskRow {
	return TaskRow{
		BaseModel:     NewBaseModel(),
		WorkflowID:    t.WorkflowID,
		TaskKey:       t.TaskKey,
		AgentName:     t.AgentName,
		Status:        string(t.Status),
		DependsOn:     t.DependsOn,
		RemainingDeps: t.RemainingDeps,
		InputTemplate: t.InputTemplate,
		EngineID:      t.EngineID,
		LeaseEpoch:    t.LeaseEpoch,
		LeaseExpiry:   t.LeaseExpiry,
		Priority:      t.Priority,
		NotBefore:     t.NotBefore,
		Attempt:       t.Attempt,
		MaxRetries:    t.MaxRetries,
		Timeout:       t.Timeout,
		StartedAt:     t.StartedAt,
		FinishedAt:    t.FinishedAt,
		ErrorMessage:  t.ErrorMessage,
	}
}

func NewTaskFromDefinition(t manifest.TaskDefinition, workflowID uuid.UUID) (TaskRow, error) {
	input, err := data.MarshalData(t.Input)
	if err != nil {
		return TaskRow{}, err
	}
	timeout := pgtype.Interval{
		Microseconds: data.ConvertSecondsToMicroSeconds(t.TimeoutInSeconds),
	}

	return TaskRow{
		BaseModel:     NewBaseModel(),
		WorkflowID:    workflowID,
		TaskKey:       t.TaskKey,
		AgentName:     t.AgentName,
		Status:        string(state.PendingTaskStatus),
		DependsOn:     t.DependsOn,
		InputTemplate: input,
		Priority:      t.Priority,
		NotBefore:     t.NotBefore,
		MaxRetries:    int(t.MaxRetries),
		Timeout:       &timeout,
	}, nil
}
