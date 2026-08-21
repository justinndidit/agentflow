package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/justinndidit/agentflow/internal/manifest"
	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/pkg/data"
	"go.opentelemetry.io/otel/attribute"
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
	// Valid must be set. A pgtype value with Valid=false encodes as NULL no
	// matter what the other fields hold, so omitting it writes no timeout at all.
	timeout := pgtype.Interval{
		Microseconds: data.ConvertSecondsToMicroSeconds(t.TimeoutInSeconds),
		Valid:        true,
	}

	return TaskRow{
		BaseModel:  NewBaseModel(),
		WorkflowID: workflowID,
		TaskKey:    t.TaskKey,
		AgentName:  t.AgentName,
		Status:     string(state.PendingTaskStatus),
		DependsOn:  t.DependsOn,
		// The scheduler treats remaining_deps == 0 as "ready to run", so it has
		// to start at the number of dependencies this task was declared with.
		RemainingDeps: len(t.DependsOn),
		InputTemplate: input,
		Priority:      t.Priority,
		NotBefore:     t.NotBefore,
		MaxRetries:    int(t.MaxRetries),
		Timeout:       &timeout,
	}, nil
}

// Key identifies this task in a span name. It satisfies the telemetry package's
// SpanSubject interface, which is defined there rather than here so telemetry
// stays independent of persistence.
func (r TaskRow) Key() string { return r.TaskKey }

// Attributes describes this task for tracing.
//
// The engine id is included so a trace shows which node ran an attempt, which
// is the first thing anyone asks when one workflow's tasks behave differently
// from another's.
func (r TaskRow) Attributes() []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("agentflow.workflow_id", r.WorkflowID.String()),
		attribute.String("agentflow.task_id", r.ID.String()),
		attribute.String("agentflow.task_key", r.TaskKey),
		attribute.String("agentflow.agent", r.AgentName),
		attribute.Int("agentflow.attempt", r.Attempt),
		attribute.Int64("agentflow.lease_epoch", r.LeaseEpoch),
	}
	if r.EngineID != nil {
		attrs = append(attrs, attribute.String("agentflow.engine_id", r.EngineID.String()))
	}
	return attrs
}
