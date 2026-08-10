package models

import (
	"time"

	"github.com/google/uuid"
)

type TaskResult struct {
	TaskID        uuid.UUID `json:"task_id" db:"task_id"`
	Attempt       int       `json:"attempt" db:"attempt"`
	Output        []byte    `json:"output" db:"output"`
	ArtifactURI   *string   `json:"artifact_uri" db:"artifact_uri"`
	ResolvedInput []byte    `json:"resolved_input" db:"resolved_input"`
	TokensUsed    int64     `json:"tokens_used" db:"tokens_used"`
	CostMicros    int64     `json:"cost_micros" db:"cost_micros"`
	DurationMS    int64     `json:"duration_ms" db:"duration_ms"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
}
