package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/state"
)

type EngineRow struct {
	BaseModel

	HostName    string    `db:"hostname"`
	Status      string    `db:"status"`
	Capacity    int       `db:"capacity"`
	StartedAt   time.Time `db:"started_at"`
	HeartBeatAt time.Time `db:"heartbeat_at"`
}

func (r EngineRow) ToDomain() state.Engine {
	return state.Engine{
		ID:          r.ID,
		HostName:    r.HostName,
		Status:      state.EngineStatus(r.Status),
		Capacity:    r.Capacity,
		StartedAt:   r.StartedAt,
		HeartBeatAt: r.HeartBeatAt,
	}
}

// NewEngineRow builds the row a node inserts when it boots. started_at and
// heartbeat_at are seeded to the same instant so a node that dies before its
// first heartbeat still ages out of liveness normally rather than looking like
// it never existed.
func NewEngineRow(hostname string, capacity int) EngineRow {
	base := NewBaseModel()

	return EngineRow{
		BaseModel:   base,
		HostName:    hostname,
		Status:      string(state.ActiveEngineStatus),
		Capacity:    capacity,
		StartedAt:   base.CreatedAt,
		HeartBeatAt: base.CreatedAt,
	}
}

// EngineID is a convenience for the many places that need only the identifier.
func (r EngineRow) EngineID() uuid.UUID { return r.ID }
