package state

import (
	"time"

	"github.com/google/uuid"
)

type EngineStatus string

const (
	// ActiveEngineStatus is a node claiming and running work.
	ActiveEngineStatus EngineStatus = "active"

	// DrainingEngineStatus is a node shutting down gracefully: it has stopped
	// claiming but is still finishing what it holds. The reaper leaves its
	// tasks alone as long as it keeps heartbeating.
	DrainingEngineStatus EngineStatus = "draining"

	// StoppedEngineStatus is a node that shut down cleanly. Anything it still
	// held is reclaimable immediately rather than after a lease timeout.
	StoppedEngineStatus EngineStatus = "stopped"
)

// Engine is a node that claims and executes tasks. Liveness is tracked here
// rather than on individual tasks: a per-task heartbeat costs one write per
// running task per interval on the hottest table in the system, while a
// per-engine heartbeat costs one write per node. Task liveness is derived by
// joining to the owning engine.
type Engine struct {
	ID          uuid.UUID
	HostName    string
	Status      EngineStatus
	Capacity    int
	StartedAt   time.Time
	HeartBeatAt time.Time
}

// IsClaiming reports whether this node should still be taking new work. A
// draining node finishes what it holds but claims nothing further.
func (e *Engine) IsClaiming() bool {
	return e.Status == ActiveEngineStatus
}

// IsStale reports whether this engine has missed heartbeats for longer than
// ttl, which is what the reaper uses to decide a node is gone.
//
// now is supplied rather than read from the clock so callers pass the database's
// time. Node clocks drift relative to each other, and a node whose clock runs
// fast would otherwise declare its healthy peers dead.
func (e *Engine) IsStale(now time.Time, ttl time.Duration) bool {
	return now.Sub(e.HeartBeatAt) > ttl
}
