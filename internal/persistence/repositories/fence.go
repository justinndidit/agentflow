package repositories

import (
	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/persistence/models"
)

// Fence identifies both a task and the specific lease under which a caller is
// entitled to write to it.
//
// It exists as a type so no scheduling write can forget the guard. Passing a
// bare task id to a completion would compile and would silently reintroduce the
// race the lease_epoch column exists to close:
//
//	t0  Engine A claims task T and begins work.
//	t1  Engine A stalls — GC pause, network partition, frozen VM.
//	t2  The lease expires; the reaper returns T to pending.
//	t3  Engine B claims T, runs it, and records its result.
//	t4  Engine A wakes and writes its own result over B's.
//
// Every write carries the epoch it was issued under, so at t4 engine A's UPDATE
// matches zero rows and is rejected as ErrFenced.
type Fence struct {
	TaskID     uuid.UUID
	EngineID   uuid.UUID
	LeaseEpoch int64
}

// FenceFor builds the fence for a task this node has just claimed. Taking it
// from the claimed row rather than from a caller's own bookkeeping means the
// epoch is always the one the database issued.
func FenceFor(task *models.TaskRow) Fence {
	fence := Fence{
		TaskID:     task.ID,
		LeaseEpoch: task.LeaseEpoch,
	}
	if task.EngineID != nil {
		fence.EngineID = *task.EngineID
	}
	return fence
}
