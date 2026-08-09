// Package state
package state

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

type TaskStatus string
type TaskPriority int8

const (
	PendingTaskStatus   TaskStatus = "pending"
	RunningTaskStatus   TaskStatus = "running"
	CompletedTaskStatus TaskStatus = "completed"
	FailedTaskStatus    TaskStatus = "failed"
	CancelledTaskStatus TaskStatus = "cancelled"

	HighTaskPriority   TaskPriority = 4
	MediumTaskPriority TaskPriority = 2
	LowTaskPriority    TaskPriority = 1
)

type Task struct {
	ID            uuid.UUID
	WorkflowID    uuid.UUID
	TaskKey       string
	AgentName     string
	Status        TaskStatus
	DependsOn     []string
	RemainingDeps int
	InputTemplate []byte
	EngineID      *uuid.UUID
	LeaseEpoch    int64
	LeaseExpiry   *time.Time
	Priority      int8
	NotBefore     *time.Time
	Attempt       int
	MaxRetries    int
	Timeout       *pgtype.Interval
	StartedAt     *time.Time
	FinishedAt    *time.Time
	ErrorMessage  *string
}

var validTransitions = map[TaskStatus][]TaskStatus{
	PendingTaskStatus: {RunningTaskStatus, CancelledTaskStatus},
	RunningTaskStatus: {CompletedTaskStatus, FailedTaskStatus, CancelledTaskStatus},
	FailedTaskStatus:  {RunningTaskStatus},
}

func (t *Task) CanTransitionTo(next TaskStatus) bool {
	for _, s := range validTransitions[t.Status] {
		if s == next {
			return true
		}
	}
	return false
}

func (t *Task) IsLeaseExpired(now time.Time) bool {
	return t.LeaseExpiry != nil && now.After(*t.LeaseExpiry)
}

func (t *Task) IsReady() bool {
	return t.RemainingDeps == 0 && t.Status == PendingTaskStatus
}

func (t *Task) CanRetry() bool {
	return t.Attempt < t.MaxRetries
}

// Transition moves the task to next state under the task's own lock.
// Returns the previous status so callers can log the full transition without
// a read outside the lock.
// func (t *Task) Transition(next TaskStatus) (TaskStatus, error) {
// 	// t.mu.Lock()
// 	// defer t.mu.Unlock()

// 	// // No-op check first — before the validity lookup, because terminal states
// 	// // have no entry in validStateChanges and would produce a spurious error.
// 	if t.Status == next {
// 		return t.Status, nil
// 	}

// 	valid, ok := validStateChanges[t.Status]
// 	if !ok {
// 		return t.Status, fmt.Errorf("no transitions allowed from %s", t.Status)
// 	}
// 	if !slices.Contains(valid, next) {
// 		return t.Status, fmt.Errorf("invalid transition: %s -> %s", t.Status, next)
// 	}

// 	// now := time.Now()
// 	old := t.Status
// 	t.Status = next
// 	// t.UpdatedAt = &now

// 	// switch next {
// 	// case RunningTaskStatus:
// 	// 	t.StartedAt = &now
// 	// case CompletedTaskStatus, CancelledTaskStatus, FailedTaskStatus:
// 	// 	t.FinishedAt = &now
// 	// }

// 	return old, nil
// }
