// Package state
package state

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

type TaskStatus string

const (
	PendingTaskStatus   TaskStatus = "pending"
	RunningTaskStatus   TaskStatus = "running"
	CompletedTaskStatus TaskStatus = "completed"
	FailedTaskStatus    TaskStatus = "failed"
	CancelledTaskStatus TaskStatus = "cancelled"
)

type Task struct {
	ID         string
	WorkflowID string
	AgentID    string
	DependsOn  []string
	Status     TaskStatus
	Payload    map[string]any
	Result     map[string]any
	Error      string
	Retries    int
	MaxRetries int
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
	UpdatedAt  *time.Time

	mu sync.Mutex
}

//var mu sync.Mutex

var validStateChanges = map[TaskStatus][]TaskStatus{
	PendingTaskStatus: {RunningTaskStatus, CancelledTaskStatus},
	RunningTaskStatus: {CompletedTaskStatus, FailedTaskStatus, CancelledTaskStatus},
	FailedTaskStatus:  {RunningTaskStatus},
}

// Transition moves the task to next state under the task's own lock.
// Returns the previous status so callers can log the full transition without
// a read outside the lock.
func (t *Task) Transition(next TaskStatus) (TaskStatus, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// No-op check first — before the validity lookup, because terminal states
	// have no entry in validStateChanges and would produce a spurious error.
	if t.Status == next {
		return t.Status, nil
	}

	valid, ok := validStateChanges[t.Status]
	if !ok {
		return t.Status, fmt.Errorf("no transitions allowed from %s", t.Status)
	}
	if !slices.Contains(valid, next) {
		return t.Status, fmt.Errorf("invalid transition: %s -> %s", t.Status, next)
	}

	now := time.Now()
	old := t.Status
	t.Status = next
	t.UpdatedAt = &now

	switch next {
	case RunningTaskStatus:
		t.StartedAt = &now
	case CompletedTaskStatus, CancelledTaskStatus, FailedTaskStatus:
		t.FinishedAt = &now
	}

	return old, nil
}
