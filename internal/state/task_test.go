package state

// import (
// 	"sync"
// 	"testing"
// 	"time"
// )

// func TestTransition(t *testing.T) {
// 	tests := []struct {
// 		name        string
// 		from        TaskStatus
// 		to          TaskStatus
// 		expectError bool
// 	}{
// 		{"pending to running", PendingTaskStatus, RunningTaskStatus, false},
// 		{"pending to cancelled", PendingTaskStatus, CancelledTaskStatus, false},
// 		{"running to completed", RunningTaskStatus, CompletedTaskStatus, false},
// 		{"completed to running", CompletedTaskStatus, RunningTaskStatus, true},
// 		{"cancelled to running", CancelledTaskStatus, RunningTaskStatus, true},
// 		{"pending to pending", PendingTaskStatus, PendingTaskStatus, false},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			task := &Task{Status: tt.from, CreatedAt: time.Now()}
// 			_, err := task.Transition(tt.to)

// 			if tt.expectError && err == nil {
// 				t.Error("expected error, got nil")
// 			}

// 			if !tt.expectError && err != nil {
// 				t.Errorf("expected no error, got: %s", err)
// 			}
// 		})
// 	}
// }

// func TestTransition_Timestamp(t *testing.T) {
// 	tests := []struct {
// 		name         string
// 		from         TaskStatus
// 		to           TaskStatus
// 		neverStarted bool
// 	}{
// 		{"pending to running", PendingTaskStatus, RunningTaskStatus, false},
// 		{"pending to cancelled", PendingTaskStatus, CancelledTaskStatus, true},
// 		{"running to completed", RunningTaskStatus, CompletedTaskStatus, false},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			task := &Task{Status: tt.from, CreatedAt: time.Now()}
// 			if tt.from == RunningTaskStatus {
// 				p := time.Now()
// 				task.StartedAt = &p
// 			}
// 			_, err := task.Transition(tt.to)

// 			if err == nil && tt.to == RunningTaskStatus && task.StartedAt == nil {
// 				t.Error("expected started at timestamp to be set but got nil")
// 			}

// 			if err == nil && tt.to == CompletedTaskStatus && task.FinishedAt == nil {
// 				t.Error("expected finish at timestamp to be set but got nil")
// 			}

// 			if err == nil && tt.neverStarted && task.StartedAt != nil {
// 				t.Error("expected nil started at")
// 			}
// 		})
// 	}
// }

// func TestTransition_ReturnsPreviousStatus(t *testing.T) {
// 	tests := []struct {
// 		name string
// 		from TaskStatus
// 		to   TaskStatus
// 	}{
// 		{"pending to running", PendingTaskStatus, RunningTaskStatus},
// 		{"pending to cancelled", PendingTaskStatus, CancelledTaskStatus},
// 		{"running to completed", RunningTaskStatus, CompletedTaskStatus},
// 		{"completed to running", CompletedTaskStatus, RunningTaskStatus},
// 		{"cancelled to running", CancelledTaskStatus, RunningTaskStatus},
// 		{"pending to pending", PendingTaskStatus, PendingTaskStatus},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			task := &Task{Status: tt.from, CreatedAt: time.Now()}

// 			oldStatus, err := task.Transition(tt.to)

// 			if err == nil && oldStatus != tt.from {
// 				t.Errorf("expected returned status to be %s, got %s ", string(tt.from), string(oldStatus))
// 			}
// 		})
// 	}
// }

// func TestTransition_Concurrent(t *testing.T) {
// 	const goroutines = 50

// 	task := &Task{Status: PendingTaskStatus, CreatedAt: time.Now()}
// 	var wg sync.WaitGroup
// 	wg.Add(goroutines)

// 	for range goroutines {
// 		go func() {
// 			defer wg.Done()
// 			// try to move to running then complete
// 			_, _ = task.Transition(RunningTaskStatus)
// 			_, _ = task.Transition(CompletedTaskStatus)
// 		}()
// 	}

// 	wg.Wait()

// 	if task.Status != CompletedTaskStatus {
// 		t.Fatalf("expected final status to be %s, got %s", CompletedTaskStatus, task.Status)
// 	}

// }
