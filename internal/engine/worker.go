// Package engine
package engine

// import (
// 	"context"
// 	"fmt"
// 	"time"

// 	"github.com/justinndidit/agentflow/internal/state"
// 	"github.com/justinndidit/agentflow/pkg/logger"
// )

// type worker struct {
// 	id int
// }

// type TaskResult struct {
// 	Task    *state.Task
// 	Message string
// 	Status  state.TaskStatus
// 	Err     error
// }

// func newTaskResult(t *state.Task, status state.TaskStatus, msg string, err error) TaskResult {
// 	return TaskResult{
// 		Task:    t,
// 		Status:  status,
// 		Message: msg,
// 		Err:     err,
// 	}
// }

// func (w *worker) run(ctx context.Context, t *state.Task) TaskResult {
// 	select {
// 	case <-time.After(time.Second * 3):
// 		oldStatus, err := t.Transition(state.CompletedTaskStatus)
// 		if err != nil {
// 			msg := fmt.Sprintf("invalid transition: %s", err)
// 			return newTaskResult(t, state.FailedTaskStatus, msg, err)
// 		}
// 		msg := logger.LogTaskTransition(t.ID, oldStatus, state.CompletedTaskStatus, &w.id, "")
// 		return newTaskResult(t, state.CompletedTaskStatus, msg, nil)
// 	case <-ctx.Done():
// 		oldStatus, err := t.Transition(state.CancelledTaskStatus)
// 		if err != nil {
// 			msg := fmt.Sprintf("invalid transition: %s", err)
// 			return newTaskResult(t, state.FailedTaskStatus, msg, err)
// 		}
// 		msg := logger.LogTaskTransition(t.ID, oldStatus, state.CancelledTaskStatus, &w.id, "context timeout")
// 		return newTaskResult(t, state.CancelledTaskStatus, msg, nil)
// 	}
// }
