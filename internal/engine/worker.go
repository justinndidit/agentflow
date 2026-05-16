// Package engine
package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/pkg/logger"
)

type worker struct {
	id int
}

func (w *worker) run(ctx context.Context, t *state.Task) string {
	select {
	case <-time.After(time.Second):
		oldStatus, err := t.Transition(state.CompletedTaskStatus)
		if err != nil {
			return fmt.Sprintf("invalid transition: %s", err)
		}
		return logger.LogTaskTransition(t.ID, oldStatus, t.Status, &w.id, "")
	case <-ctx.Done():
		oldStatus, err := t.Transition(state.CancelledTaskStatus)
		if err != nil {
			return fmt.Sprintf("invalid transition: %s", err)
		}
		return logger.LogTaskTransition(t.ID, oldStatus, t.Status, &w.id, "context timeout")
	}
}
