// Package logger
package logger

import (
	"fmt"

	"github.com/justinndidit/agentflow/internal/state"
)

func LogTaskTransition(taskID string, from, to state.TaskStatus, workerID *int, reason string) string {
	workerPart := "never started"
	if workerID != nil {
		workerPart = fmt.Sprintf("worker %d", *workerID)
	}
	suffix := ""
	if reason != "" {
		suffix = ", " + reason
	}
	return fmt.Sprintf("[%s] [%s] -> [%s] \t (%s%s)", taskID, from, to, workerPart, suffix)
}
