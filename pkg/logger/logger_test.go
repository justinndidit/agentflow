package logger

import (
	"testing"

	"github.com/justinndidit/agentflow/internal/state"
)

func TestLogTaskTransition_Format(t *testing.T) {
	from := state.PendingTaskStatus
	to := state.RunningTaskStatus
	out := LogTaskTransition("task-1", from, to, nil, "")

	if out == "" {
		t.Fatalf("expected non-empty log output")
	}
}
