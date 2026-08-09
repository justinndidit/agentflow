package engine

// import (
// 	"context"
// 	"strings"
// 	"testing"
// 	"time"

// 	"github.com/justinndidit/agentflow/internal/state"
// )

// func TestWorkerRun_ContextCancelled(t *testing.T) {
// 	w := &worker{id: 1}
// 	task := &state.Task{ID: "t-1", Status: state.PendingTaskStatus, CreatedAt: time.Now()}

// 	ctx, cancel := context.WithCancel(context.Background())
// 	cancel()

// 	res := w.run(ctx, task)

// 	if !strings.Contains(res.Message, "context timeout") {
// 		t.Fatalf("expected context timeout in result, got: %s", res.Message)
// 	}

// 	if task.Status != state.CancelledTaskStatus {
// 		t.Fatalf("expected task status to be cancelled, got %s", task.Status)
// 	}
// }
