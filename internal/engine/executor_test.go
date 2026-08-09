package engine

// import (
// 	"context"
// 	"strings"
// 	"testing"
// 	"time"

// 	"github.com/justinndidit/agentflow/internal/state"
// )

// func TestExecutorRun_WorkflowCompletesInWaves(t *testing.T) {
// 	tasks := []*state.Task{
// 		{ID: "t1", WorkflowID: "wf1", AgentID: "agent-a", Status: state.PendingTaskStatus, DependsOn: []string{}, CreatedAt: time.Now()},
// 		{ID: "t2", WorkflowID: "wf1", AgentID: "agent-b", Status: state.PendingTaskStatus, DependsOn: []string{"t1"}, CreatedAt: time.Now()},
// 		{ID: "t3", WorkflowID: "wf1", AgentID: "agent-c", Status: state.PendingTaskStatus, DependsOn: []string{"t1"}, CreatedAt: time.Now()},
// 		{ID: "t4", WorkflowID: "wf1", AgentID: "agent-d", Status: state.PendingTaskStatus, DependsOn: []string{"t2", "t3"}, CreatedAt: time.Now()},
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	executor := NewExecutor(3, 10)
// 	if err := executor.Run(ctx, tasks); err != nil {
// 		t.Fatalf("unexpected error running workflow: %v", err)
// 	}

// 	for _, task := range tasks {
// 		if task.Status != state.CompletedTaskStatus {
// 			t.Fatalf("expected task %s to be completed, got %s", task.ID, task.Status)
// 		}
// 		if task.StartedAt == nil {
// 			t.Fatalf("expected task %s to have a started timestamp", task.ID)
// 		}
// 		if task.FinishedAt == nil {
// 			t.Fatalf("expected task %s to have a finished timestamp", task.ID)
// 		}
// 	}

// 	if tasks[1].StartedAt.Before(*tasks[0].FinishedAt) {
// 		t.Fatalf("expected t2 to start after t1 finished: t1 finished at %s, t2 started at %s", tasks[0].FinishedAt.Format(time.RFC3339Nano), tasks[1].StartedAt.Format(time.RFC3339Nano))
// 	}
// 	if tasks[2].StartedAt.Before(*tasks[0].FinishedAt) {
// 		t.Fatalf("expected t3 to start after t1 finished: t1 finished at %s, t3 started at %s", tasks[0].FinishedAt.Format(time.RFC3339Nano), tasks[2].StartedAt.Format(time.RFC3339Nano))
// 	}

// 	lastWaveReadyAt := *tasks[1].FinishedAt
// 	if tasks[2].FinishedAt.After(lastWaveReadyAt) {
// 		lastWaveReadyAt = *tasks[2].FinishedAt
// 	}
// 	if tasks[3].StartedAt.Before(lastWaveReadyAt) {
// 		t.Fatalf("expected t4 to start after t2 and t3 finished: t2 finished at %s, t3 finished at %s, t4 started at %s", tasks[1].FinishedAt.Format(time.RFC3339Nano), tasks[2].FinishedAt.Format(time.RFC3339Nano), tasks[3].StartedAt.Format(time.RFC3339Nano))
// 	}
// }

// func TestExecutorRun_ReturnsCycleError(t *testing.T) {
// 	tasks := []*state.Task{
// 		{ID: "t1", WorkflowID: "wf1", AgentID: "agent-a", Status: state.PendingTaskStatus, DependsOn: []string{"t3"}, CreatedAt: time.Now()},
// 		{ID: "t2", WorkflowID: "wf1", AgentID: "agent-b", Status: state.PendingTaskStatus, DependsOn: []string{"t1"}, CreatedAt: time.Now()},
// 		{ID: "t3", WorkflowID: "wf1", AgentID: "agent-c", Status: state.PendingTaskStatus, DependsOn: []string{"t2"}, CreatedAt: time.Now()},
// 	}

// 	executor := NewExecutor(2, 2)
// 	err := executor.Run(context.Background(), tasks)
// 	if err == nil {
// 		t.Fatalf("expected cycle error, got nil")
// 	}
// 	if !strings.Contains(err.Error(), "error sorting order of execution of tasks") {
// 		t.Fatalf("expected sorting error, got %v", err)
// 	}
// }
