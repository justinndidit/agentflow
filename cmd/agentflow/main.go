package main

import (
	"context"
	"fmt"
	"time"

	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/state"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	// workflow: t1 runs first
	// t2 and t3 run in parallel after t1
	// t4 runs after t2 and t3 both complete
	tasks := []*state.Task{
		{ID: "t1", WorkflowID: "wf1", AgentID: "agent-a", Status: state.PendingTaskStatus, DependsOn: []string{}, CreatedAt: time.Now()},
		{ID: "t2", WorkflowID: "wf1", AgentID: "agent-b", Status: state.PendingTaskStatus, DependsOn: []string{"t1"}, CreatedAt: time.Now()},
		{ID: "t3", WorkflowID: "wf1", AgentID: "agent-c", Status: state.PendingTaskStatus, DependsOn: []string{"t1"}, CreatedAt: time.Now()},
		{ID: "t4", WorkflowID: "wf1", AgentID: "agent-d", Status: state.PendingTaskStatus, DependsOn: []string{"t2", "t3"}, CreatedAt: time.Now()},
	}
	executor := engine.NewExecutor(3, 10)

	if err := executor.Run(ctx, tasks); err != nil {
		fmt.Printf("workflow failed: %s\n", err)
		return
	}

	fmt.Println("workflow completed successfully!")
}
