package main

import (
	"context"
	"fmt"
	"time"

	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/state"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	wp := engine.New(3, 10)

	for n := range 8 {
		task := &state.Task{ID: fmt.Sprintf("t-%d", n+1), WorkflowID: "wf1", AgentID: "agent-a", Status: state.PendingTaskStatus, CreatedAt: time.Now()}
		if err := wp.Submit(task); err != nil {
			fmt.Println(err)
			continue
		}
	}

	for result := range wp.Start(ctx) {
		fmt.Println(result)
	}
	fmt.Println("All done!")
}
