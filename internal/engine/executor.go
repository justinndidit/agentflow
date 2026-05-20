package engine

import (
	"context"
	"fmt"
	"slices"

	"github.com/justinndidit/agentflow/internal/state"
)

type Executor struct {
	pool *WorkerPool
}

func NewExecutor(workerCount, bufferSize int) *Executor {
	return &Executor{
		pool: NewWorkerPool(workerCount, bufferSize),
	}
}

func (e *Executor) Run(ctx context.Context, tasks []*state.Task) error {
	graph, err := NewGraph(tasks)
	if err != nil {
		return fmt.Errorf("error building task graph: %s", err)
	}

	waves, err := graph.TopologicalSort()
	if err != nil {
		return fmt.Errorf("error sorting order of execution of tasks: %s", err)
	}

	resultChan := e.pool.Start(ctx)

	for n, wave := range waves {
		completed := 0

		//submit wave of task
		for _, task := range wave {
			if err := e.pool.Submit(ctx, task); err != nil {
				_, tErr := task.Transition(state.FailedTaskStatus)
				if tErr != nil {
					return fmt.Errorf("failed to submit task: %s -> transition error: %s", err, tErr)
				}
				return fmt.Errorf("failed to submit task: %s", err)
			}
		}
		//collect result
		for completed < len(wave) {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled on wave %d", n)
			case result, ok := <-resultChan:
				if !ok {
					if completed == len(wave) {
						continue
					}
					return fmt.Errorf("result channel closed unexpectedly on wave %d", n)
				}

				if result.Task != nil && isTerminal(result.Status) {
					completed++
				}
				// if len(wave) == completed {
				// 	break
				// }
			}
		}
	}
	return nil
}

func isTerminal(status state.TaskStatus) bool {
	return slices.Contains(
		[]state.TaskStatus{state.CancelledTaskStatus, state.CompletedTaskStatus, state.FailedTaskStatus},
		status)
}
