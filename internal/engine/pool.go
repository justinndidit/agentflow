package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/pkg/logger"
)

type WorkerPool struct {
	WorkerCount int
	BufferSize  int
	jobChan     chan *state.Task
	wg          sync.WaitGroup
}

func NewWorkerPool(workerCount, bufferSize int) *WorkerPool {
	jobChan := make(chan *state.Task, bufferSize)
	return &WorkerPool{
		WorkerCount: workerCount,
		BufferSize:  bufferSize,
		jobChan:     jobChan,
	}
}

// public API to submit tasks for execution
func (wp *WorkerPool) TrySubmit(t *state.Task) error {
	//select ensures this never blocks
	//tasks are either sent to the channel or defaults - no blocking
	select {
	case wp.jobChan <- t:
		return nil
	default:
		return fmt.Errorf("job channel filled: task %s rejected", t.ID)
	}
}

// Internal submission - blocking submission
// Ensures all tasks in an execution wave gets submitted
func (wp *WorkerPool) Submit(ctx context.Context, t *state.Task) error {
	select {
	case wp.jobChan <- t:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while submitting task %s", t.ID)
	}
}

func (wp *WorkerPool) Start(ctx context.Context) <-chan TaskResult {
	resultChan := make(chan TaskResult)
	var once sync.Once

	for n := range wp.WorkerCount {
		wp.wg.Add(1)
		go func(n int) {
			defer wp.wg.Done()
			w := &worker{
				id: n,
			}
			for {
				select {
				case <-ctx.Done():
					//drain job channel
					once.Do(func() {
						//close job channel first
						close(wp.jobChan)
						for task := range wp.jobChan {
							oldStatus, err := task.Transition(state.CancelledTaskStatus)
							if err != nil {
								msg := fmt.Sprintf("error transitioning state: %s", err)
								resultChan <- newTaskResult(nil, state.FailedTaskStatus, msg, err)
								continue
							}
							msg := logger.LogTaskTransition(task.ID, oldStatus, state.CancelledTaskStatus, nil, "context deadline")
							resultChan <- newTaskResult(task, state.CancelledTaskStatus, msg, nil)
						}
					})
					return
				case task, ok := <-wp.jobChan:
					if !ok {
						msg := "job channel closed"
						resultChan <- newTaskResult(task, state.CancelledTaskStatus, msg, nil)
						return
					}
					oldStatus, err := task.Transition(state.RunningTaskStatus)
					if err != nil {
						msg := fmt.Sprintf("error transitioning state: %s", err)
						resultChan <- newTaskResult(task, state.FailedTaskStatus, msg, err)
					}
					msg := logger.LogTaskTransition(task.ID, oldStatus, state.RunningTaskStatus, &w.id, "")
					resultChan <- newTaskResult(task, state.RunningTaskStatus, msg, nil)
					resultChan <- w.run(ctx, task)
				}
			}
		}(n + 1)
	}

	go func() {
		wp.wg.Wait()
		close(resultChan)
	}()
	return resultChan
}
