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

func New(workerCount, bufferSize int) *WorkerPool {
	jobChan := make(chan *state.Task, bufferSize)
	return &WorkerPool{
		WorkerCount: workerCount,
		BufferSize:  bufferSize,
		jobChan:     jobChan,
	}
}

func (wp *WorkerPool) Submit(t *state.Task) error {
	//select ensures this never blocks
	//tasks are either sent to the channel or defaults - no blocking
	select {
	case wp.jobChan <- t:
		return nil
	default:
		return fmt.Errorf("job channel filled: task %s rejected", t.ID)
	}
}

func (wp *WorkerPool) Start(ctx context.Context) <-chan string {
	resultChan := make(chan string)
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
						//drain logic
						//close job channel first
						close(wp.jobChan)
						for task := range wp.jobChan {
							oldStatus, err := task.Transition(state.CancelledTaskStatus)
							if err != nil {
								resultChan <- fmt.Sprintf("error transitioning state: %s", err)
								continue
							}
							resultChan <- logger.LogTaskTransition(task.ID, oldStatus, task.Status, nil, "context deadline")
						}
					})
					return
				case task, ok := <-wp.jobChan:
					if !ok {
						resultChan <- "job channel closed"
						return
					}
					oldStatus, err := task.Transition(state.RunningTaskStatus)
					if err != nil {
						resultChan <- fmt.Sprintf("error transitioning state: %s", err)
					}
					resultChan <- logger.LogTaskTransition(task.ID, oldStatus, task.Status, &w.id, "")
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
