package runtime

import (
	"context"
	"fmt"
	"time"
)

// Echo is a worker that runs no container: it waits, then returns its input as
// its output.
//
// It exists so the claim, commit and reap loops can be proven correct before
// containers are involved. That ordering is deliberate — a scheduling bug and a
// container bug look identical from the outside, and separating them after the
// fact is far harder than never mixing them.
type Echo struct {
	// Delay is how long each task appears to take. Zero returns immediately.
	Delay time.Duration

	// FailKeys names task keys that should fail rather than succeed, so the
	// retry and cascade paths can be exercised end to end.
	FailKeys map[string]bool

	// TokensPerTask is reported on every success, so cost accounting has
	// something to accumulate.
	TokensPerTask int64
}

func NewEcho(delay time.Duration) *Echo {
	return &Echo{Delay: delay, TokensPerTask: 100}
}

func (e *Echo) Name() string { return "echo" }

// Execute waits out the delay and echoes the input back.
//
// The wait is interruptible. A runtime that slept unconditionally would keep
// running past its deadline, and the engine would then be committing a result
// for a lease it no longer holds.
func (e *Echo) Execute(ctx context.Context, req Request) (*Response, error) {
	if e.Delay > 0 {
		timer := time.NewTimer(e.Delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("task %s cancelled after %s: %w",
				req.TaskKey, e.Delay, ctx.Err())
		case <-timer.C:
		}
	}

	if e.FailKeys[req.TaskKey] {
		return nil, fmt.Errorf("echo runtime was told to fail task %s", req.TaskKey)
	}

	output := req.Input
	if len(output) == 0 {
		output = []byte(`{}`)
	}

	return &Response{
		Output:     output,
		TokensUsed: e.TokensPerTask,
	}, nil
}
