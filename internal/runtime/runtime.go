// Package runtime executes a single task's work.
//
// The engine does not know or care what runs inside — that is the point of the
// boundary. Docker is the intended implementation; this interface exists so the
// scheduler can be proven correct against a fake worker first, and so a
// Kubernetes backend can be added later without touching any scheduling code.
package runtime

import (
	"context"

	"github.com/google/uuid"
)

// Request is everything a worker is told about the task it is running.
type Request struct {
	TaskID     uuid.UUID
	WorkflowID uuid.UUID
	TaskKey    string
	AgentName  string

	// Attempt is 1 on the first try. Workers may use it for logging or to
	// distinguish a retry, but must not use it to decide whether work was
	// already done — that is what IdempotencyKey is for.
	Attempt int

	// IdempotencyKey is stable across every attempt at this task, by design.
	//
	// Execution is at-least-once: a node can claim a task, spend real money on
	// LLM calls and send real email, then die before its result is committed.
	// The lease expires, another node reclaims, and it happens again. No
	// scheduler design closes that — the side effects are outside any
	// transaction the engine controls. So the guarantee is stated where it can
	// be kept, and workers with external side effects are required to honour
	// this key.
	IdempotencyKey string

	// Input is the resolved JSON the worker receives. Template resolution is
	// not implemented yet, so today this is the stored input_template verbatim.
	Input []byte
}

// Response is what a worker reports on success.
type Response struct {
	Output     []byte
	TokensUsed int64
	CostMicros int64
}

// Runtime executes one task and returns its result.
//
// A returned error means the attempt failed and should count against the retry
// budget. Respecting ctx cancellation is mandatory: the engine bounds every
// attempt by the shorter of the task's timeout and its lease, and a runtime
// that ignores the deadline lets work outlive the lease that protects it.
type Runtime interface {
	Execute(ctx context.Context, req Request) (*Response, error)
	Name() string
}

// RuntimeFunc adapts a function to Runtime, for tests and for wiring a
// one-off behaviour without declaring a type.
type RuntimeFunc func(ctx context.Context, req Request) (*Response, error)

func (f RuntimeFunc) Execute(ctx context.Context, req Request) (*Response, error) {
	return f(ctx, req)
}

func (f RuntimeFunc) Name() string { return "func" }
