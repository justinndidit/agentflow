package engine

import (
	"context"
	"testing"
	"time"

	"github.com/justinndidit/agentflow/internal/state"
)

func TestNewWorkerPool_SubmitRejectsWhenFull(t *testing.T) {
	wp := New(1, 1)

	t1 := &state.Task{ID: "t1"}
	t2 := &state.Task{ID: "t2"}

	if err := wp.Submit(t1); err != nil {
		t.Fatalf("expected first submit to succeed, got %v", err)
	}

	if err := wp.Submit(t2); err == nil {
		t.Fatalf("expected second submit to be rejected when buffer full")
	}
}

func TestStart_WaitsForContextCancel(t *testing.T) {
	wp := New(1, 2)
	// no submitted tasks; start and cancel quickly
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	resCh := wp.Start(ctx)
	// consume results until closed
	for range resCh {
	}
}
