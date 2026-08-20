package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func request(taskKey string) Request {
	return Request{
		TaskID:         uuid.New(),
		WorkflowID:     uuid.New(),
		TaskKey:        taskKey,
		AgentName:      "research-agent",
		Attempt:        1,
		IdempotencyKey: "stable",
		Input:          []byte(`{"role":"engineer"}`),
	}
}

func TestEcho_ReturnsItsInput(t *testing.T) {
	echo := NewEcho(0)

	response, err := echo.Execute(context.Background(), request("a"))
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(response.Output) != `{"role":"engineer"}` {
		t.Errorf("Output = %s, want the input echoed back", response.Output)
	}
	if response.TokensUsed == 0 {
		t.Error("TokensUsed = 0; cost accounting has nothing to accumulate")
	}
}

// input_template is NOT NULL, but an empty payload should still produce storable
// JSON rather than an empty column.
func TestEcho_EmptyInputProducesValidJSON(t *testing.T) {
	echo := NewEcho(0)

	req := request("a")
	req.Input = nil

	response, err := echo.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if string(response.Output) != `{}` {
		t.Errorf("Output = %s, want an empty JSON object", response.Output)
	}
}

func TestEcho_FailsNamedTasks(t *testing.T) {
	echo := NewEcho(0)
	echo.FailKeys = map[string]bool{"doomed": true}

	if _, err := echo.Execute(context.Background(), request("doomed")); err == nil {
		t.Error("expected the named task to fail")
	}
	if _, err := echo.Execute(context.Background(), request("fine")); err != nil {
		t.Errorf("unnamed task failed: %v", err)
	}
}

// The engine bounds every attempt by the shorter of the task timeout and the
// lease. A runtime that ignored the deadline would keep working past the lease
// that protects it, and its result would be committed against a lease another
// node already holds.
func TestEcho_RespectsCancellation(t *testing.T) {
	echo := NewEcho(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	started := time.Now()
	_, err := echo.Execute(ctx, request("slow"))
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("expected the cancelled attempt to fail")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want it to wrap the deadline", err)
	}
	if elapsed > time.Second {
		t.Errorf("Execute took %s; the wait is not interruptible", elapsed)
	}
}

func TestEcho_Name(t *testing.T) {
	if got := NewEcho(0).Name(); got != "echo" {
		t.Errorf("Name() = %q, want echo", got)
	}
}
