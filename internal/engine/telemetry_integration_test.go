//go:build integration

package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/justinndidit/agentflow/internal/blob"
	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/internal/telemetry"
)

// recordSpans installs an in-memory exporter for the duration of a test, so
// tracing is verified without standing up a collector.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})

	return recorder
}

// The claim that the whole scheme rests on: every attempt in a workflow lands
// in one trace, with the workflow's own id as the trace id, even though no node
// was ever told what that trace was.
func TestTelemetry_AllTasksShareTheWorkflowTrace(t *testing.T) {
	ctx := context.Background()
	recorder := recordSpans(t)

	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{
		"fetch":  nil,
		"rank":   {"fetch"},
		"score":  {"fetch"},
		"report": {"rank", "score"},
	})

	node := engine.NewNode(nodeConfig(t, 4), pool, runtime.NewEcho(0), blob.Disabled{}, nopLogger())

	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = node.Run(runCtx) }()
	waitForWorkflow(t, pool, workflow.ID, string(state.CompletedWorkflowStatus), 60*time.Second)
	cancel()

	// Let the last commit's span close before reading.
	time.Sleep(500 * time.Millisecond)

	want := telemetry.TraceIDForWorkflow(workflow.ID)
	parent := telemetry.SpanIDForWorkflow(workflow.ID)

	attempts := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range recorder.Ended() {
		if span.SpanContext().TraceID() != want {
			continue
		}
		for _, attr := range span.Attributes() {
			if attr.Key == "agentflow.task_key" {
				attempts[attr.Value.AsString()] = span
			}
		}
	}

	for _, key := range []string{"fetch", "rank", "score", "report"} {
		span, ok := attempts[key]
		if !ok {
			t.Errorf("no span in the workflow's trace for task %s", key)
			continue
		}
		// The parent was derived, never propagated — the submit process that
		// would have created it is not even running.
		if span.Parent().SpanID() != parent {
			t.Errorf("task %s has parent %s, want the derived workflow root %s",
				key, span.Parent().SpanID(), parent)
		}
		if !span.Parent().IsRemote() {
			t.Errorf("task %s does not treat the workflow root as remote", key)
		}
	}

	if len(attempts) != 4 {
		t.Errorf("found spans for %d tasks, want 4", len(attempts))
	}
}

// A span has to say enough to be useful on its own: which agent, which attempt,
// which node.
func TestTelemetry_AttemptSpansCarryTheirContext(t *testing.T) {
	ctx := context.Background()
	recorder := recordSpans(t)

	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{"only": nil})

	node := engine.NewNode(nodeConfig(t, 2), pool, runtime.NewEcho(0), blob.Disabled{}, nopLogger())
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = node.Run(runCtx) }()
	waitForWorkflow(t, pool, workflow.ID, string(state.CompletedWorkflowStatus), 60*time.Second)
	cancel()
	time.Sleep(500 * time.Millisecond)

	var found bool
	for _, span := range recorder.Ended() {
		attrs := map[string]string{}
		for _, attr := range span.Attributes() {
			attrs[string(attr.Key)] = attr.Value.Emit()
		}
		if attrs["agentflow.task_key"] != "only" {
			continue
		}
		found = true

		if attrs["agentflow.agent"] != "research-agent" {
			t.Errorf("agent = %q", attrs["agentflow.agent"])
		}
		if attrs["agentflow.attempt"] != "1" {
			t.Errorf("attempt = %q, want 1", attrs["agentflow.attempt"])
		}
		if attrs["agentflow.engine_id"] == "" {
			t.Error("no engine id on the span; a trace cannot say which node ran the attempt")
		}
		if attrs["agentflow.outcome"] != "completed" {
			t.Errorf("outcome = %q, want completed", attrs["agentflow.outcome"])
		}
		if attrs["agentflow.workflow_id"] != workflow.ID.String() {
			t.Errorf("workflow_id = %q", attrs["agentflow.workflow_id"])
		}
	}

	if !found {
		t.Fatal("no span recorded for the task")
	}
}

// A failure has to name the stage. "The input could not be resolved", "the
// image is missing" and "the agent exited non-zero" are three different
// problems, and a span that only says "error" makes the reader open the logs.
func TestTelemetry_FailedSpansNameTheStage(t *testing.T) {
	ctx := context.Background()
	recorder := recordSpans(t)

	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{"doomed": nil})
	if _, err := pool.Exec(ctx, `UPDATE tasks SET max_retries = 0`); err != nil {
		t.Fatalf("failed to set the retry budget: %v", err)
	}

	echo := runtime.NewEcho(0)
	echo.FailKeys = map[string]bool{"doomed": true}

	node := engine.NewNode(nodeConfig(t, 2), pool, echo, blob.Disabled{}, nopLogger())
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = node.Run(runCtx) }()
	waitForWorkflow(t, pool, workflow.ID, string(state.FailedWorkflowStatus), 60*time.Second)
	cancel()
	time.Sleep(500 * time.Millisecond)

	var found bool
	for _, span := range recorder.Ended() {
		attrs := map[string]string{}
		for _, attr := range span.Attributes() {
			attrs[string(attr.Key)] = attr.Value.Emit()
		}
		if attrs["agentflow.task_key"] != "doomed" {
			continue
		}
		found = true

		if attrs["agentflow.outcome"] != "failed" {
			t.Errorf("outcome = %q, want failed", attrs["agentflow.outcome"])
		}
		if attrs["agentflow.stage"] != "execute" {
			t.Errorf("stage = %q, want execute", attrs["agentflow.stage"])
		}
		if len(span.Events()) == 0 {
			t.Error("no exception event recorded on the failed span")
		}
	}

	if !found {
		t.Fatal("no span recorded for the failing task")
	}
}

// Two workflows running at once must not blend into one trace.
func TestTelemetry_ConcurrentWorkflowsStaySeparate(t *testing.T) {
	ctx := context.Background()
	recorder := recordSpans(t)

	pool := dbtest.Pool(t)
	first := seedGraph(t, pool, map[string][]string{"a": nil})

	// A second workflow in the same database, sharing the node.
	second := seedGraph(t, pool, map[string][]string{"b": nil})

	node := engine.NewNode(nodeConfig(t, 4), pool, runtime.NewEcho(0), blob.Disabled{}, nopLogger())
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = node.Run(runCtx) }()
	waitForWorkflow(t, pool, first.ID, string(state.CompletedWorkflowStatus), 60*time.Second)
	waitForWorkflow(t, pool, second.ID, string(state.CompletedWorkflowStatus), 60*time.Second)
	cancel()
	time.Sleep(500 * time.Millisecond)

	traces := map[string]uuid.UUID{}
	for _, span := range recorder.Ended() {
		for _, attr := range span.Attributes() {
			if attr.Key == "agentflow.task_key" {
				traces[attr.Value.AsString()] = uuid.UUID(span.SpanContext().TraceID())
			}
		}
	}

	if traces["a"] == traces["b"] {
		t.Error("two workflows' tasks landed in the same trace")
	}
	if traces["a"] != first.ID {
		t.Errorf("task a is in trace %s, want workflow %s", traces["a"], first.ID)
	}
	if traces["b"] != second.ID {
		t.Errorf("task b is in trace %s, want workflow %s", traces["b"], second.ID)
	}
}
