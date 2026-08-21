package telemetry

import (
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// A workflow's id is its trace id. That is what lets a node derive where its
// spans belong from data it already holds, with no context propagated between
// processes that never talk to each other.
func TestTraceIDForWorkflow_IsTheWorkflowID(t *testing.T) {
	workflowID := uuid.New()
	traceID := TraceIDForWorkflow(workflowID)

	if !traceID.IsValid() {
		t.Fatal("derived trace id is not valid")
	}
	// Compared as UUIDs rather than as strings: a trace id renders as plain
	// hex and a UUID renders with dashes, so their String forms never match
	// even when the bytes are identical.
	if uuid.UUID(traceID) != workflowID {
		t.Errorf("trace id = %s, want the workflow id %s", uuid.UUID(traceID), workflowID)
	}
	for i := range workflowID {
		if traceID[i] != workflowID[i] {
			t.Fatalf("trace id byte %d = %x, want %x", i, traceID[i], workflowID[i])
		}
	}
}

// Derivation has to be a function of the id alone, or two nodes tracing the
// same workflow would produce two traces.
func TestDerivation_IsDeterministic(t *testing.T) {
	workflowID, taskID := uuid.New(), uuid.New()

	for range 100 {
		if TraceIDForWorkflow(workflowID) != TraceIDForWorkflow(workflowID) {
			t.Fatal("TraceIDForWorkflow is not deterministic")
		}
		if SpanIDForWorkflow(workflowID) != SpanIDForWorkflow(workflowID) {
			t.Fatal("SpanIDForWorkflow is not deterministic")
		}
		if SpanIDForAttempt(taskID, 2) != SpanIDForAttempt(taskID, 2) {
			t.Fatal("SpanIDForAttempt is not deterministic")
		}
	}
}

// Distinct inputs must give distinct ids, or spans collide and a trace becomes
// unreadable.
func TestDerivation_IsCollisionFreeAcrossInputs(t *testing.T) {
	seen := map[string]string{}
	note := func(kind, id string) {
		if previous, clash := seen[id]; clash {
			t.Errorf("%s collides with %s", kind, previous)
		}
		seen[id] = kind
	}

	for range 200 {
		workflowID, taskID := uuid.New(), uuid.New()
		note("workflow span", SpanIDForWorkflow(workflowID).String())
		for attempt := 1; attempt <= 4; attempt++ {
			note("attempt span", SpanIDForAttempt(taskID, attempt).String())
		}
	}
}

// A retry is a sibling span rather than a second span claiming the same
// identity — the same reason task_results is keyed (task_id, attempt).
func TestSpanIDForAttempt_DiffersPerAttempt(t *testing.T) {
	taskID := uuid.New()

	first := SpanIDForAttempt(taskID, 1)
	second := SpanIDForAttempt(taskID, 2)

	if first == second {
		t.Error("two attempts of the same task share a span id")
	}
	if !first.IsValid() || !second.IsValid() {
		t.Error("derived span ids are not valid")
	}
}

// A workflow's root span and its attempts' spans must not collide, or an
// attempt would appear to be its own parent.
func TestSpanIDs_RootAndAttemptsDiffer(t *testing.T) {
	id := uuid.New()

	if SpanIDForWorkflow(id) == SpanIDForAttempt(id, 1) {
		t.Error("a workflow root and an attempt derived the same span id")
	}
}

// An all-zero span id is invalid per the spec and would make a span look
// parentless, so the derivation must never produce one.
func TestDerivedSpanIDs_AreNeverZero(t *testing.T) {
	var zero uuid.UUID

	for _, id := range []uuid.UUID{zero, uuid.New(), uuid.New()} {
		if !SpanIDForWorkflow(id).IsValid() {
			t.Errorf("SpanIDForWorkflow(%s) is invalid", id)
		}
		for attempt := 0; attempt <= 3; attempt++ {
			if !SpanIDForAttempt(id, attempt).IsValid() {
				t.Errorf("SpanIDForAttempt(%s, %d) is invalid", id, attempt)
			}
		}
	}
}

// WorkflowContext has to put a valid, sampled, remote parent on the context, or
// spans started from it are orphans in a different trace.
func TestWorkflowContext_CarriesTheDerivedParent(t *testing.T) {
	workflowID := uuid.New()

	ctx := WorkflowContext(t.Context(), workflowID)
	spanCtx := trace.SpanContextFromContext(ctx)

	if !spanCtx.IsValid() {
		t.Fatal("WorkflowContext produced an invalid span context")
	}
	if spanCtx.TraceID() != TraceIDForWorkflow(workflowID) {
		t.Error("span context carries the wrong trace id")
	}
	if spanCtx.SpanID() != SpanIDForWorkflow(workflowID) {
		t.Error("span context carries the wrong parent span id")
	}
	// Remote because it genuinely is: created by another process, often on
	// another machine, possibly hours earlier.
	if !spanCtx.IsRemote() {
		t.Error("the workflow parent is not marked remote")
	}
	// Sampled, or ParentBased sampling drops every task span in the workflow.
	if !spanCtx.IsSampled() {
		t.Error("the workflow parent is not marked sampled")
	}
}

// Two different workflows must never share a trace.
func TestWorkflowContext_SeparatesWorkflows(t *testing.T) {
	first := WorkflowContext(t.Context(), uuid.New())
	second := WorkflowContext(t.Context(), uuid.New())

	if trace.SpanContextFromContext(first).TraceID() ==
		trace.SpanContextFromContext(second).TraceID() {
		t.Error("two workflows landed in the same trace")
	}
}
