// Package telemetry emits traces and metrics for the execution path.
package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// A workflow's trace has to span several nodes and, for a long pipeline, hours
// — and the nodes never talk to each other. Propagating a trace context the
// usual way would mean carrying it from whoever submitted the manifest to
// whichever node happens to claim each task, which is a coordination channel
// this architecture deliberately does not have.
//
// So trace identity is derived rather than propagated. A W3C trace ID is
// sixteen bytes and so is a UUID, which means a workflow's own id *is* its
// trace id: any node, at any time, can work out where its spans belong from
// data it already holds. Readiness propagates through the rows rather than
// through a scheduler, and so does trace context.

// TraceIDForWorkflow returns the trace every span in a workflow belongs to.
func TraceIDForWorkflow(workflowID uuid.UUID) trace.TraceID {
	return trace.TraceID(workflowID)
}

// SpanIDForWorkflow returns the id of a workflow's root span.
//
// Derived from the workflow id rather than generated, so a task span created on
// one node can name a parent that was created on another — or that has not been
// created yet, since submit and execution are separate processes.
func SpanIDForWorkflow(workflowID uuid.UUID) trace.SpanID {
	return deriveSpanID("workflow", workflowID[:], 0)
}

// SpanIDForAttempt returns the id of one attempt's span.
//
// Keyed by attempt as well as task, so a retry is a sibling span rather than a
// second span claiming the same identity — the same reason task_results is
// keyed (task_id, attempt).
func SpanIDForAttempt(taskID uuid.UUID, attempt int) trace.SpanID {
	return deriveSpanID("attempt", taskID[:], attempt)
}

// deriveSpanID hashes its inputs down to eight bytes.
//
// A span id of all zeros is invalid per the spec and would make the span look
// parentless, so the astronomically unlikely case is nudged rather than left to
// produce a silently broken trace.
func deriveSpanID(kind string, id []byte, ordinal int) trace.SpanID {
	hash := sha256.New()
	hash.Write([]byte(kind))
	hash.Write(id)

	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], uint64(ordinal))
	hash.Write(suffix[:])

	var spanID trace.SpanID
	copy(spanID[:], hash.Sum(nil))

	if !spanID.IsValid() {
		spanID[7] = 1
	}
	return spanID
}

// WorkflowContext returns a context whose active span is the workflow's root,
// so anything started from it becomes a child in the right trace.
//
// The parent is marked remote because it genuinely is: it was created by a
// different process, usually on a different machine, quite possibly hours
// earlier.
func WorkflowContext(ctx context.Context, workflowID uuid.UUID) context.Context {
	return trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    TraceIDForWorkflow(workflowID),
		SpanID:     SpanIDForWorkflow(workflowID),
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	}))
}
