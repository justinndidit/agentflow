package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// StartAttemptSpan opens the span for one attempt, as a child of the workflow
// root already on ctx.
func StartAttemptSpan(ctx context.Context, attempt SpanSubject) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "task "+attempt.Key(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attempt.Attributes()...),
	)
}

// SpanSubject is the minimum a span needs about its task. An interface rather
// than the row type, so this package stays independent of persistence.
type SpanSubject interface {
	Key() string
	Attributes() []attribute.KeyValue
}

// RecordFailure marks a span failed at a named stage.
//
// The stage matters as much as the error: "the input could not be resolved",
// "the image is missing" and "the agent exited non-zero" are three different
// problems for three different people, and a span that only says "error" makes
// the reader open the logs to find out which.
func RecordFailure(span trace.Span, stage string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, stage+": "+err.Error())
	span.SetAttributes(AttrOutcome.String("failed"), attribute.String("agentflow.stage", stage))
}

// RecordSuccess marks a span completed.
func RecordSuccess(span trace.Span, tokens int64) {
	span.SetStatus(codes.Ok, "")
	span.SetAttributes(
		AttrOutcome.String("completed"),
		attribute.Int64("agentflow.tokens_used", tokens),
	)
}

// RecordArtifact notes that a worker wrote to blob storage instead of returning
// its output inline.
func RecordArtifact(span trace.Span, uri *string) {
	if uri == nil {
		return
	}
	span.SetAttributes(attribute.String("agentflow.artifact_uri", *uri))
}
