package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName identifies this instrumentation in exported telemetry.
const ScopeName = "github.com/justinndidit/agentflow"

// Config addresses an OTLP collector.
type Config struct {
	Enabled bool

	// Endpoint is the collector's host:port for OTLP over gRPC.
	Endpoint string

	ServiceName string
	Insecure    bool

	// SampleRatio is the fraction of workflows traced, from 0 to 1.
	//
	// Sampling is per trace and a workflow is one trace, so a sampled workflow
	// is sampled whole. Half of every workflow's spans would be far less useful
	// than all of half of them.
	SampleRatio float64
}

// Shutdown flushes and releases the providers. Always non-nil, so a caller can
// defer it without checking.
type Shutdown func(context.Context) error

// Init installs the global tracer and meter providers.
//
// Disabled telemetry is not an error and not a special case at the call sites:
// OpenTelemetry's default global providers are no-ops, so every span and metric
// in the codebase costs almost nothing and the instrumentation reads the same
// either way.
func Init(ctx context.Context, cfg Config, logger *zerolog.Logger) (Shutdown, error) {
	if !cfg.Enabled {
		logger.Info().Str("func", "Init").Msg("telemetry disabled")
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
	))
	if err != nil {
		return nil, fmt.Errorf("build telemetry resource: %w", err)
	}

	traceOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	metricOpts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
	}

	traceExporter, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
		// ParentBased so a task span inherits the workflow's sampling decision.
		// Sampling each span independently would scatter a workflow across
		// partial traces, which is worse than not tracing it at all.
		sdktrace.WithSampler(sdktrace.ParentBased(
			sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)

	metricExporter, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		return nil, fmt.Errorf("create metric exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(30*time.Second))),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)

	// After the provider, or the instruments bind to the no-op that preceded it
	// and every counter reports zero forever.
	RebindMeters()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	// Exporter failures are logged rather than returned. A collector being down
	// must never stop a node running work — telemetry is for watching the
	// system, not part of it.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn().Err(err).Str("component", "telemetry").Msg("telemetry export failed")
	}))

	logger.Info().
		Str("func", "Init").
		Str("endpoint", cfg.Endpoint).
		Str("service", cfg.ServiceName).
		Float64("sample_ratio", cfg.SampleRatio).
		Msg("telemetry enabled")

	return func(ctx context.Context) error {
		// Flush both before returning: a node that shuts down cleanly should
		// not drop the spans explaining what it did last.
		traceErr := tracerProvider.Shutdown(ctx)
		metricErr := meterProvider.Shutdown(ctx)
		if traceErr != nil {
			return traceErr
		}
		return metricErr
	}, nil
}

// Tracer returns the engine's tracer.
func Tracer() trace.Tracer { return otel.Tracer(ScopeName) }

// Attribute keys, named once so a dashboard query written against one signal
// keeps working against another.
const (
	AttrWorkflowID = attribute.Key("agentflow.workflow_id")
	AttrTaskID     = attribute.Key("agentflow.task_id")
	AttrTaskKey    = attribute.Key("agentflow.task_key")
	AttrAgent      = attribute.Key("agentflow.agent")
	AttrAttempt    = attribute.Key("agentflow.attempt")
	AttrEngineID   = attribute.Key("agentflow.engine_id")
	AttrOutcome    = attribute.Key("agentflow.outcome")
	AttrReason     = attribute.Key("agentflow.reason")
	AttrRuntime    = attribute.Key("agentflow.runtime")
)
