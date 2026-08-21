package telemetry

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Instruments are the engine's metrics.
type Instruments struct {
	claimed   metric.Int64Counter
	completed metric.Int64Counter
	failed    metric.Int64Counter
	cancelled metric.Int64Counter
	reclaimed metric.Int64Counter
	fenced    metric.Int64Counter
	duration  metric.Float64Histogram
	tokens    metric.Int64Counter
	cost      metric.Int64Counter
	inflight  metric.Int64UpDownCounter
}

// meters holds the instruments the whole engine records through.
//
// An atomic pointer rather than a sync.Once, because instruments bind to
// whichever meter provider exists when they are built and stay bound to it.
// Caching them permanently on first use would mean the binding is decided by
// whatever happened to record a metric first — fine in a process that calls
// Init at startup, and silently wrong anywhere a provider is installed later.
// Rebind is explicit instead.
var meters atomic.Pointer[Instruments]

// Meters returns the engine's instruments, building them against the current
// global meter if they do not exist yet.
func Meters() *Instruments {
	if existing := meters.Load(); existing != nil {
		return existing
	}

	built := NewInstruments()
	if meters.CompareAndSwap(nil, built) {
		return built
	}
	return meters.Load()
}

// RebindMeters points the instruments at the current global meter provider.
// Init calls it after installing one; tests call it after installing their own.
func RebindMeters() { meters.Store(NewInstruments()) }

// NewInstruments builds a set of instruments from the current global meter.
func NewInstruments() *Instruments {
	meter := otel.Meter(ScopeName)

	return &Instruments{
		claimed:   counter(meter, "agentflow.tasks.claimed", "Tasks claimed by this node"),
		completed: counter(meter, "agentflow.tasks.completed", "Tasks that finished successfully"),
		failed:    counter(meter, "agentflow.tasks.failed", "Attempts that failed"),
		cancelled: counter(meter, "agentflow.tasks.cancelled", "Tasks cancelled by a failure cascade"),
		reclaimed: counter(meter, "agentflow.tasks.reclaimed", "Tasks taken back from a lost or stalled node"),
		// The number an operator should watch most closely: a fenced write
		// means a task's work was done twice and one result was discarded.
		// Sustained non-zero means the lease TTL is too tight for the workload,
		// not that anything is broken.
		fenced:   counter(meter, "agentflow.tasks.fenced", "Writes rejected because the lease had moved on"),
		duration: histogram(meter, "agentflow.task.duration", "s", "Wall-clock duration of one attempt"),
		tokens:   counter(meter, "agentflow.tokens.used", "Model tokens reported by workers"),
		cost:     counter(meter, "agentflow.cost.micros", "Cost in micros reported by workers"),
		inflight: upDownCounter(meter, "agentflow.pool.inflight", "Tasks currently running on this node"),
	}
}

func counter(meter metric.Meter, name, description string) metric.Int64Counter {
	instrument, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		// Instrument creation only fails on a malformed name, which is a
		// programming error fixed at compile time rather than a runtime
		// condition worth propagating through every call site.
		otel.Handle(err)
	}
	return instrument
}

func histogram(meter metric.Meter, name, unit, description string) metric.Float64Histogram {
	instrument, err := meter.Float64Histogram(name,
		metric.WithUnit(unit), metric.WithDescription(description))
	if err != nil {
		otel.Handle(err)
	}
	return instrument
}

func upDownCounter(meter metric.Meter, name, description string) metric.Int64UpDownCounter {
	instrument, err := meter.Int64UpDownCounter(name, metric.WithDescription(description))
	if err != nil {
		otel.Handle(err)
	}
	return instrument
}

// TaskClaimed records tasks taken by a node in one claim pass.
func (i *Instruments) TaskClaimed(ctx context.Context, count int, engineID string) {
	i.add(ctx, i.claimed, int64(count), AttrEngineID.String(engineID))
}

// TaskCompleted records a success, along with the cost the worker reported.
//
// Cost is best-effort by design: a crash between spending and committing
// undercounts permanently, and making it exact would need a distributed
// transaction with the model provider that does not exist.
func (i *Instruments) TaskCompleted(ctx context.Context, agent string, tokens, costMicros int64, took time.Duration) {
	attrs := AttrAgent.String(agent)
	i.add(ctx, i.completed, 1, attrs)
	i.add(ctx, i.tokens, tokens, attrs)
	i.add(ctx, i.cost, costMicros, attrs)
	i.record(ctx, i.duration, took.Seconds(), attrs, AttrOutcome.String("completed"))
}

// TaskFailed records a failed attempt, whether or not it will be retried.
func (i *Instruments) TaskFailed(ctx context.Context, agent string, took time.Duration) {
	attrs := AttrAgent.String(agent)
	i.add(ctx, i.failed, 1, attrs)
	i.record(ctx, i.duration, took.Seconds(), attrs, AttrOutcome.String("failed"))
}

// TasksCancelled records dependents cancelled by a permanent failure.
func (i *Instruments) TasksCancelled(ctx context.Context, count int) {
	if count > 0 {
		i.add(ctx, i.cancelled, int64(count))
	}
}

// TaskReclaimed records work taken back, tagged with why.
func (i *Instruments) TaskReclaimed(ctx context.Context, reason string) {
	i.add(ctx, i.reclaimed, 1, AttrReason.String(reason))
}

// WriteFenced records a result discarded because the lease had moved on.
func (i *Instruments) WriteFenced(ctx context.Context, agent string) {
	i.add(ctx, i.fenced, 1, AttrAgent.String(agent))
}

// PoolInflight moves the count of tasks running on this node.
func (i *Instruments) PoolInflight(ctx context.Context, delta int) {
	i.addUpDown(ctx, i.inflight, int64(delta))
}

// The guards below keep a failed instrument construction from panicking every
// call site: a nil instrument means metrics are simply not recorded, which is
// the right failure mode for telemetry.

func (i *Instruments) add(ctx context.Context, c metric.Int64Counter, value int64, attrs ...attribute.KeyValue) {
	if c == nil {
		return
	}
	c.Add(ctx, value, metric.WithAttributes(attrs...))
}

func (i *Instruments) addUpDown(ctx context.Context, c metric.Int64UpDownCounter, value int64, attrs ...attribute.KeyValue) {
	if c == nil {
		return
	}
	c.Add(ctx, value, metric.WithAttributes(attrs...))
}

func (i *Instruments) record(ctx context.Context, h metric.Float64Histogram, value float64, attrs ...attribute.KeyValue) {
	if h == nil {
		return
	}
	h.Record(ctx, value, metric.WithAttributes(attrs...))
}
