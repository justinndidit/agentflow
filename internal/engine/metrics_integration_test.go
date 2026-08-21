//go:build integration

package engine_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/justinndidit/agentflow/internal/blob"
	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/internal/telemetry"
)

// recordMetrics installs an in-memory reader for the duration of a test.
//
// RebindMeters is the important part. Instruments stay bound to the provider
// they were built against, so without an explicit rebind every test after the
// first would record into a provider that had already been shut down and read
// zero for everything.
func recordMetrics(t *testing.T) *metric.ManualReader {
	t.Helper()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	telemetry.RebindMeters()

	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetMeterProvider(previous)
		telemetry.RebindMeters()
	})

	return reader
}

// counterValue sums every data point of a counter, across attribute sets.
func counterValue(t *testing.T, reader *metric.ManualReader, name string) int64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", name, m.Data)
			}
			for _, point := range sum.DataPoints {
				total += point.Value
			}
		}
	}
	return total
}

func TestMetrics_CountsAWorkflowThrough(t *testing.T) {
	ctx := context.Background()
	reader := recordMetrics(t)

	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{
		"fetch": nil,
		"rank":  {"fetch"},
		"score": {"fetch"},
	})

	node := engine.NewNode(nodeConfig(t, 4), pool, runtime.NewEcho(0), blob.Disabled{}, nopLogger())
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = node.Run(runCtx) }()
	waitForWorkflow(t, pool, workflow.ID, string(state.CompletedWorkflowStatus), 60*time.Second)
	cancel()
	time.Sleep(500 * time.Millisecond)

	if got := counterValue(t, reader, "agentflow.tasks.claimed"); got != 3 {
		t.Errorf("tasks.claimed = %d, want 3", got)
	}
	if got := counterValue(t, reader, "agentflow.tasks.completed"); got != 3 {
		t.Errorf("tasks.completed = %d, want 3", got)
	}
	if got := counterValue(t, reader, "agentflow.tasks.failed"); got != 0 {
		t.Errorf("tasks.failed = %d, want 0", got)
	}
	// Cost aggregation: the echo runtime reports 100 tokens per task.
	if got := counterValue(t, reader, "agentflow.tokens.used"); got != 300 {
		t.Errorf("tokens.used = %d, want 300", got)
	}
	// Every started task finished, so the gauge is back where it began.
	if got := counterValue(t, reader, "agentflow.pool.inflight"); got != 0 {
		t.Errorf("pool.inflight = %d, want 0 once the workflow is done", got)
	}
}

// A permanent failure should show up as failures, and as the dependents it
// cancelled.
func TestMetrics_CountsFailuresAndCascades(t *testing.T) {
	ctx := context.Background()
	reader := recordMetrics(t)

	pool := dbtest.Pool(t)
	workflow := seedGraph(t, pool, map[string][]string{
		"doomed": nil,
		"child":  {"doomed"},
	})
	if _, err := pool.Exec(ctx, `UPDATE tasks SET max_retries = 0`); err != nil {
		t.Fatalf("failed to set the retry budget: %v", err)
	}

	echo := runtime.NewEcho(0)
	echo.FailKeys = map[string]bool{"doomed": true}

	node := engine.NewNode(nodeConfig(t, 4), pool, echo, blob.Disabled{}, nopLogger())
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = node.Run(runCtx) }()
	waitForWorkflow(t, pool, workflow.ID, string(state.FailedWorkflowStatus), 60*time.Second)
	cancel()
	time.Sleep(500 * time.Millisecond)

	if got := counterValue(t, reader, "agentflow.tasks.failed"); got < 1 {
		t.Errorf("tasks.failed = %d, want at least 1", got)
	}
	if got := counterValue(t, reader, "agentflow.tasks.cancelled"); got != 1 {
		t.Errorf("tasks.cancelled = %d, want the one dependent", got)
	}
}

// Reclaims are tagged with why, because "the node died" and "the work overran
// its lease" call for different responses.
func TestMetrics_CountsReclaims(t *testing.T) {
	ctx := context.Background()
	reader := recordMetrics(t)

	f := newCommitFixture(t, map[string][]string{"hung": nil}, noBackoff)
	claimed := f.claim(t)["hung"]
	expireLease(t, f.pool, claimed.ID)

	if _, err := newReaper(f.pool).ReapOnce(ctx); err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}

	if got := counterValue(t, reader, "agentflow.tasks.reclaimed"); got != 1 {
		t.Errorf("tasks.reclaimed = %d, want 1", got)
	}
}

// A fenced write means a task's work was done twice and one result was thrown
// away. It is the number an operator should watch most closely, so it has to be
// counted rather than only logged.
func TestMetrics_CountsFencedWrites(t *testing.T) {
	ctx := context.Background()
	reader := recordMetrics(t)

	f := newCommitFixture(t, map[string][]string{"contested": nil}, noBackoff)
	claimed := f.claimedNow(t)

	gate := make(chan struct{})
	pool := engine.NewPool(2, runtime.RuntimeFunc(
		func(ctx context.Context, req runtime.Request) (*runtime.Response, error) {
			<-gate
			return &runtime.Response{Output: []byte(`{"from":"stalled"}`)}, nil
		}),
		engine.NewCommitter(repositories.NewTxManager(f.pool, nopLogger()), nopLogger(), noBackoff),
		engine.StaticResolver{}, engine.StaticAgentImages("unused"),
		blob.Disabled{}, testLeaseTTL, nopLogger())

	if err := pool.Handle(ctx, claimed); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	// The lease is reclaimed and the task redone while the first worker stalls.
	expireLease(t, f.pool, claimed[0].ID)
	if _, err := newReaper(f.pool).ReapOnce(ctx); err != nil {
		t.Fatalf("ReapOnce failed: %v", err)
	}
	replacement := f.claimedNow(t)
	if len(replacement) != 1 {
		t.Fatalf("re-claimed %d tasks, want 1", len(replacement))
	}
	err := f.committer.Commit(ctx, repositories.FenceFor(replacement[0]), engine.Outcome{
		Output: []byte(`{"from":"replacement"}`),
	})
	if err != nil {
		t.Fatalf("the replacement's commit failed: %v", err)
	}

	close(gate)
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if got := counterValue(t, reader, "agentflow.tasks.fenced"); got != 1 {
		t.Errorf("tasks.fenced = %d, want 1", got)
	}
}
