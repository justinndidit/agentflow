package engine

import (
	"context"
	"time"

	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/telemetry"
	"github.com/rs/zerolog"
)

// DefaultRetentionInterval is how often a node sweeps for expired workflows.
// Hourly is ample: retention is measured in days, so sweeping more often only
// spreads the same deletes across more passes.
const DefaultRetentionInterval = time.Hour

// DefaultRetentionBatch bounds one pass, so a large backlog is worked through
// over several sweeps rather than one statement holding locks across a wide
// slice of the table.
const DefaultRetentionBatch = 500

// Retention deletes finished workflows once they are old enough.
//
// Without it tasks and task_results grow forever: every run leaves rows behind,
// and the scheduling path pays for them through tables that only get bigger.
// The alternative the architecture doc offers is partitioning by month, which
// is the better answer at large volume and a much heavier change — it forces
// the task primary key to include created_at, which breaks the foreign key from
// task_results and turns lookups by id alone into a scan of every partition.
// Retention caps growth without touching the schema, and unlike partitioning it
// does not get harder to adopt later.
//
// Every node runs a sweep. Concurrent deletes are safe: the batch is selected
// under SKIP LOCKED, so two nodes take disjoint slices, and a row deleted twice
// is simply a row that is already gone.
type Retention struct {
	txManager *repositories.TxManager
	logger    *zerolog.Logger

	maxAge    time.Duration
	interval  time.Duration
	batchSize int
}

type RetentionOption func(*Retention)

// WithRetentionInterval overrides how often the sweep runs. Mainly for tests.
func WithRetentionInterval(interval time.Duration) RetentionOption {
	return func(r *Retention) { r.interval = interval }
}

// WithRetentionBatch overrides how many workflows one pass removes.
func WithRetentionBatch(size int) RetentionOption {
	return func(r *Retention) { r.batchSize = size }
}

func NewRetention(
	txManager *repositories.TxManager,
	cfg *config.Retention,
	logger *zerolog.Logger,
	opts ...RetentionOption,
) *Retention {
	var maxAge time.Duration
	if cfg != nil {
		maxAge = time.Duration(cfg.MaxAgeDays) * 24 * time.Hour
	}

	r := &Retention{
		txManager: txManager,
		logger:    logger,
		maxAge:    maxAge,
		interval:  DefaultRetentionInterval,
		batchSize: DefaultRetentionBatch,
	}
	for _, opt := range opts {
		opt(r)
	}

	// Defend against a config that never went through validation, the same way
	// the reaper and dispatcher do.
	if r.interval <= 0 {
		r.interval = DefaultRetentionInterval
	}
	if r.batchSize <= 0 {
		r.batchSize = DefaultRetentionBatch
	}
	return r
}

// Run sweeps until ctx is cancelled.
func (r *Retention) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.Info().
		Str("func", "Run").
		Dur("interval", r.interval).
		Dur("max_age", r.maxAge).
		Int("batch_size", r.batchSize).
		Msg("retention started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Info().Str("func", "Run").Msg("retention stopped")
			return nil

		case <-ticker.C:
			if _, err := r.SweepOnce(ctx); err != nil {
				// Logged and retried next tick. Failing to reclaim disk is not
				// a reason to stop running work.
				r.logger.Error().Err(err).
					Str("func", "Run").
					Msg("retention sweep failed, retrying next tick")
			}
		}
	}
}

// SweepOnce deletes one batch and reports how many workflows went.
func (r *Retention) SweepOnce(ctx context.Context) (int, error) {
	if r.maxAge <= 0 {
		// No retention window means keep everything, which is what a
		// deployment that wants its history intact should get.
		return 0, nil
	}

	var deleted int
	err := r.txManager.WithTransaction(ctx, func(ctx context.Context, stores *repositories.Stores) error {
		count, err := stores.WorkflowStore.DeleteExpired(ctx, r.maxAge, r.batchSize)
		if err != nil {
			return err
		}
		deleted = count
		return nil
	})
	if err != nil {
		return 0, err
	}

	if deleted > 0 {
		telemetry.Meters().WorkflowsExpired(ctx, deleted)
		r.logger.Info().
			Str("func", "SweepOnce").
			Int("workflows", deleted).
			Dur("older_than", r.maxAge).
			Msg("deleted expired workflows")
	}

	return deleted, nil
}
