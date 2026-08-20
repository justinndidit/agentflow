package engine

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/rs/zerolog"
)

// DefaultPollInterval is the floor on how often a node looks for work when
// nothing wakes it sooner. It is a floor rather than the primary trigger: once
// LISTEN/NOTIFY lands the committer will wake dispatchers directly, and this
// only covers the gap where a notification was missed.
const DefaultPollInterval = 2 * time.Second

// Capacity reports how many more tasks a node can take right now. The worker
// pool implements it; until that exists, a static value stands in.
type Capacity interface {
	FreeSlots() int
}

// StaticCapacity is a fixed number of slots, for tests and for wiring the
// dispatcher before the pool exists.
type StaticCapacity int

func (c StaticCapacity) FreeSlots() int { return int(c) }

// Handler receives tasks the dispatcher has claimed. Returning an error means
// the node could not take them on after all; the tasks keep their lease and are
// reclaimed when it expires, rather than being silently dropped.
type Handler interface {
	Handle(ctx context.Context, tasks []*models.TaskRow) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, tasks []*models.TaskRow) error

func (f HandlerFunc) Handle(ctx context.Context, tasks []*models.TaskRow) error {
	return f(ctx, tasks)
}

// Dispatcher claims ready work for one node and hands it to a Handler.
//
// It owns no scheduling logic of its own — readiness is a property of the task
// rows, and the claim query decides what is runnable. This loop only answers
// "how much, how often, and for whom".
type Dispatcher struct {
	store    repositories.TaskStore
	capacity Capacity
	handler  Handler
	logger   *zerolog.Logger

	engineID     uuid.UUID
	leaseTTL     time.Duration
	batchSize    int
	pollInterval time.Duration
}

type DispatcherOption func(*Dispatcher)

// WithPollInterval overrides the poll floor. Mainly for tests, which cannot
// wait two seconds per assertion.
func WithPollInterval(interval time.Duration) DispatcherOption {
	return func(d *Dispatcher) { d.pollInterval = interval }
}

// WithBatchSize caps how many tasks one claim asks for, independently of
// capacity. One round trip per task caps throughput at 1/rtt per node, so the
// default is the node's whole capacity rather than a single task.
func WithBatchSize(size int) DispatcherOption {
	return func(d *Dispatcher) { d.batchSize = size }
}

func NewDispatcher(
	store repositories.TaskStore,
	engineID uuid.UUID,
	capacity Capacity,
	handler Handler,
	cfg *config.Engine,
	logger *zerolog.Logger,
	opts ...DispatcherOption,
) *Dispatcher {
	d := &Dispatcher{
		store:        store,
		capacity:     capacity,
		handler:      handler,
		logger:       logger,
		engineID:     engineID,
		leaseTTL:     time.Duration(cfg.LeaseTTL) * time.Second,
		batchSize:    cfg.Capacity,
		pollInterval: DefaultPollInterval,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Run claims until ctx is cancelled.
//
// The loop deliberately keeps claiming while a pass came back full: a node with
// free capacity and a full queue should not wait out a poll interval between
// batches. It falls back to the ticker only once a pass returns less than it
// asked for, which is the signal that the ready queue is drained.
func (d *Dispatcher) Run(ctx context.Context) error {
	if d.engineID == uuid.Nil {
		return errors.New("dispatcher: Run called with no engine id; register first")
	}

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	d.logger.Info().
		Str("func", "Run").
		Str("engine_id", d.engineID.String()).
		Dur("poll_interval", d.pollInterval).
		Dur("lease_ttl", d.leaseTTL).
		Int("batch_size", d.batchSize).
		Msg("dispatcher started")

	for {
		select {
		case <-ctx.Done():
			d.logger.Info().Str("func", "Run").Msg("dispatcher stopped")
			return nil

		case <-ticker.C:
			for {
				claimed, err := d.claimOnce(ctx)
				if err != nil {
					// Logged and retried on the next tick. A database blip
					// should not stop a node claiming forever.
					d.logger.Error().Err(err).
						Str("func", "Run").
						Str("engine_id", d.engineID.String()).
						Msg("claim failed, retrying next tick")
					break
				}
				// Short read means the ready queue is drained; wait for the
				// next tick rather than spinning on an empty table.
				if claimed < d.claimLimit() || claimed == 0 {
					break
				}
				if ctx.Err() != nil {
					break
				}
			}
		}
	}
}

// ClaimOnce runs a single claim pass and reports how many tasks it took. Run
// calls it on a tick; tests call it directly to avoid waiting on one.
func (d *Dispatcher) ClaimOnce(ctx context.Context) (int, error) {
	return d.claimOnce(ctx)
}

func (d *Dispatcher) claimOnce(ctx context.Context) (int, error) {
	limit := d.claimLimit()
	if limit <= 0 {
		// Full. Claiming anything now would mean holding a lease this node
		// cannot start work against.
		return 0, nil
	}

	claimed, err := d.store.ClaimTasks(ctx, d.engineID, limit, d.leaseTTL)
	if err != nil {
		return 0, err
	}
	if len(claimed) == 0 {
		return 0, nil
	}

	d.logger.Info().
		Str("func", "claimOnce").
		Str("engine_id", d.engineID.String()).
		Int("claimed", len(claimed)).
		Int("limit", limit).
		Msg("claimed tasks")

	if err := d.handler.Handle(ctx, claimed); err != nil {
		// The tasks are already leased to this node. Nothing is rolled back:
		// the lease expires and the reaper returns them, which is the same path
		// a crash between claim and start would take.
		d.logger.Error().Err(err).
			Str("func", "claimOnce").
			Int("tasks", len(claimed)).
			Msg("handler rejected claimed tasks; leaving them to lease expiry")
		return len(claimed), err
	}

	return len(claimed), nil
}

// claimLimit is the smaller of the configured batch size and what the node can
// actually start.
func (d *Dispatcher) claimLimit() int {
	free := d.capacity.FreeSlots()
	if free < 0 {
		free = 0
	}
	if d.batchSize < free {
		return d.batchSize
	}
	return free
}
