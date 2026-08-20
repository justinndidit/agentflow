package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/state"
	"github.com/rs/zerolog"
)

// Registrar owns this node's row in the engines table: it inserts one on boot,
// refreshes heartbeat_at on a tick, and marks the node stopped on the way out.
//
// Liveness for every task this node holds is derived from that single row. The
// alternative — heartbeating each running task — costs one write per running
// task per interval on the hottest table in the system, so the cost of knowing a
// node is alive would scale with how much work it is doing.
type Registrar struct {
	store  repositories.EngineStore
	logger *zerolog.Logger
	cfg    *config.Engine

	// engine is this node's registration record. It is written once by Register
	// and never mutated again: Run executes in its own goroutine while Drain and
	// Stop are called from the shutdown path, and the fields either of them
	// might have updated — heartbeat_at, status — are authoritative in Postgres,
	// not here. Keeping no mutable shared state removes the question entirely.
	engine *models.EngineRow
}

func NewRegistrar(store repositories.EngineStore, cfg *config.Engine, logger *zerolog.Logger) *Registrar {
	return &Registrar{
		store:  store,
		logger: logger,
		cfg:    cfg,
	}
}

// Register claims this node's identity. It must succeed before anything is
// claimed: tasks.engine_id is a foreign key to engines(id), so a claim from an
// unregistered node fails at the database.
func (r *Registrar) Register(ctx context.Context) (*models.EngineRow, error) {
	hostname, err := os.Hostname()
	if err != nil {
		// A node without a resolvable hostname can still work; the name is for
		// operators reading the table, not for correctness.
		hostname = "unknown"
		r.logger.Warn().Err(err).Str("func", "Register").Msg("could not resolve hostname")
	}

	row := models.NewEngineRow(hostname, r.cfg.Capacity)
	registered, err := r.store.Register(ctx, &row)
	if err != nil {
		r.logger.Error().Err(err).Str("func", "Register").Msg("failed to register engine")
		return nil, err
	}
	r.engine = registered

	r.logger.Info().
		Str("func", "Register").
		Str("engine_id", registered.ID.String()).
		Str("hostname", registered.HostName).
		Int("capacity", registered.Capacity).
		Msg("engine registered")

	// A copy, not the row the registrar keeps: Run mutates its own copy on
	// every heartbeat from a background goroutine, and handing the caller a
	// pointer into that would make a value they hold change underneath them —
	// and race with the goroutine while doing it.
	snapshot := *registered
	return &snapshot, nil
}

// ID is this node's engine id, or uuid.Nil before Register has run.
func (r *Registrar) ID() uuid.UUID {
	if r.engine == nil {
		return uuid.Nil
	}
	return r.engine.ID
}

// Run heartbeats until ctx is cancelled, then returns nil. Shutdown is the
// caller's job — see Drain and Stop — because the order matters and only the
// caller knows when in-flight work has finished.
//
// A failed heartbeat is logged and retried on the next tick rather than being
// fatal. A transient database blip should not take a node down; if the outage
// outlasts the lease TTL the reaper reclaims this node's work, which is the
// designed behaviour rather than a failure of it.
func (r *Registrar) Run(ctx context.Context) error {
	if r.engine == nil {
		return errors.New("registrar: Run called before Register")
	}

	interval := time.Duration(r.cfg.HeartbeatInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r.logger.Info().
		Str("func", "Run").
		Str("engine_id", r.engine.ID.String()).
		Dur("interval", interval).
		Msg("heartbeat started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Info().Str("func", "Run").Msg("heartbeat stopped")
			return nil

		case <-ticker.C:
			// Deliberately not ctx: a cancelled context would fail the very
			// heartbeat that keeps this node's work from being reclaimed while
			// it drains. The bounded timeout keeps a wedged connection from
			// blocking the loop.
			beatCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), interval)
			beat, err := r.store.Heartbeat(beatCtx, r.engine.ID)
			cancel()

			if err != nil {
				r.logger.Error().Err(err).
					Str("func", "Run").
					Str("engine_id", r.engine.ID.String()).
					Msg("heartbeat failed, retrying next tick")
				continue
			}

			r.logger.Debug().
				Str("func", "Run").
				Str("engine_id", r.engine.ID.String()).
				Time("heartbeat_at", beat).
				Msg("heartbeat")
		}
	}
}

// Drain stops this node claiming new work while it finishes what it holds. The
// reaper leaves a draining node's tasks alone as long as it keeps heartbeating,
// so the heartbeat has to outlive this call.
func (r *Registrar) Drain(ctx context.Context) error {
	return r.setStatus(ctx, state.DrainingEngineStatus)
}

// Stop marks this node as cleanly shut down. Anything it still holds becomes
// reclaimable immediately rather than after the lease expires, so it must be
// called only once in-flight work is finished or abandoned.
func (r *Registrar) Stop(ctx context.Context) error {
	return r.setStatus(ctx, state.StoppedEngineStatus)
}

func (r *Registrar) setStatus(ctx context.Context, status state.EngineStatus) error {
	if r.engine == nil {
		return fmt.Errorf("registrar: cannot set status %s before Register", status)
	}

	if err := r.store.SetStatus(ctx, r.engine.ID, status); err != nil {
		r.logger.Error().Err(err).
			Str("func", "setStatus").
			Str("engine_id", r.engine.ID.String()).
			Str("status", string(status)).
			Msg("failed to update engine status")
		return err
	}

	r.logger.Info().
		Str("func", "setStatus").
		Str("engine_id", r.engine.ID.String()).
		Str("status", string(status)).
		Msg("engine status updated")

	return nil
}
