package engine

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// drainTimeout bounds how long a shutting-down node waits for in-flight work.
// Anything still running when it expires is abandoned to lease expiry, which is
// the same path a crash takes.
const drainTimeout = 30 * time.Second

// Node is one engine process: registrar, dispatcher, worker pool, committer and
// reaper, all against one database.
//
// Nodes never talk to each other. Every node runs the same loops, there is no
// leader and no election, and all coordination is coordination through Postgres.
// Losing a node loses no work — its leases expire and another node picks the
// tasks up.
type Node struct {
	cfg    *config.Config
	pool   *pgxpool.Pool
	logger *zerolog.Logger

	registrar  *Registrar
	dispatcher *Dispatcher
	workers    *Pool
	reaper     *Reaper
	listener   *Listener
}

// NewNode assembles a node. The database pool is supplied already open so the
// caller controls its lifetime, and migrations have run before this point.
func NewNode(
	cfg *config.Config,
	pool *pgxpool.Pool,
	rt runtime.Runtime,
	logger *zerolog.Logger,
) *Node {
	txManager := repositories.NewTxManager(pool, logger)
	stores := repositories.NewStore(repositories.NewPostgresRepository(pool, logger, nil))

	committer := NewCommitter(txManager, logger, DefaultBackoff)
	leaseTTL := time.Duration(cfg.Engine.LeaseTTL) * time.Second

	return &Node{
		cfg:       cfg,
		pool:      pool,
		logger:    logger,
		registrar: NewRegistrar(stores.EngineStore, cfg.Engine, logger),
		workers: NewPool(cfg.Engine.Capacity, rt, committer,
			NewTemplateResolver(stores.TaskResultStore), leaseTTL, logger),
		reaper: NewReaper(txManager, cfg.Engine, DefaultBackoff, logger,
			WithReapInterval(time.Duration(cfg.Engine.ReapInterval)*time.Second)),
		listener: NewListener(cfg.Database.DSN(), ReadyChannel, logger),
	}
}

// Run registers this node and blocks until ctx is cancelled, then shuts down
// gracefully.
//
// The shutdown order matters and is the reverse of the startup order: stop
// claiming, let in-flight work finish, then release the node. Doing it the other
// way round — marking the node stopped first — would make the reaper reclaim
// tasks this node is still running, and the same work would execute twice.
func (n *Node) Run(ctx context.Context) error {
	registered, err := n.registrar.Register(ctx)
	if err != nil {
		return err
	}

	stores := repositories.NewStore(repositories.NewPostgresRepository(n.pool, n.logger, nil))
	wake := make(chan struct{}, 1)

	n.dispatcher = NewDispatcher(
		stores.TaskStore,
		registered.ID,
		n.workers,
		n.workers,
		n.cfg.Engine,
		n.logger,
		WithWakeup(wake),
		WithPollInterval(time.Duration(n.cfg.Engine.PollInterval)*time.Second),
	)

	n.logger.Info().
		Str("func", "Run").
		Str("engine_id", registered.ID.String()).
		Int("capacity", n.cfg.Engine.Capacity).
		Msg("engine node started")

	// Loops run until the shared context is cancelled. Each returns nil on
	// cancellation, so the group only reports genuine failures.
	group, loopCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return n.registrar.Run(loopCtx) })
	group.Go(func() error { return n.dispatcher.Run(loopCtx) })
	group.Go(func() error { return n.reaper.Run(loopCtx) })
	group.Go(func() error { return n.listener.Run(loopCtx, wake) })

	runErr := group.Wait()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		n.logger.Error().Err(runErr).Str("func", "Run").Msg("engine loop failed")
	}

	return errors.Join(runErr, n.shutdown())
}

// shutdown drains in-flight work and releases the node.
//
// It runs on a fresh context because the one that triggered the shutdown is
// already cancelled, and every step here is a database write that would fail
// immediately if it inherited that.
func (n *Node) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout+10*time.Second)
	defer cancel()

	// Draining first: the reaper leaves a draining node's tasks alone as long as
	// it keeps heartbeating, and the heartbeat has already stopped, so this is
	// bounded by the lease TTL rather than being safe indefinitely.
	if err := n.registrar.Drain(ctx); err != nil {
		n.logger.Error().Err(err).Str("func", "shutdown").Msg("failed to mark node draining")
	}

	drainCtx, drainCancel := context.WithTimeout(ctx, drainTimeout)
	defer drainCancel()
	if err := n.workers.Drain(drainCtx); err != nil {
		n.logger.Warn().Err(err).Str("func", "shutdown").Msg("pool did not drain cleanly")
	}

	// Only now: a stopped node's remaining tasks are reclaimable immediately,
	// so this must not happen while any are still running.
	if err := n.registrar.Stop(ctx); err != nil {
		n.logger.Error().Err(err).Str("func", "shutdown").Msg("failed to mark node stopped")
		return err
	}

	n.logger.Info().Str("func", "shutdown").Msg("engine node stopped")
	return nil
}
