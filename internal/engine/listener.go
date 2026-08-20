package engine

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
)

// listenerRetryDelay is how long to wait before reconnecting a dropped LISTEN
// connection. Losing it is not an outage — the dispatcher's poll interval keeps
// work moving, just less promptly — so reconnection is unhurried.
const listenerRetryDelay = 2 * time.Second

// Listener turns Postgres notifications into wake-ups for a dispatcher.
//
// It holds its own direct connection rather than borrowing from the pool, for a
// reason that is easy to miss and expensive to retrofit: LISTEN is session
// state, and a transaction-mode connection pooler hands a different backend to
// each transaction. Routing LISTEN through a pooler produces a subscription that
// silently never fires. Designing around it now costs one connection per node;
// discovering it later means reworking the dispatcher.
//
// Notifications are an optimisation, never a correctness requirement. A missed
// wake-up costs latency until the next poll tick, so this loop is free to drop
// a connection, reconnect, and lose whatever arrived in between.
type Listener struct {
	dsn     string
	channel string
	logger  *zerolog.Logger
}

func NewListener(dsn, channel string, logger *zerolog.Logger) *Listener {
	return &Listener{dsn: dsn, channel: channel, logger: logger}
}

// Run listens until ctx is cancelled, sending a wake-up on the returned channel
// for every notification.
//
// Sends are non-blocking against a buffered channel: the wake-up means "there
// may be work", so a second one while the first is still pending adds nothing.
// Dropping it keeps a burst of completions from stalling this loop.
func (l *Listener) Run(ctx context.Context, wake chan<- struct{}) error {
	for {
		if err := l.listen(ctx, wake); err != nil {
			if ctx.Err() != nil {
				l.logger.Info().Str("func", "Run").Msg("listener stopped")
				return nil
			}
			l.logger.Warn().Err(err).
				Str("func", "Run").
				Dur("retry_in", listenerRetryDelay).
				Msg("listener connection lost; the poll interval covers the gap")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(listenerRetryDelay):
		}
	}
}

func (l *Listener) listen(ctx context.Context, wake chan<- struct{}) error {
	conn, err := pgx.Connect(ctx, l.dsn)
	if err != nil {
		return err
	}
	defer func() {
		// Its own context: the caller's is usually already cancelled by the time
		// this runs, which would abort the close and leak the connection.
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize()); err != nil {
		return err
	}

	l.logger.Info().
		Str("func", "listen").
		Str("channel", l.channel).
		Msg("listening for ready notifications")

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}

		l.logger.Debug().
			Str("func", "listen").
			Str("channel", notification.Channel).
			Str("payload", notification.Payload).
			Msg("ready notification")

		select {
		case wake <- struct{}{}:
		default:
			// A wake-up is already pending; a second says nothing new.
		}
	}
}
