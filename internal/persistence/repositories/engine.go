package repositories

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/state"
)

type EngineStore interface {
	Register(context.Context, *models.EngineRow) (*models.EngineRow, error)
	Heartbeat(context.Context, uuid.UUID) (time.Time, error)
	SetStatus(context.Context, uuid.UUID, state.EngineStatus) error
	GetEngineByID(context.Context, uuid.UUID) (*models.EngineRow, error)
	ListStale(context.Context, time.Duration) ([]*models.EngineRow, error)
	Now(context.Context) (time.Time, error)
}

type PostgresEngineStore struct {
	repo Repository
}

func NewPostgresEngineStore(repo Repository) *PostgresEngineStore {
	return &PostgresEngineStore{repo: repo}
}

// engineColumns is declared once and reused by every statement so the SELECT
// list and the scan order cannot drift apart.
const engineColumns = `id, hostname, status, capacity,
	started_at, heartbeat_at, created_at, updated_at`

// Register inserts this node's row on boot.
//
// status is cast explicitly because engine_status is an enum; without the cast
// Postgres cannot infer the parameter type from a bare string.
func (p *PostgresEngineStore) Register(ctx context.Context, engine *models.EngineRow) (*models.EngineRow, error) {
	stmt := `INSERT INTO engines (
		id, hostname, status, capacity, started_at, heartbeat_at, created_at, updated_at
	) VALUES (
		@id, @hostname, @status::engine_status, @capacity, @startedAt, @heartbeatAt, @createdAt, @updatedAt
	) RETURNING ` + engineColumns

	rows, err := p.repo.Query(ctx, stmt, pgx.NamedArgs{
		"id":          engine.ID,
		"hostname":    engine.HostName,
		"status":      engine.Status,
		"capacity":    engine.Capacity,
		"startedAt":   engine.StartedAt,
		"heartbeatAt": engine.HeartBeatAt,
		"createdAt":   engine.CreatedAt,
		"updatedAt":   engine.UpdatedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("register engine %s: %w", engine.ID, err)
	}

	registered, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[models.EngineRow])
	if err != nil {
		return nil, fmt.Errorf("collect registered engine %s: %w", engine.ID, err)
	}
	return registered, nil
}

// Heartbeat refreshes this node's liveness and returns the timestamp Postgres
// recorded.
//
// The time is taken from now() rather than supplied by the caller: every
// staleness decision in the system compares heartbeats against the database
// clock, so writing a node's own clock here would make liveness depend on how
// well the fleet's clocks agree.
func (p *PostgresEngineStore) Heartbeat(ctx context.Context, id uuid.UUID) (time.Time, error) {
	var beat time.Time

	err := p.repo.QueryRow(ctx,
		`UPDATE engines
		    SET heartbeat_at = now(), updated_at = now()
		  WHERE id = $1
		RETURNING heartbeat_at`, id).Scan(&beat)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, fmt.Errorf("heartbeat engine %s: %w", id, ErrNotFound)
		}
		return time.Time{}, fmt.Errorf("heartbeat engine %s: %w", id, err)
	}
	return beat, nil
}

// SetStatus moves a node between active, draining and stopped.
func (p *PostgresEngineStore) SetStatus(ctx context.Context, id uuid.UUID, status state.EngineStatus) error {
	tag, err := p.repo.Exec(ctx,
		`UPDATE engines SET status = $2::engine_status, updated_at = now() WHERE id = $1`,
		id, string(status))
	if err != nil {
		return fmt.Errorf("set engine %s status to %s: %w", id, status, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("set engine %s status to %s: %w", id, status, ErrNotFound)
	}
	return nil
}

func (p *PostgresEngineStore) GetEngineByID(ctx context.Context, id uuid.UUID) (*models.EngineRow, error) {
	rows, err := p.repo.Query(ctx,
		`SELECT `+engineColumns+` FROM engines WHERE id = @id`,
		pgx.NamedArgs{"id": id})
	if err != nil {
		return nil, fmt.Errorf("get engine %s: %w", id, err)
	}

	engine, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[models.EngineRow])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get engine %s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("collect engine %s: %w", id, err)
	}
	return engine, nil
}

// ListStale returns engines whose heartbeat is older than ttl — the nodes the
// reaper should reclaim work from.
//
// The cutoff is computed as now() - ttl inside the statement so it uses the
// database clock. Passing a timestamp calculated on the caller's node would let
// one machine with a fast clock declare the rest of a healthy fleet dead.
//
// Stopped engines are included: a node that shut down cleanly may still own rows
// if it was killed between setting its status and finishing its work, and those
// are reclaimable immediately rather than after a timeout.
func (p *PostgresEngineStore) ListStale(ctx context.Context, ttl time.Duration) ([]*models.EngineRow, error) {
	rows, err := p.repo.Query(ctx,
		`SELECT `+engineColumns+`
		   FROM engines
		  WHERE heartbeat_at < now() - @ttl::interval
		  ORDER BY heartbeat_at`,
		pgx.NamedArgs{"ttl": durationToInterval(ttl)})
	if err != nil {
		return nil, fmt.Errorf("list stale engines: %w", err)
	}

	stale, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[models.EngineRow])
	if err != nil {
		return nil, fmt.Errorf("collect stale engines: %w", err)
	}
	return stale, nil
}

// Now returns the database's clock. Every liveness and lease comparison is made
// against it rather than against any node's local time.
func (p *PostgresEngineStore) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := p.repo.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database time: %w", err)
	}
	return now, nil
}
