package repositories

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

type Repository interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
}

type PostgresRepository struct {
	pool   *pgxpool.Pool
	logger *zerolog.Logger
	tx     pgx.Tx
}

type TxManager struct {
	db     *pgxpool.Pool
	logger *zerolog.Logger
}

type Stores struct {
	TaskStore     TaskStore
	WorkflowStore WorkflowStore
}

func NewStore(repo Repository) *Stores {
	taskStore := NewPostgresTaskStore(repo)
	workflowStore := NewPostgresWorkflowStore(repo)
	return &Stores{
		TaskStore:     taskStore,
		WorkflowStore: workflowStore,
	}
}

func NewPostgresRepository(pool *pgxpool.Pool, logger *zerolog.Logger, tx pgx.Tx) *PostgresRepository {
	return &PostgresRepository{
		pool:   pool,
		logger: logger,
		tx:     tx,
	}
}

func (p *PostgresRepository) Exec(ctx context.Context, stmt string, args ...any) (pgconn.CommandTag, error) {
	if p.tx != nil {
		return p.tx.Exec(ctx, stmt, args...)
	}
	return p.pool.Exec(ctx, stmt, args...)
}

func (p *PostgresRepository) Query(ctx context.Context, stmt string, args ...any) (pgx.Rows, error) {
	if p.tx != nil {
		return p.tx.Query(ctx, stmt, args...)
	}
	return p.pool.Query(ctx, stmt, args...)
}

func (p *PostgresRepository) QueryRow(ctx context.Context, stmt string, args ...any) pgx.Row {
	if p.tx != nil {
		return p.tx.QueryRow(ctx, stmt, args...)
	}
	return p.pool.QueryRow(ctx, stmt, args...)
}

func (p *PostgresRepository) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if p.tx != nil {
		return p.tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
	}
	return p.pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func NewTxManager(db *pgxpool.Pool, logger *zerolog.Logger) *TxManager {
	return &TxManager{
		db:     db,
		logger: logger,
	}
}

func (t *TxManager) WithTransaction(ctx context.Context, fn func(ctx context.Context, stores *Stores) error) error {
	tx, err := t.db.Begin(ctx)
	if err != nil {
		t.logger.Error().Err(err).Str("func", "WithTransaction").Msg("failed to start db transaction")
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				t.logger.Error().Err(rbErr).Str("func", "WithTransaction").Msg("failed to rollback transaction")
			}
			panic(p)
		}
	}()

	repo := NewPostgresRepository(t.db, t.logger, tx)
	stores := NewStore(repo)

	// The rollback result goes into its own variable. Assigning it to err would
	// replace the reason the transaction failed with the outcome of cleaning up
	// after it — and a successful rollback returns nil, turning a failed
	// transaction into a silent success.
	if err = fn(ctx, stores); err != nil {
		t.logger.Error().Err(err).Str("func", "WithTransaction").Msg("failed to execute transaction")
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			t.logger.Error().Err(rbErr).Str("func", "WithTransaction").Msg("failed to rollback transaction")
		}
		return err
	}

	return tx.Commit(ctx)
}
