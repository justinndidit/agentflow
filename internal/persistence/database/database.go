// Package database
// connects to postgres database
package database

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/justinndidit/agentflow/internal/config"
	"github.com/rs/zerolog"
)

type Database interface {
	Open(context.Context) error
	Close(context.Context) error
}

type PostgresDatabase struct {
	config *config.Database
	logger *zerolog.Logger
	Pool   *pgxpool.Pool
}

func NewPostgresDatabase(config *config.Database, logger *zerolog.Logger) *PostgresDatabase {
	return &PostgresDatabase{
		config: config,
		logger: logger,
	}
}

func (d *PostgresDatabase) Open(ctx context.Context) error {
	pool, err := initializeDB(ctx, d.config)
	if err != nil {
		d.logger.Error().Err(err).Str("func", "InitializeDB").Msg("failed to create db connection pool")
		return err
	}
	d.Pool = pool
	return nil
}

func (d *PostgresDatabase) Close(ctx context.Context) error {
	if d.Pool == nil {
		d.logger.Warn().Str("func", "Close").Msg("database already closed...")
		return nil
	}

	d.Pool.Close()
	d.Pool = nil
	return nil
}

func initializeDB(ctx context.Context, cfg *config.Database) (*pgxpool.Pool, error) {

	hostPort := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	encodePassword := url.QueryEscape(cfg.Password)
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
		cfg.User,
		encodePassword,
		hostPort,
		cfg.Name,
		cfg.SSLMode,
	)

	pgxPoolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database DSN: %w", err)
	}

	pgxPoolConfig.MaxConns = int32(cfg.MaxOpenConns)
	pgxPoolConfig.MinConns = int32(cfg.MaxIdleConns)
	pgxPoolConfig.MaxConnLifetime = time.Duration(cfg.ConnMaxIdleTime) * time.Second
	pgxPoolConfig.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, pgxPoolConfig)
	if err != nil {
		return nil, err
	}
	return pool, nil
}
