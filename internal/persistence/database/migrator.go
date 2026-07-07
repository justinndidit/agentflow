package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type Migrator struct {
	dbUrl          string
	migrationsPath string
	pool           *pgxpool.Pool
	logger         *zerolog.Logger
}

func NewMigrator(dbUrl, path string, pool *pgxpool.Pool, logger *zerolog.Logger) (*Migrator, error) {
	if dbUrl == "" {
		logger.Error().Str("func", "NewMigrator").Msg("empty database connection string")
		return nil, fmt.Errorf("database connection string cannot be empty")
	}

	return &Migrator{
		dbUrl:          dbUrl,
		migrationsPath: path,
		pool:           pool,
		logger:         logger,
	}, nil
}

func (m *Migrator) Migrate(ctx context.Context) error {
	db := stdlib.OpenDBFromPool(m.pool)

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		m.logger.Error().Err(err).Str("func", "Migrate").Msg("failed to acquire database drive from db instance")
		return err
	}

	migrator, err := migrate.NewWithDatabaseInstance(m.migrationsPath, "postgres", driver)
	if err != nil {
		m.logger.Error().Err(err).Str("func", "Migrate").Msg("failed to acquire migrate instance")
		return err
	}

	err = migrator.Up()
	if err != nil {
		m.logger.Error().Err(err).Str("func", "Migrate").Msg("failed to migrate database")
		return err
	}

	return nil
}
