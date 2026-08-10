package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/justinndidit/agentflow/internal/config"
	"github.com/rs/zerolog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type Migrator struct {
	migrations *config.Migrations
	pool       *pgxpool.Pool
	logger     *zerolog.Logger
}

func NewMigrator(migrationsConfig *config.Migrations, pool *pgxpool.Pool, logger *zerolog.Logger) (*Migrator, error) {
	if migrationsConfig == nil {
		logger.Error().Str("func", "NewMigrator").Msg("migrations config struct is nil")
		return nil, fmt.Errorf("migration config struct cannot be empty")
	}
	return &Migrator{
		migrations: migrationsConfig,
		pool:       pool,
		logger:     logger,
	}, nil
}

func (m *Migrator) Migrate(ctx context.Context) error {
	m.logger.Info().Str("func", "Migrate").Msg("starting db migration...")
	db := stdlib.OpenDBFromPool(m.pool)
	defer db.Close()

	// WithInstance checks out a dedicated connection and holds it for the driver's
	// lifetime — that is where golang-migrate takes its advisory lock. sql.DB.Close
	// does not force-close a connection that is still checked out, so the driver
	// must be closed explicitly or pgxpool.Close will block forever waiting for a
	// connection that is never returned.
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		m.logger.Error().Err(err).Str("func", "Migrate").Msg("failed to acquire database drive from db instance")
		return err
	}

	migrator, err := migrate.NewWithDatabaseInstance(m.migrations.MigrationsPath, "postgres", driver)
	if err != nil {
		m.logger.Error().Err(err).Str("func", "Migrate").Msg("failed to acquire migrate instance")
		// migrator does not exist yet, so the driver has to be released here.
		if closeErr := driver.Close(); closeErr != nil {
			m.logger.Error().Err(closeErr).Str("func", "Migrate").Msg("failed to close migration driver")
		}
		return err
	}

	// Close returns (sourceErr, databaseErr); the second is what hands the
	// connection back to the pool.
	defer func() {
		srcErr, dbErr := migrator.Close()
		if srcErr != nil {
			m.logger.Error().Err(srcErr).Str("func", "Migrate").Msg("failed to close migration source")
		}
		if dbErr != nil {
			m.logger.Error().Err(dbErr).Str("func", "Migrate").Msg("failed to close migration database driver")
		}
	}()

	// ErrNoChange means the schema is already at the latest version, which is the
	// normal case on every restart after the first. Treating it as a failure
	// makes a healthy node refuse to boot.
	err = migrator.Up()
	if err != nil {
		if !errors.Is(err, migrate.ErrNoChange) {
			m.logger.Error().Err(err).Str("func", "Migrate").Msg("failed to migrate database")
			return err
		}
		m.logger.Info().Str("func", "Migrate").Msg("no schema change")
	}
	m.logger.Info().Str("func", "Migrate").Msg("db migrated successfully")

	return nil
}
