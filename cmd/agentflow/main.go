package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/justinndidit/agentflow/internal/config"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/database"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/rs/zerolog"
)

func main() {

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
	var manifestFile string
	var seedDevData bool
	flag.StringVar(&manifestFile, "manifest", "example-workflow.yml", "workflow manifest location")
	flag.BoolVar(&seedDevData, "seed", false, "insert the development agents the example manifest refers to")
	flag.Parse()

	cfg, err := config.LoadConfig(&logger)
	if err != nil {
		logger.Error().Err(err).Str("func", "main").Msg("failed to parse config")
		return
	}

	startContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db := database.NewPostgresDatabase(cfg.Database, &logger)
	err = db.Open(startContext)
	if err != nil {
		logger.Error().Err(err).Str("func", "main").Msg("failed to open db connection")
		return
	}
	defer func() {
		if err := db.Close(startContext); err != nil {
			logger.Error().Err(err).Str("func", "main").Msg("error on db close")
			return
		}
	}()

	dbMigrator, err := database.NewMigrator(cfg.Migrations, db.Pool, &logger)
	if err != nil {
		logger.Error().Err(err).Str("func", "main").Msg("failed to create db migrator")
		return
	}
	err = dbMigrator.Migrate(startContext)
	if err != nil {
		logger.Error().Err(err).Str("func", "main").Msg("failed to migrate to database")
		return
	}

	// Must come after Migrate and before SubmitManifest: tasks.agent_name is a
	// foreign key to agents(name), so submitting a manifest against an unseeded
	// database fails the whole bulk insert.
	if seedDevData {
		err = database.SeedDevAgents(startContext, db.Pool, &logger)
		if err != nil {
			logger.Error().Err(err).Str("func", "main").Msg("failed to seed development agents")
			return
		}
	}

	txManager := repositories.NewTxManager(db.Pool, &logger)
	manifestProcessor := engine.NewManifestProcessor(&logger, txManager)

	workflow, err := manifestProcessor.SubmitManifest(startContext, manifestFile)
	if err != nil {
		fmt.Printf("error parsing manifest file: %s\n", err)
		return
	}

	timeout := time.Duration(workflow.DefaultTimeout)

	_, cancel := context.WithTimeout(startContext, timeout)
	defer cancel()

	// executor := engine.NewExecutor(workflow.DefaultWorkerCount, 10)
	//TODO: update Run() to only accept context then pull tasks from db
	// if err := executor.Run(ctx, nil); err != nil {
	// 	fmt.Printf("workflow failed: %s\n", err)
	// 	return
	// }

	fmt.Println("workflow submitted successfully!")
}
