package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/manifest"
)

func main() {
	var manifestFile string
	flag.StringVar(&manifestFile, "manifest", "example-workflow.yml", "workflow manifest location")
	flag.Parse()
	workflow, err := manifest.Parse(manifestFile)
	if err != nil {
		fmt.Printf("error parsing manifest file: %s\n", err)
		return
	}

	timeout, err := time.ParseDuration(workflow.DefaultTimeout)
	if err != nil {
		fmt.Printf("error parsing default timeout: %s\n", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	executor := engine.NewExecutor(workflow.DefaultWorkerCount, 10)
	if err := executor.Run(ctx, workflow.ToTasks()); err != nil {
		fmt.Printf("workflow failed: %s\n", err)
		return
	}

	fmt.Println("workflow completed successfully!")
}
