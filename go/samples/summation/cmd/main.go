package main

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/mchandschuh/temporal-actors/go/samples/summation"
	"github.com/mchandschuh/temporal-actors/go/src/actor"
)

const (
	temporalHostPort  = "localhost:7233"
	temporalNamespace = "default"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The sample intentionally connects to the local Temporal server-dev
	// service. Start it separately with: temporal server start-dev
	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort:  temporalHostPort,
		Namespace: temporalNamespace,
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal at %s: %w", temporalHostPort, err)
	}
	defer temporalClient.Close()

	definition, err := summation.NewDefinition()
	if err != nil {
		return fmt.Errorf("build summation definition: %w", err)
	}
	identity, err := definition.NewIdentity("sample-account")
	if err != nil {
		return fmt.Errorf("build summation identity: %w", err)
	}
	routeResolver := definition.RouteResolver()
	workflowOptions, err := actor.WorkflowOptionsWithResolver(identity, routeResolver)
	if err != nil {
		return fmt.Errorf("build actor workflow options: %w", err)
	}

	worker := worker.New(temporalClient, workflowOptions.TaskQueue, worker.Options{})
	worker.RegisterWorkflow(summation.Workflow)
	if err := worker.Start(); err != nil {
		return fmt.Errorf("start Temporal worker: %w", err)
	}
	defer worker.Stop()

	workflowRun, err := actor.StartWorkflowWithResolver(ctx, temporalClient, identity, routeResolver, summation.Workflow, 0)
	if err != nil {
		return fmt.Errorf("start summation workflow: %w", err)
	}

	for _, message := range []summation.Add{
		{Value: 1},
		{Value: 2},
		{Value: 5},
	} {
		if err := temporalClient.SignalWorkflow(
			ctx,
			workflowRun.GetID(),
			workflowRun.GetRunID(),
			summation.AddSignalName,
			message,
		); err != nil {
			return fmt.Errorf("send Add signal: %w", err)
		}
	}

	if err := temporalClient.SignalWorkflow(
		ctx,
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		actor.StopSignalName,
		struct{}{},
	); err != nil {
		return fmt.Errorf("stop summation workflow: %w", err)
	}

	var result int
	if err := workflowRun.Get(ctx, &result); err != nil {
		return fmt.Errorf("get summation workflow result: %w", err)
	}

	fmt.Printf("summation actor %s result: %d\n", identity.ID, result)
	return nil
}
