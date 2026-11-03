package main

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	listSample "github.com/mchandschuh/temporal-actors/go/samples/list"
	"github.com/mchandschuh/temporal-actors/go/src/actor"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The sample assumes temporal server start-dev is already running.
	temporalClient, err := client.DialContext(ctx, client.Options{
		HostPort:  "localhost:7233",
		Namespace: "default",
	})
	if err != nil {
		return err
	}
	defer temporalClient.Close()

	definition, err := listSample.NewDefinition()
	if err != nil {
		return err
	}
	identity, err := definition.NewIdentity("sample-cart")
	if err != nil {
		return err
	}
	route := definition.RouteResolver()
	options, err := actor.WorkflowOptionsWithResolver(identity, route)
	if err != nil {
		return err
	}

	worker := worker.New(temporalClient, options.TaskQueue, worker.Options{})
	worker.RegisterWorkflow(listSample.Workflow)
	if err := worker.Start(); err != nil {
		return err
	}
	defer worker.Stop()

	workflowRun, err := actor.StartWorkflowWithResolver(ctx, temporalClient, identity, route, listSample.Workflow, listSample.State{})
	if err != nil {
		return err
	}

	for index, message := range []listSample.Add{{Element: "alpha"}, {Element: "beta"}} {
		request, err := definition.EncodeMessage(message)
		if err != nil {
			return err
		}
		if _, err := actor.SendMailboxUpdate(ctx, temporalClient, identity, fmt.Sprintf("add-%d", index), request); err != nil {
			return err
		}
	}
	setRequest, err := definition.EncodeMessage(listSample.Set{Index: 0, Element: "ALPHA"})
	if err != nil {
		return err
	}
	if _, err := actor.SendMailboxUpdate(ctx, temporalClient, identity, "set-0", setRequest); err != nil {
		return err
	}
	removeRequest, err := definition.EncodeMessage(listSample.Remove{Element: "beta"})
	if err != nil {
		return err
	}
	if _, err := actor.SendMailboxUpdate(ctx, temporalClient, identity, "remove-beta", removeRequest); err != nil {
		return err
	}
	getRequest, err := definition.EncodeMessage(listSample.Get{})
	if err != nil {
		return err
	}
	response, err := actor.SendMailboxUpdate(ctx, temporalClient, identity, "get", getRequest)
	if err != nil {
		return err
	}
	var result listSample.State
	if err := response.DecodeResult(&result); err != nil {
		return err
	}
	if err := temporalClient.SignalWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID(), actor.StopSignalName, struct{}{}); err != nil {
		return err
	}
	var finalState listSample.State
	if err := workflowRun.Get(ctx, &finalState); err != nil {
		return err
	}
	fmt.Printf("list actor %s result: %#v (read: %#v)\n", identity.ID, finalState, result)
	return nil
}
