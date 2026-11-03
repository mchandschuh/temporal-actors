package actor

import (
	"context"
	"errors"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// StopSignalName is the reserved Signal used to finish an actor workflow and
// return its current state. It is separate from a handler's message Signal.
const StopSignalName = stopSignalName

// WorkflowOptions returns Temporal start options using the default route
// resolver.
func WorkflowOptions(identity ActorIdentity) (client.StartWorkflowOptions, error) {
	return WorkflowOptionsWithResolver(identity, DefaultRouteResolver())
}

// WorkflowOptionsWithResolver returns Temporal start options using resolver.
// The actor identity determines the Workflow ID; the resolver determines the
// Task Queue and fairness metadata.
func WorkflowOptionsWithResolver(identity ActorIdentity, resolver RouteResolver) (client.StartWorkflowOptions, error) {
	if err := identity.Validate(); err != nil {
		return client.StartWorkflowOptions{}, err
	}
	if resolver == nil {
		return client.StartWorkflowOptions{}, errors.New("actor route resolver must not be nil")
	}
	route, err := resolver.Resolve(identity)
	if err != nil {
		return client.StartWorkflowOptions{}, err
	}
	if err := route.Validate(); err != nil {
		return client.StartWorkflowOptions{}, err
	}
	return client.StartWorkflowOptions{
		ID:        identity.WorkflowID(),
		TaskQueue: route.TaskQueue,
		Priority: temporal.Priority{
			PriorityKey:    route.PriorityKey,
			FairnessKey:    route.FairnessKey,
			FairnessWeight: route.FairnessWeight,
		},
	}, nil
}

// StartWorkflow starts a workflow whose function accepts (initialState,
// ActorIdentity). The handler remains code registered in the workflow
// implementation; it is not serialized as workflow input.
func StartWorkflow[S any](
	ctx context.Context,
	c client.Client,
	identity ActorIdentity,
	workflowFn any,
	initialState S,
) (client.WorkflowRun, error) {
	return StartWorkflowWithResolver(ctx, c, identity, DefaultRouteResolver(), workflowFn, initialState)
}

// StartWorkflowWithResolver starts an actor workflow using resolver for its
// Task Queue and fairness metadata.
func StartWorkflowWithResolver[S any](
	ctx context.Context,
	c client.Client,
	identity ActorIdentity,
	resolver RouteResolver,
	workflowFn any,
	initialState S,
) (client.WorkflowRun, error) {
	options, err := WorkflowOptionsWithResolver(identity, resolver)
	if err != nil {
		return nil, err
	}
	return c.ExecuteWorkflow(ctx, options, workflowFn, initialState, identity)
}

// Run executes one typed actor handler until StopSignalName is received. The
// actor Context is created once and reused for every message. The explicit
// state value is the only actor state managed by this runtime.
func Run[S, M any](
	ctx workflow.Context,
	initialState S,
	identity ActorIdentity,
	h Handler[S, M],
) (S, error) {
	if err := identity.Validate(); err != nil {
		return initialState, err
	}
	if h.fn == nil {
		return initialState, errors.New("actor handler is not initialized")
	}

	actorContext := newContext(identity)
	messageChannel := workflow.GetSignalChannel(ctx, h.SignalName())
	stopChannel := workflow.GetSignalChannel(ctx, StopSignalName)

	var message M
	var stopped bool
	var received bool
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(stopChannel, func(c workflow.ReceiveChannel, _ bool) {
		var stop struct{}
		c.Receive(ctx, &stop)
		stopped = true
	})
	selector.AddReceive(messageChannel, func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, &message)
		received = true
	})

	state := initialState
	for {
		stopped = false
		received = false
		selector.Select(ctx)
		if stopped {
			// Stop is graceful for messages that are already buffered on this
			// handler's channel. Signals on different names do not expose a
			// shared receive queue, so the runtime cannot infer a total order
			// between the stop signal and another signal name here.
			for messageChannel.ReceiveAsync(&message) {
				state = h.Handle(actorContext, state, message)
			}
			return state, nil
		}
		if received {
			state = h.Handle(actorContext, state, message)
		}
	}
}
