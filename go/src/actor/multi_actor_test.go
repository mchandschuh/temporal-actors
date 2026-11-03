package actor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type multiActorAdd struct {
	Value int
}

type multiActorGet struct{}

type multiActorState struct {
	Values []string
}

func TestDefinitionDispatchesManyActorInstancesInterleaved(t *testing.T) {
	const (
		actorCount       = 32
		messagesPerActor = 128
	)

	definition, err := NewDefinition(
		DefinitionOptions{TypeName: "runtime-multi-actor"},
		Command(func(ctx Context, state multiActorState, message multiActorAdd) multiActorState {
			state.Values = append(state.Values, fmt.Sprintf("%s:%d", ctx.Identity().ID, message.Value))
			return state
		}),
		Request(func(_ Context, state multiActorState, _ multiActorGet) ([]string, error) {
			values := append([]string(nil), state.Values...)
			return values, nil
		}),
	)
	require.NoError(t, err)

	type actorInstance struct {
		identity ActorIdentity
		context  Context
		state    multiActorState
		expected []string
	}
	instances := make([]actorInstance, actorCount)
	var sharedTaskQueue string
	fairnessKeys := make(map[string]struct{}, actorCount)
	for index := range instances {
		identity, identityErr := definition.NewIdentity(fmt.Sprintf("actor-%02d", index))
		require.NoError(t, identityErr)
		route, routeErr := definition.RouteResolver().Resolve(identity)
		require.NoError(t, routeErr)
		if index == 0 {
			sharedTaskQueue = route.TaskQueue
		} else {
			require.Equal(t, sharedTaskQueue, route.TaskQueue)
		}
		if _, exists := fairnessKeys[route.FairnessKey]; exists {
			t.Fatalf("duplicate fairness key for %s", identity.ID)
		}
		fairnessKeys[route.FairnessKey] = struct{}{}
		instances[index] = actorInstance{
			identity: identity,
			context:  newContext(identity),
		}
	}

	for messageIndex := 0; messageIndex < messagesPerActor; messageIndex++ {
		for actorIndex := range instances {
			instance := &instances[actorIndex]
			result, dispatchErr := definition.Dispatch(
				instance.context,
				instance.state,
				multiActorAdd{Value: messageIndex},
			)
			require.NoError(t, dispatchErr)
			require.Equal(t, CommandHandler, result.Kind)
			instance.state = result.State
			instance.expected = append(instance.expected, fmt.Sprintf("%s:%d", instance.identity.ID, messageIndex))

			if messageIndex%16 != 0 && messageIndex != messagesPerActor-1 {
				continue
			}
			read, readErr := definition.Dispatch(instance.context, instance.state, multiActorGet{})
			require.NoError(t, readErr)
			require.Equal(t, RequestHandler, read.Kind)
			require.Equal(t, instance.state, read.State)
			require.Equal(t, instance.expected, read.Result)
		}
	}

	require.Len(t, fairnessKeys, actorCount)
	for _, instance := range instances {
		require.Len(t, instance.state.Values, messagesPerActor)
		require.Equal(t, instance.expected, instance.state.Values)
	}
}
