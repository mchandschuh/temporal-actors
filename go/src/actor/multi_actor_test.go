package actor

import (
	"fmt"
	"math/rand"
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
			values := append([]string(nil), state.Values...)
			values = append(values, fmt.Sprintf("%s:%d", ctx.Identity().ID, message.Value))
			return multiActorState{Values: values}
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

func TestDefinitionPreservesPerActorMessageOrderUnderRandomInterleaving(t *testing.T) {
	const (
		actorCount       = 32
		messagesPerActor = 128
	)

	definition, err := NewDefinition(
		DefinitionOptions{TypeName: "runtime-randomized-actor"},
		Command(func(_ Context, state multiActorState, message multiActorAdd) multiActorState {
			values := append([]string(nil), state.Values...)
			values = append(values, fmt.Sprintf("%d", message.Value))
			return multiActorState{Values: values}
		}),
		Request(func(_ Context, state multiActorState, _ multiActorGet) ([]string, error) {
			return append([]string(nil), state.Values...), nil
		}),
	)
	require.NoError(t, err)

	identities := make([]ActorIdentity, actorCount)
	contexts := make([]Context, actorCount)
	states := make([]multiActorState, actorCount)
	expected := make([][]string, actorCount)
	for index := 0; index < actorCount; index++ {
		identity, identityErr := definition.NewIdentity(fmt.Sprintf("actor-%02d", index))
		require.NoError(t, identityErr)
		identities[index] = identity
		contexts[index] = newContext(identity)
	}

	rng := rand.New(rand.NewSource(0x5eed))
	remaining := make([]int, actorCount)
	for index := range remaining {
		remaining[index] = messagesPerActor
	}
	remainingMessages := actorCount * messagesPerActor
	for remainingMessages > 0 {
		actorIndex := rng.Intn(actorCount)
		if remaining[actorIndex] == 0 {
			continue
		}

		sequence := messagesPerActor - remaining[actorIndex]
		result, dispatchErr := definition.Dispatch(
			contexts[actorIndex],
			states[actorIndex],
			multiActorAdd{Value: sequence},
		)
		require.NoError(t, dispatchErr)
		require.Equal(t, CommandHandler, result.Kind)
		states[actorIndex] = result.State
		expected[actorIndex] = append(expected[actorIndex], fmt.Sprintf("%d", sequence))
		remaining[actorIndex]--
		remainingMessages--

		if rng.Intn(5) != 0 {
			continue
		}
		read, readErr := definition.Dispatch(contexts[actorIndex], states[actorIndex], multiActorGet{})
		require.NoError(t, readErr)
		require.Equal(t, RequestHandler, read.Kind)
		require.Equal(t, states[actorIndex], read.State)
		require.Equal(t, expected[actorIndex], read.Result)
	}

	for actorIndex := range states {
		read, readErr := definition.Dispatch(contexts[actorIndex], states[actorIndex], multiActorGet{})
		require.NoError(t, readErr)
		require.Equal(t, expected[actorIndex], read.Result)
		require.Len(t, states[actorIndex].Values, messagesPerActor)
	}
}

func TestDefinitionDispatchDoesNotDependOnRegistrationOrder(t *testing.T) {
	command := Command(func(Context, int, definitionMessage) int { return 1 })
	request := Request(func(_ Context, state int, _ struct{}) (int, error) { return state, nil })

	first, err := NewDefinition(DefinitionOptions{TypeName: "registration-order-a"}, command, request)
	require.NoError(t, err)
	second, err := NewDefinition(DefinitionOptions{TypeName: "registration-order-b"}, request, command)
	require.NoError(t, err)

	firstCommand, err := first.Dispatch(Context{}, 0, definitionMessage{})
	require.NoError(t, err)
	secondCommand, err := second.Dispatch(Context{}, 0, definitionMessage{})
	require.NoError(t, err)
	require.Equal(t, firstCommand.State, secondCommand.State)

	firstRequest, err := first.Dispatch(Context{}, 9, struct{}{})
	require.NoError(t, err)
	secondRequest, err := second.Dispatch(Context{}, 9, struct{}{})
	require.NoError(t, err)
	require.Equal(t, firstRequest.Result, secondRequest.Result)
}
