package actor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTypeMailboxResolverSharesQueueByActorType(t *testing.T) {
	resolver := TypeMailboxResolver{}
	first := ActorIdentity{Type: "list", ID: "cart-1"}
	second := ActorIdentity{Type: "list", ID: "cart-2"}
	otherType := ActorIdentity{Type: "summation", ID: "cart-1"}

	firstRoute, err := resolver.Resolve(first)
	require.NoError(t, err)
	secondRoute, err := resolver.Resolve(second)
	require.NoError(t, err)
	otherTypeRoute, err := resolver.Resolve(otherType)
	require.NoError(t, err)

	require.Equal(t, firstRoute.TaskQueue, secondRoute.TaskQueue)
	require.NotEqual(t, firstRoute.FairnessKey, secondRoute.FairnessKey)
	require.NotEqual(t, firstRoute.TaskQueue, otherTypeRoute.TaskQueue)
	require.NotEqual(t, first.WorkflowID(), second.WorkflowID())
	require.Equal(t, float32(1), firstRoute.FairnessWeight)
	require.NoError(t, firstRoute.Validate())
}

func TestPerActorResolverUsesDistinctQueues(t *testing.T) {
	resolver := PerActorResolver{}
	first := ActorIdentity{Type: "list", ID: "cart-1"}
	second := ActorIdentity{Type: "list", ID: "cart-2"}

	firstRoute, err := resolver.Resolve(first)
	require.NoError(t, err)
	secondRoute, err := resolver.Resolve(second)
	require.NoError(t, err)

	require.NotEqual(t, firstRoute.TaskQueue, secondRoute.TaskQueue)
	require.NotEqual(t, firstRoute.FairnessKey, secondRoute.FairnessKey)
	if !strings.HasPrefix(firstRoute.TaskQueue, legacyQueuePrefix+"/") {
		t.Fatalf("unexpected legacy queue %q", firstRoute.TaskQueue)
	}
}

func TestFairnessKeyIsStableAndBounded(t *testing.T) {
	identity := ActorIdentity{
		Type: strings.Repeat("type-", 30),
		ID:   strings.Repeat("id-", 30),
	}
	first, err := TypeMailboxResolver{}.Resolve(identity)
	require.NoError(t, err)
	second, err := TypeMailboxResolver{}.Resolve(identity)
	require.NoError(t, err)

	require.Equal(t, first.FairnessKey, second.FairnessKey)
	require.LessOrEqual(t, len(first.FairnessKey), maxFairnessKeySize)
	require.NotEqual(t, "", first.FairnessKey)
}

func TestRouteValidation(t *testing.T) {
	tests := map[string]Route{
		"missing queue":        {FairnessKey: "actor"},
		"long queue":           {TaskQueue: strings.Repeat("q", maxTemporalNameBytes+1)},
		"long fairness key":    {TaskQueue: "actors", FairnessKey: strings.Repeat("k", maxFairnessKeySize+1)},
		"low fairness weight":  {TaskQueue: "actors", FairnessWeight: 0.0009},
		"high fairness weight": {TaskQueue: "actors", FairnessWeight: 1000.1},
		"negative priority":    {TaskQueue: "actors", PriorityKey: -1},
	}
	for name, route := range tests {
		t.Run(name, func(t *testing.T) {
			require.Error(t, route.Validate())
		})
	}
}

func TestWorkflowOptionsWithResolverPropagatesRoute(t *testing.T) {
	identity := ActorIdentity{Type: "list", ID: "cart-42"}
	want := Route{
		TaskQueue:      "custom/list/mailbox",
		FairnessKey:    "list/cart-42",
		FairnessWeight: 2.5,
		PriorityKey:    2,
	}
	resolver := RouteResolverFunc(func(got ActorIdentity) (Route, error) {
		require.Equal(t, identity, got)
		return want, nil
	})

	options, err := WorkflowOptionsWithResolver(identity, resolver)
	require.NoError(t, err)
	require.Equal(t, want.TaskQueue, options.TaskQueue)
	require.Equal(t, want.PriorityKey, options.Priority.PriorityKey)
	require.Equal(t, want.FairnessKey, options.Priority.FairnessKey)
	require.Equal(t, want.FairnessWeight, options.Priority.FairnessWeight)
}

func TestWorkflowOptionsRejectsNilResolver(t *testing.T) {
	_, err := WorkflowOptionsWithResolver(ActorIdentity{Type: "list", ID: "cart-42"}, nil)
	require.Error(t, err)
}
