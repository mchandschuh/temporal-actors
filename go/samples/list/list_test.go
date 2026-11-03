package list

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/mchandschuh/temporal-actors/go/src/actor"
)

func TestHandlers(t *testing.T) {
	ctx := actor.Context{}
	state := AddElement(ctx, nil, Add{Element: "a"})
	state = AddElement(ctx, state, Add{Element: "b"})
	require.Equal(t, State{"a", "b"}, state)

	state = SetElement(ctx, state, Set{Index: 0, Element: "A"})
	state = RemoveElement(ctx, state, Remove{Element: "b"})
	require.Equal(t, State{"A"}, state)

	state = RemoveAtElement(ctx, state, RemoveAt{Index: 99})
	require.Equal(t, State{"A"}, state)
	state = ClearList(ctx, state, Clear{})
	require.Empty(t, state)
}

func TestGetReturnsCopyAndGetAtValidatesIndex(t *testing.T) {
	state := State{"a", "b"}
	copyState, err := GetList(actor.Context{}, state, Get{})
	require.NoError(t, err)
	copyState[0] = "changed"
	require.Equal(t, State{"a", "b"}, state)

	value, err := GetAtElement(actor.Context{}, state, GetAt{Index: 1})
	require.NoError(t, err)
	require.Equal(t, "b", value)
	_, err = GetAtElement(actor.Context{}, state, GetAt{Index: 2})
	require.ErrorIs(t, err, ErrIndexOutOfRange)
}

func TestDefinition(t *testing.T) {
	definition, err := NewDefinition()
	require.NoError(t, err)
	identity, err := definition.NewIdentity("cart-42")
	require.NoError(t, err)
	require.Equal(t, "list", identity.Type)

	result, err := definition.Dispatch(actor.Context{}, State{}, Add{Element: "a"})
	require.NoError(t, err)
	require.Equal(t, State{"a"}, result.State)
	result, err = definition.Dispatch(actor.Context{}, result.State, Get{})
	require.NoError(t, err)
	require.Equal(t, State{"a"}, result.Result)
}

func TestWorkflowSequencesWritesBeforeRead(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	identity := actor.ActorIdentity{Type: "list", ID: "cart-42"}
	definition, err := NewDefinition()
	require.NoError(t, err)

	var response actor.MailboxResponse
	var responseErr error
	var send func(int)
	send = func(index int) {
		message, encodeErr := definition.EncodeMessage(Add{Element: string(rune('a' + index))})
		require.NoError(t, encodeErr)
		environment.UpdateWorkflow(actor.MailboxUpdateName, string(rune('0'+index)), &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("write rejected: %v", err) },
			OnComplete: func(_ any, err error) {
				require.NoError(t, err)
				if index < 2 {
					environment.RegisterDelayedCallback(func() { send(index + 1) }, 0)
					return
				}
				environment.RegisterDelayedCallback(func() {
					request, encodeErr := definition.EncodeMessage(Get{})
					require.NoError(t, encodeErr)
					environment.UpdateWorkflow(actor.MailboxUpdateName, "get", &testsuite.TestUpdateCallback{
						OnReject: func(err error) { responseErr = err },
						OnComplete: func(result any, err error) {
							responseErr = err
							if err == nil {
								response = result.(actor.MailboxResponse)
							}
							environment.RegisterDelayedCallback(func() {
								environment.SignalWorkflow(actor.StopSignalName, struct{}{})
							}, 0)
						},
					}, request)
				}, 0)
			},
		}, message)
	}
	environment.RegisterDelayedCallback(func() { send(0) }, 0)
	environment.ExecuteWorkflow(Workflow, State{}, identity)

	var finalState State
	require.NoError(t, environment.GetWorkflowResult(&finalState))
	require.NoError(t, responseErr)
	require.Equal(t, State{"a", "b", "c"}, finalState)
	var observed State
	require.NoError(t, response.DecodeResult(&observed))
	require.Equal(t, finalState, observed)
}
