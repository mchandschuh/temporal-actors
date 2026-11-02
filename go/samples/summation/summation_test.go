package summation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/mchandschuh/temporal-actors/go/src/actor"
)

func TestSummation(t *testing.T) {
	identity := actor.ActorIdentity{Type: "summation", ID: "account-42"}
	state := Summation(actor.Context{}, 0, Add{Value: 1})
	state = Summation(actor.Context{}, state, Add{Value: 2})
	state = Summation(actor.Context{}, state, Add{Value: 5})
	require.Equal(t, 8, state)
	_ = identity
}

func TestWorkflowProcessesMessagesAndReturnsStateOnStop(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	identity := actor.ActorIdentity{Type: "summation", ID: "account-42"}

	environment.RegisterDelayedCallback(func() {
		environment.SignalWorkflow(AddSignalName, Add{Value: 1})
		environment.SignalWorkflow(AddSignalName, Add{Value: 2})
		environment.SignalWorkflow(AddSignalName, Add{Value: 5})
		environment.SignalWorkflow(actor.StopSignalName, struct{}{})
	}, 0)
	environment.ExecuteWorkflow(Workflow, 0, identity)

	var result int
	require.NoError(t, environment.GetWorkflowResult(&result))
	require.Equal(t, 8, result)
}
