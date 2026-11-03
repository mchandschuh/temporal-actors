package actor

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type mailboxAdd struct {
	Value int
}

type mailboxGet struct{}

func newMailboxTestDefinition(t *testing.T) Definition[int] {
	t.Helper()
	definition, err := NewDefinition(DefinitionOptions{TypeName: "test-mailbox"},
		CommandWithTypeID("test/add/v1", func(Context, int, mailboxAdd) int {
			return 0
		}),
		RequestWithTypeID("test/get/v1", func(_ Context, state int, _ mailboxGet) (int, error) {
			return state, nil
		}),
	)
	require.NoError(t, err)
	return definition
}

func mailboxWorkflow(ctx workflow.Context, initialState int, identity ActorIdentity) (int, error) {
	definition, err := NewDefinition(DefinitionOptions{TypeName: "test-mailbox"},
		CommandWithTypeID("test/add/v1", func(_ Context, state int, message mailboxAdd) int {
			return state + message.Value
		}),
		RequestWithTypeID("test/get/v1", func(_ Context, state int, _ mailboxGet) (int, error) {
			return state, nil
		}),
	)
	if err != nil {
		return initialState, err
	}
	return RunDefinition(ctx, definition, identity, initialState)
}

func TestRunDefinitionSequencesCommandsBeforeRequest(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	identity := ActorIdentity{Type: "test-mailbox", ID: "one"}
	definition := newMailboxTestDefinition(t)

	var requestResponse MailboxResponse
	var requestError error
	var sendAdd func(int)
	sendAdd = func(index int) {
		request, err := definition.EncodeMessage(mailboxAdd{Value: 1})
		require.NoError(t, err)
		environment.UpdateWorkflow(MailboxUpdateName, fmt.Sprintf("add-%04d", index), &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("add update rejected: %v", err) },
			OnComplete: func(_ any, err error) {
				if err != nil {
					t.Errorf("add update failed: %v", err)
					return
				}
				if index+1 < 1000 {
					environment.RegisterDelayedCallback(func() {
						sendAdd(index + 1)
					}, 0)
					return
				}

				environment.RegisterDelayedCallback(func() {
					request, err := definition.EncodeMessage(mailboxGet{})
					require.NoError(t, err)
					environment.UpdateWorkflow(MailboxUpdateName, "get", &testsuite.TestUpdateCallback{
						OnReject: func(err error) { requestError = err },
						OnComplete: func(result any, err error) {
							requestError = err
							if err == nil {
								requestResponse = result.(MailboxResponse)
							}
							environment.RegisterDelayedCallback(func() {
								environment.SignalWorkflow(StopSignalName, struct{}{})
							}, 0)
						},
					}, request)
				}, 0)
			},
		}, request)
	}
	environment.RegisterDelayedCallback(func() { sendAdd(0) }, 0)

	environment.ExecuteWorkflow(mailboxWorkflow, 0, identity)

	var finalState int
	require.NoError(t, environment.GetWorkflowResult(&finalState))
	require.NoError(t, requestError)
	require.Equal(t, 1000, finalState)
	var observed int
	require.NoError(t, requestResponse.DecodeResult(&observed))
	require.Equal(t, 1000, observed)
}

func TestRunDefinitionKeepsStateWhenRequestFails(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	identity := ActorIdentity{Type: "test-mailbox", ID: "error"}

	definition, err := NewDefinition(DefinitionOptions{TypeName: "test-mailbox"},
		RequestWithTypeID("test/fail/v1", func(_ Context, state int, _ struct{}) (int, error) {
			return state, errors.New("expected request failure")
		}),
	)
	require.NoError(t, err)

	var requestError error
	environment.RegisterDelayedCallback(func() {
		request, encodeErr := definition.EncodeMessage(struct{}{})
		require.NoError(t, encodeErr)
		environment.UpdateWorkflow(MailboxUpdateName, "fail", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { requestError = err },
			OnComplete: func(_ any, err error) {
				requestError = err
				environment.RegisterDelayedCallback(func() {
					environment.SignalWorkflow(StopSignalName, struct{}{})
				}, 0)
			},
		}, request)
	}, 0)

	workflowWithFailure := func(ctx workflow.Context, initial int, identity ActorIdentity) (int, error) {
		return RunDefinition(ctx, definition, identity, initial)
	}
	environment.ExecuteWorkflow(workflowWithFailure, 7, identity)

	var finalState int
	require.NoError(t, environment.GetWorkflowResult(&finalState))
	require.Error(t, requestError)
	require.Equal(t, 7, finalState)
}

func TestMailboxMessageTypeIDOnlySelectsDecoder(t *testing.T) {
	definition, err := NewDefinition(DefinitionOptions{TypeName: "test-mailbox"},
		CommandWithTypeID("test/add/v1", func(Context, int, mailboxAdd) int { return 1 }),
	)
	require.NoError(t, err)

	request, err := definition.EncodeMessage(mailboxAdd{Value: 99})
	require.NoError(t, err)
	message, err := definition.DecodeMessage(request)
	require.NoError(t, err)
	require.Equal(t, mailboxAdd{Value: 99}, message)

	result, err := definition.Dispatch(Context{}, 0, message)
	require.NoError(t, err)
	require.Equal(t, 1, result.State)
}

func TestMailboxRejectsUnknownTypeID(t *testing.T) {
	definition, err := NewDefinition(DefinitionOptions{TypeName: "test-mailbox"},
		CommandWithTypeID("test/add/v1", func(Context, int, mailboxAdd) int { return 1 }),
	)
	require.NoError(t, err)

	_, err = definition.DecodeMessage(MailboxRequest{
		TypeID:  "test/unknown/v1",
		Payload: []byte(`{}`),
	})
	require.ErrorIs(t, err, ErrUnknownMessageTypeID)
}

func TestMailboxRejectsDuplicateTypeID(t *testing.T) {
	_, err := NewDefinition(DefinitionOptions{TypeName: "test-mailbox"},
		CommandWithTypeID("test/shared/v1", func(Context, int, mailboxAdd) int { return 1 }),
		RequestWithTypeID("test/shared/v1", func(Context, int, mailboxGet) (int, error) { return 1, nil }),
	)
	require.ErrorIs(t, err, ErrDuplicateMessageTypeID)
}

func TestMailboxResponseDecodeResult(t *testing.T) {
	response := MailboxResponse{Kind: RequestHandler, Payload: []byte(`7`)}
	var result int
	require.NoError(t, response.DecodeResult(&result))
	require.Equal(t, 7, result)

	require.Error(t, (MailboxResponse{Kind: CommandHandler}).DecodeResult(&result))
	require.Error(t, response.DecodeResult(nil))
}
