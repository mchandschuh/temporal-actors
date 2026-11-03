package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

// MailboxUpdateName is the one runtime-owned Temporal Update registered for an
// actor workflow. Actor packages do not define one Update function per
// handler.
const MailboxUpdateName = "temporal-actors/v1/mailbox"

// MailboxRequest is the serialized application message passed to the common
// actor Update handler. TypeID is a schema/decoding discriminator, not a
// handler lookup key. Payload is JSON encoded by Definition.EncodeMessage.
type MailboxRequest struct {
	TypeID  string `json:"type_id"`
	Payload []byte `json:"payload"`
}

// MailboxResponse is returned by the common actor Update handler. Commands
// have no payload. Requests contain a JSON-encoded result.
type MailboxResponse struct {
	Kind    HandlerKind `json:"kind"`
	Payload []byte      `json:"payload,omitempty"`
}

// DecodeResult decodes a request result into target. Commands return an empty
// payload and should be acknowledged without calling DecodeResult.
func (r MailboxResponse) DecodeResult(target any) error {
	if r.Kind != RequestHandler {
		return errors.New("mailbox response does not contain a request result")
	}
	if target == nil {
		return errors.New("mailbox result target must not be nil")
	}
	if len(r.Payload) == 0 {
		return errors.New("mailbox response has no request result payload")
	}
	return json.Unmarshal(r.Payload, target)
}

// RunDefinition registers the runtime-owned mailbox Update handler and keeps
// the actor workflow alive until StopSignalName is received.
//
// The Update handler performs one complete synchronous operation:
//
//	decode -> Definition.Dispatch -> replace state -> encode response
//
// It does not call any Temporal operation that yields the workflow scheduler.
// Actor handlers receive actor.Context rather than workflow.Context and must
// remain deterministic, non-blocking, and replay-safe. This is the actor
// serial-execution invariant: one mailbox message completes before the next
// mailbox message can begin.
func RunDefinition[S any](
	ctx workflow.Context,
	definition Definition[S],
	identity ActorIdentity,
	initialState S,
) (S, error) {
	if err := identity.Validate(); err != nil {
		return initialState, err
	}
	actorContext := newContext(identity)
	state := initialState

	if err := workflow.SetUpdateHandler(ctx, MailboxUpdateName,
		func(_ workflow.Context, request MailboxRequest) (MailboxResponse, error) {
			message, err := definition.DecodeMessage(request)
			if err != nil {
				return MailboxResponse{}, err
			}
			result, err := definition.Dispatch(actorContext, state, message)
			if err != nil {
				return MailboxResponse{}, err
			}
			state = result.State

			response := MailboxResponse{Kind: result.Kind}
			if result.Kind == RequestHandler {
				response.Payload, err = json.Marshal(result.Result)
				if err != nil {
					return MailboxResponse{}, fmt.Errorf("encode mailbox result: %w", err)
				}
			}
			return response, nil
		}); err != nil {
		return initialState, err
	}

	workflow.GetSignalChannel(ctx, StopSignalName).Receive(ctx, nil)
	if err := workflow.Await(ctx, func() bool {
		return workflow.AllHandlersFinished(ctx)
	}); err != nil {
		return state, err
	}
	return state, nil
}

// SendMailboxUpdate sends a request to an actor's common mailbox Update and
// waits for the handler to complete. UpdateID must be unique for the
// Namespace+WorkflowID+RunID scope; callers should reuse it when retrying the
// same logical request.
func SendMailboxUpdate(
	ctx context.Context,
	c client.Client,
	identity ActorIdentity,
	updateID string,
	request MailboxRequest,
) (MailboxResponse, error) {
	if updateID == "" {
		return MailboxResponse{}, errors.New("mailbox update ID must not be empty")
	}
	if err := identity.Validate(); err != nil {
		return MailboxResponse{}, err
	}
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   identity.WorkflowID(),
		UpdateID:     updateID,
		UpdateName:   MailboxUpdateName,
		Args:         []any{request},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return MailboxResponse{}, err
	}
	var response MailboxResponse
	if err := handle.Get(ctx, &response); err != nil {
		return MailboxResponse{}, err
	}
	return response, nil
}
