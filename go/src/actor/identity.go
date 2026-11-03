package actor

import (
	"encoding/base64"
	"errors"
	"fmt"
)

const (
	identityPrefix       = "temporal-actors/v1"
	maxTemporalNameBytes = 1000
)

// ActorIdentity is the stable logical identity of an actor.
//
// Type identifies the actor implementation and ID identifies one instance of
// that type. Both values are encoded when they are used in the durable
// Workflow ID so arbitrary strings cannot create ambiguous names. Task Queue
// and fairness-key naming is delegated to a RouteResolver.
type ActorIdentity struct {
	Type string
	ID   string
}

// Validate checks that an actor identity can be converted into Temporal names.
func (i ActorIdentity) Validate() error {
	if i.Type == "" {
		return errors.New("actor type must not be empty")
	}
	if i.ID == "" {
		return errors.New("actor ID must not be empty")
	}
	if workflowID := i.WorkflowID(); len(workflowID) > maxTemporalNameBytes {
		return fmt.Errorf("actor workflow ID is %d bytes, maximum is %d", len(workflowID), maxTemporalNameBytes)
	}
	return nil
}

// WorkflowID returns the conventional Temporal Workflow ID for this actor.
func (i ActorIdentity) WorkflowID() string {
	return i.temporalName("workflow")
}

func (i ActorIdentity) temporalName(kind string) string {
	return fmt.Sprintf(
		"%s/%s/%s/%s",
		identityPrefix,
		kind,
		encodeIdentityPart(i.Type),
		encodeIdentityPart(i.ID),
	)
}

func encodeIdentityPart(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
