package list

import (
	"go.temporal.io/sdk/workflow"

	"github.com/mchandschuh/temporal-actors/go/src/actor"
)

// Workflow runs one isolated list actor instance.
func Workflow(ctx workflow.Context, initialState State, identity actor.ActorIdentity) (State, error) {
	definition, err := NewDefinition()
	if err != nil {
		return initialState, err
	}
	return actor.RunDefinition(ctx, definition, identity, initialState)
}
