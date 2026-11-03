package summation

import (
	"go.temporal.io/sdk/workflow"

	"github.com/mchandschuh/temporal-actors/go/src/actor"
)

const AddSignalName = "temporal-actors/v1/summation/add"

// Add is the message consumed by Summation.
type Add struct {
	Value int
}

// Summation is a left-fold state transition. The actor runtime owns state and
// supplies the current value explicitly for every message.
func Summation(_ actor.Context, state int, message Add) int {
	return state + message.Value
}

// NewDefinition returns the immutable handler registry for a summation actor.
func NewDefinition() (actor.Definition[int], error) {
	return actor.NewDefinition(actor.DefinitionOptions{TypeName: "summation"}, actor.Command(Summation))
}

// Workflow is the Temporal workflow definition for one summation actor.
func Workflow(ctx workflow.Context, initialState int, identity actor.ActorIdentity) (int, error) {
	handler, err := actor.NewHandler(AddSignalName, Summation)
	if err != nil {
		return initialState, err
	}
	return actor.Run(ctx, initialState, identity, handler)
}
