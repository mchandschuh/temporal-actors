package list

import "github.com/mchandschuh/temporal-actors/go/src/actor"

// NewDefinition constructs the immutable definition for list actors.
func NewDefinition() (actor.Definition[State], error) {
	return actor.NewDefinition(
		actor.DefinitionOptions{TypeName: "list"},
		actor.Command(AddElement),
		actor.Command(RemoveElement),
		actor.Command(RemoveAtElement),
		actor.Command(SetElement),
		actor.Command(ClearList),
		actor.Request(GetList),
		actor.Request(GetAtElement),
	)
}
