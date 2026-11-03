package actor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefinitionRejectsMismatchedIdentity(t *testing.T) {
	definition, err := NewDefinition(
		DefinitionOptions{TypeName: "list"},
		Command(func(Context, int, struct{}) int { return 0 }),
	)
	require.NoError(t, err)

	err = definition.ValidateIdentity(ActorIdentity{Type: "summation", ID: "one"})
	require.Error(t, err)
}
