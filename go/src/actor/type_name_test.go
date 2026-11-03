package actor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultTypeNameEncoderPreservesShortSafeNames(t *testing.T) {
	encoded, err := DefaultTypeNameEncoder("list")
	require.NoError(t, err)
	require.Equal(t, "list", encoded)
}

func TestDefaultTypeNameEncoderShortensLongNamesDeterministically(t *testing.T) {
	typeName := strings.Repeat("list-actor-", 10)
	first, err := DefaultTypeNameEncoder(typeName)
	require.NoError(t, err)
	second, err := DefaultTypeNameEncoder(typeName)
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Len(t, first, defaultTypeNameMaxBytes)
	require.Equal(t, typeName[:defaultTypeNameMaxBytes-typeNameHashSuffixBytes], first[:defaultTypeNameMaxBytes-typeNameHashSuffixBytes])
	require.NotEqual(t, typeName[:defaultTypeNameMaxBytes], first)
}

func TestDefaultTypeNameEncoderRejectsUnsafeNames(t *testing.T) {
	for _, typeName := range []string{"", "list/actor", " list", "list ", "list\nactor"} {
		_, err := DefaultTypeNameEncoder(typeName)
		require.Error(t, err, typeName)
	}
}

func TestDefinitionUsesSharedDefaultConfig(t *testing.T) {
	first, err := NewDefinition(DefinitionOptions{TypeName: "first"}, Command(func(Context, int, struct{}) int { return 0 }))
	require.NoError(t, err)
	second, err := NewDefinition(DefinitionOptions{TypeName: "second"}, Command(func(Context, int, string) int { return 0 }))
	require.NoError(t, err)

	require.Same(t, first.config, second.config)
	require.Equal(t, defaultRouteVersion, first.config.routeVersion)
}

func TestDefinitionOverrideUsesCustomEncoderAndIdentity(t *testing.T) {
	definition, err := NewDefinition(
		DefinitionOptions{
			TypeName:     "list",
			RouteVersion: "test",
			TypeNameEncoder: TypeNameEncoderFunc(func(typeName string) (string, error) {
				return "custom-" + typeName, nil
			}),
		},
		Command(func(Context, int, struct{}) int { return 0 }),
	)
	require.NoError(t, err)

	identity, err := definition.NewIdentity("cart-42")
	require.NoError(t, err)
	require.Equal(t, "list", identity.Type)

	route, err := definition.RouteResolver().Resolve(identity)
	require.NoError(t, err)
	require.Equal(t, "temporal-actors/test/actors/custom-list/mailbox", route.TaskQueue)
}

func TestCustomEncoderMustReturnQueueSafeSegment(t *testing.T) {
	definition, err := NewDefinition(
		DefinitionOptions{
			TypeName: "list",
			TypeNameEncoder: TypeNameEncoderFunc(func(string) (string, error) {
				return "", nil
			}),
		},
		Command(func(Context, int, struct{}) int { return 0 }),
	)
	require.NoError(t, err)
	identity, err := definition.NewIdentity("cart-42")
	require.NoError(t, err)
	_, err = definition.RouteResolver().Resolve(identity)
	require.Error(t, err)
}

func TestConfigureAfterDefinitionCreationIsRejected(t *testing.T) {
	require.Error(t, Configure(Config{RouteVersion: "late"}))
}
