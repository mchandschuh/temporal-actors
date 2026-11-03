package actor

import (
	"errors"
	"testing"
)

type definitionMessage struct {
	Value int
}

type definitionOtherMessage struct{}

type definitionMessageInterface interface {
	definitionMessage()
}

type definitionEmptyInterface interface{}

type definitionSpecificInterface interface {
	definitionMessageInterface
	definitionSpecific()
}

type definitionAmbiguousA interface {
	definitionMessage()
	definitionAmbiguousA()
}

type definitionAmbiguousB interface {
	definitionMessage()
	definitionAmbiguousB()
}

type definitionSpecificMessage struct{}

func (definitionSpecificMessage) definitionMessage()    {}
func (definitionSpecificMessage) definitionSpecific()   {}
func (definitionSpecificMessage) definitionAmbiguousA() {}
func (definitionSpecificMessage) definitionAmbiguousB() {}

func TestDefinitionDispatchesCommandsAndRequestsByMessageType(t *testing.T) {
	type state struct{ Total int }
	type request struct{}

	definition, err := NewDefinition(DefinitionOptions{TypeName: "definition-test"},
		Command(func(_ Context, state state, message definitionMessage) state {
			state.Total += message.Value
			return state
		}),
		Request(func(_ Context, state state, _ request) (int, error) {
			return state.Total, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	commandResult, err := definition.Dispatch(Context{}, state{}, definitionMessage{Value: 3})
	if err != nil {
		t.Fatal(err)
	}
	if commandResult.Kind != CommandHandler || commandResult.State.Total != 3 || commandResult.Result != nil {
		t.Fatalf("unexpected command result: %#v", commandResult)
	}

	requestResult, err := definition.Dispatch(Context{}, commandResult.State, request{})
	if err != nil {
		t.Fatal(err)
	}
	if requestResult.Kind != RequestHandler || requestResult.State.Total != 3 || requestResult.Result != 3 {
		t.Fatalf("unexpected request result: %#v", requestResult)
	}
}

func TestDefinitionUsesExactTypeBeforeInterface(t *testing.T) {
	definition, err := NewDefinition(DefinitionOptions{TypeName: "exact-type-test"},
		Command(func(Context, int, definitionMessageInterface) int { return 1 }),
		Command(func(Context, int, definitionMessage) int { return 2 }),
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := definition.Dispatch(Context{}, 0, definitionMessage{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != 2 {
		t.Fatalf("got state %d, want exact handler result 2", result.State)
	}
}

func TestDefinitionUsesMostSpecificInterface(t *testing.T) {
	definition, err := NewDefinition(DefinitionOptions{TypeName: "specific-interface-test"},
		Command(func(Context, int, definitionSpecificInterface) int { return 2 }),
		Command(func(Context, int, definitionMessageInterface) int { return 1 }),
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := definition.Dispatch(Context{}, 0, definitionSpecificMessage{})
	if err != nil {
		t.Fatal(err)
	}
	if result.State != 2 {
		t.Fatalf("got state %d, want specific interface result 2", result.State)
	}
}

func TestRequestErrorLeavesStateUnchanged(t *testing.T) {
	type state struct{ Total int }
	type request struct{}
	expected := errors.New("request failed")

	definition, err := NewDefinition(DefinitionOptions{TypeName: "request-error-test"}, Request(func(_ Context, state state, _ request) (int, error) {
		return state.Total, expected
	}))
	if err != nil {
		t.Fatal(err)
	}

	input := state{Total: 7}
	result, err := definition.Dispatch(Context{}, input, request{})
	if !errors.Is(err, expected) {
		t.Fatalf("got error %v, want %v", err, expected)
	}
	if result.State != input {
		t.Fatalf("got state %#v, want unchanged state %#v", result.State, input)
	}
}

func TestDefinitionRejectsAmbiguousInterfaces(t *testing.T) {
	definition, err := NewDefinition(DefinitionOptions{TypeName: "ambiguous-interface-test"},
		Command(func(Context, int, definitionAmbiguousA) int { return 1 }),
		Command(func(Context, int, definitionAmbiguousB) int { return 2 }),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = definition.Dispatch(Context{}, 0, definitionSpecificMessage{})
	if !errors.Is(err, ErrAmbiguousHandler) {
		t.Fatalf("got error %v, want ErrAmbiguousHandler", err)
	}
}

func TestNewDefinitionRejectsInvalidRegistrations(t *testing.T) {
	valid := Command(func(Context, int, definitionMessage) int { return 0 })
	for name, specs := range map[string][]HandlerSpec[int]{
		"duplicate message type": {valid, valid},
		"nil command":            {Command[int, definitionMessage](nil)},
		"nil request":            {Request[int, definitionMessage, int](nil)},
		"empty interface":        {Command(func(Context, int, any) int { return 0 })},
		"named empty interface":  {Command(func(Context, int, definitionEmptyInterface) int { return 0 })},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewDefinition(DefinitionOptions{TypeName: "invalid-registration-test"}, specs...); err == nil {
				t.Fatal("expected definition construction to fail")
			}
		})
	}
}

func TestDefinitionReturnsNoHandlerForUnknownMessage(t *testing.T) {
	definition, err := NewDefinition(DefinitionOptions{TypeName: "unknown-message-test"}, Command(func(Context, int, definitionMessage) int { return 0 }))
	if err != nil {
		t.Fatal(err)
	}

	_, err = definition.Dispatch(Context{}, 7, definitionOtherMessage{})
	if !errors.Is(err, ErrNoHandler) {
		t.Fatalf("got error %v, want ErrNoHandler", err)
	}
}

func TestDefinitionDispatchesTypedNilPointer(t *testing.T) {
	type pointerMessage struct{}
	definition, err := NewDefinition(DefinitionOptions{TypeName: "typed-nil-test"}, Command(func(Context, int, *pointerMessage) int { return 9 }))
	if err != nil {
		t.Fatal(err)
	}

	var message *pointerMessage
	result, err := definition.Dispatch(Context{}, 0, message)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != 9 {
		t.Fatalf("got state %d, want 9", result.State)
	}
}
