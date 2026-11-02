package actor

import "testing"

type testMessage struct {
	Amount int
}

func TestHandlerAppliesExplicitStateTransition(t *testing.T) {
	identity := ActorIdentity{Type: "test", ID: "one"}
	handler, err := NewHandler("test/message", func(ctx Context, state int, message testMessage) int {
		if ctx.Identity() != identity {
			t.Fatalf("unexpected identity: %#v", ctx.Identity())
		}
		return state + message.Amount
	})
	if err != nil {
		t.Fatal(err)
	}

	state := handler.Handle(newContext(identity), 3, testMessage{Amount: 4})
	if state != 7 {
		t.Fatalf("got state %d, want 7", state)
	}
}

func TestNoOpHandlerCanReturnSameReference(t *testing.T) {
	type state struct{ Value int }
	message := struct{}{}
	handler, err := NewHandler("test/observe", func(Context, *state, struct{}) *state {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	current := &state{Value: 4}
	returned := handler.Handle(Context{}, current, message)
	if returned != nil {
		t.Fatal("unexpected non-nil state")
	}

	handler, err = NewHandler("test/observe-identity", func(_ Context, state *state, _ struct{}) *state {
		return state
	})
	if err != nil {
		t.Fatal(err)
	}
	if handler.Handle(Context{}, current, message) != current {
		t.Fatal("expected handler to return the same reference")
	}
}

func TestValidateHandlerRejectsInvalidShapes(t *testing.T) {
	tests := map[string]any{
		"not a function":  42,
		"nil function":    (func(Context, int, testMessage) int)(nil),
		"missing context": func(int, testMessage) int { return 0 },
		"wrong return":    func(Context, int, testMessage) string { return "" },
		"two returns":     func(Context, int, testMessage) (int, error) { return 0, nil },
		"variadic":        func(Context, int, ...testMessage) int { return 0 },
	}
	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateHandler(fn); err == nil {
				t.Fatal("expected invalid handler")
			}
		})
	}
}
