package actor

import (
	"errors"
	"fmt"
	"reflect"
)

const stopSignalName = "temporal-actors/v1/stop"

var contextType = reflect.TypeOf(Context{})

// Handler is a typed state transition for one message type.
//
// The handler function is code, not actor state. The runtime stores only the
// value returned by Handle as the next actor state.
type Handler[S, M any] struct {
	signalName string
	fn         func(Context, S, M) S
}

// NewHandler validates and constructs a handler for a Temporal Signal name.
// A handler has the state transition shape:
//
//	func(Context, S, M) S
//
// The Context is actor-scoped and reused. State and message are explicit
// arguments and are not stored on Context.
func NewHandler[S, M any](signalName string, fn func(Context, S, M) S) (Handler[S, M], error) {
	if signalName == "" {
		return Handler[S, M]{}, errors.New("handler signal name must not be empty")
	}
	if signalName == stopSignalName {
		return Handler[S, M]{}, fmt.Errorf("handler signal name %q is reserved", stopSignalName)
	}
	if err := ValidateHandler(fn); err != nil {
		return Handler[S, M]{}, err
	}
	return Handler[S, M]{signalName: signalName, fn: fn}, nil
}

// ValidateHandler verifies the runtime handler shape. It is useful when a
// handler is discovered dynamically, while NewHandler also checks every typed
// handler at construction time.
func ValidateHandler(fn any) error {
	typ := reflect.TypeOf(fn)
	if typ == nil || typ.Kind() != reflect.Func {
		return errors.New("handler must be a function")
	}
	if reflect.ValueOf(fn).IsNil() {
		return errors.New("handler must not be nil")
	}
	if typ.IsVariadic() {
		return errors.New("handler must not be variadic")
	}
	if typ.NumIn() != 3 {
		return fmt.Errorf("handler must have 3 parameters, got %d", typ.NumIn())
	}
	if typ.In(0) != contextType {
		return fmt.Errorf("handler parameter 1 must be actor.Context, got %s", typ.In(0))
	}
	if typ.NumOut() != 1 {
		return fmt.Errorf("handler must have 1 return value, got %d", typ.NumOut())
	}
	if typ.In(1) != typ.Out(0) {
		return fmt.Errorf("handler return type must equal state parameter type, got state %s and return %s", typ.In(1), typ.Out(0))
	}
	return nil
}

// SignalName returns the Temporal Signal used for this handler's messages.
func (h Handler[S, M]) SignalName() string {
	return h.signalName
}

// Handle computes the next state. It does not copy state before invoking the
// function, so a no-op handler may return the same reference without an
// allocation. Handlers must treat state as immutable and must not rely on
// pointer identity across Temporal replay.
func (h Handler[S, M]) Handle(ctx Context, state S, message M) S {
	return h.fn(ctx, state, message)
}
