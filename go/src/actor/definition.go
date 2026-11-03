package actor

import (
	"errors"
	"fmt"
	"reflect"
)

// HandlerKind identifies how a registered handler participates in the actor
// mailbox.
//
// A command computes the next state. A request observes the current state and
// returns a result while leaving the state value unchanged. Both kinds are
// mailbox messages and must be sequenced by the actor runtime.
type HandlerKind uint8

const (
	// CommandHandler identifies a state-transition handler.
	CommandHandler HandlerKind = iota
	// RequestHandler identifies a sequenced read handler.
	RequestHandler
)

func (k HandlerKind) String() string {
	switch k {
	case CommandHandler:
		return "command"
	case RequestHandler:
		return "request"
	default:
		return fmt.Sprintf("handler kind %d", k)
	}
}

var (
	// ErrNoHandler reports that a message has no registered handler.
	ErrNoHandler = errors.New("no actor handler matches message type")
	// ErrAmbiguousHandler reports that more than one equally specific handler
	// matches a message type.
	ErrAmbiguousHandler = errors.New("actor handler selection is ambiguous")
	// ErrDuplicateMessageType reports multiple registrations for one message
	// type. A message type can have only one handler kind in a definition.
	ErrDuplicateMessageType = errors.New("actor message type is registered more than once")
	// ErrInvalidHandlerSpec reports a zero or otherwise malformed handler spec.
	ErrInvalidHandlerSpec = errors.New("invalid actor handler specification")
)

// HandlerSpec is an immutable, typed registration entry for a Definition.
// Construct it with Command or Request rather than initializing it directly.
type HandlerSpec[S any] struct {
	kind        HandlerKind
	messageType reflect.Type
	invoke      func(Context, S, any) (DispatchResult[S], error)
}

// Command registers a state transition with the shape:
//
//	func(Context, S, M) S
//
// The returned state is the only state transition performed by the runtime.
// The handler must not keep mutable state in a closure or receiver and must
// treat the state argument as immutable, even when S contains references.
func Command[S, M any](fn func(Context, S, M) S) HandlerSpec[S] {
	messageType := reflect.TypeOf((*M)(nil)).Elem()
	if fn == nil {
		return HandlerSpec[S]{}
	}
	return HandlerSpec[S]{
		kind:        CommandHandler,
		messageType: messageType,
		invoke: func(ctx Context, state S, message any) (DispatchResult[S], error) {
			typedMessage, ok := message.(M)
			if !ok {
				return DispatchResult[S]{}, fmt.Errorf("message has type %T, cannot invoke handler for %s", message, messageType)
			}
			return DispatchResult[S]{
				Kind:  CommandHandler,
				State: fn(ctx, state, typedMessage),
			}, nil
		},
	}
}

// Request registers a sequenced read handler with the shape:
//
//	func(Context, S, M) (R, error)
//
// Requests are mailbox messages, not Temporal Query handlers. The actor
// processes them after all earlier mailbox messages and leaves the state
// unchanged. The result is returned through DispatchResult.Result.
func Request[S, M, R any](fn func(Context, S, M) (R, error)) HandlerSpec[S] {
	messageType := reflect.TypeOf((*M)(nil)).Elem()
	if fn == nil {
		return HandlerSpec[S]{}
	}
	return HandlerSpec[S]{
		kind:        RequestHandler,
		messageType: messageType,
		invoke: func(ctx Context, state S, message any) (DispatchResult[S], error) {
			typedMessage, ok := message.(M)
			if !ok {
				return DispatchResult[S]{}, fmt.Errorf("message has type %T, cannot invoke handler for %s", message, messageType)
			}
			result, err := fn(ctx, state, typedMessage)
			if err != nil {
				return DispatchResult[S]{State: state, Kind: RequestHandler}, err
			}
			return DispatchResult[S]{
				Kind:   RequestHandler,
				State:  state,
				Result: result,
			}, nil
		},
	}
}

// DispatchResult is the result of one Definition dispatch. For a command,
// State is the next actor state and Result is nil. For a request, State is the
// unchanged input state and Result contains the request result.
type DispatchResult[S any] struct {
	Kind   HandlerKind
	State  S
	Result any
}

// Definition is an immutable registry of handlers for one actor type and one
// state type.
//
// Definitions are actor-specific code, not actor instances. Every actor
// instance has its own explicit state and identity, while all instances of a
// definition share this immutable handler registry.
//
// Handler selection is based on the message's Go type, not a handler name. An
// exact concrete type wins first. If no exact handler exists, assignable
// interface handlers are considered; a more specific interface wins over a
// broader interface. Incomparable matching interfaces are rejected as
// ambiguous rather than selected by registration order.
//
// Definition does not assign or persist handler names. Any future serialized
// mailbox envelope may need a schema/type discriminator to decode a message,
// but decoded messages are still dispatched by their Go type in this registry.
type Definition[S any] struct {
	handlers []registeredHandler[S]
}

type registeredHandler[S any] struct {
	HandlerSpec[S]
}

// NewDefinition constructs an immutable handler registry. Registration order
// has no effect on dispatch. A concrete or interface message type may be
// registered only once in a definition.
func NewDefinition[S any](specs ...HandlerSpec[S]) (Definition[S], error) {
	handlers := make([]registeredHandler[S], 0, len(specs))
	seen := make(map[reflect.Type]struct{}, len(specs))
	for index, spec := range specs {
		if spec.invoke == nil || spec.messageType == nil {
			return Definition[S]{}, fmt.Errorf("handler %d: %w", index, ErrInvalidHandlerSpec)
		}
		if spec.messageType.Kind() == reflect.Interface && spec.messageType.NumMethod() == 0 {
			return Definition[S]{}, fmt.Errorf("handler %d: empty interface message types are not allowed", index)
		}
		if _, exists := seen[spec.messageType]; exists {
			return Definition[S]{}, fmt.Errorf("message type %s: %w", spec.messageType, ErrDuplicateMessageType)
		}
		seen[spec.messageType] = struct{}{}
		handlers = append(handlers, registeredHandler[S]{HandlerSpec: spec})
	}
	return Definition[S]{handlers: handlers}, nil
}

// Dispatch selects and invokes the most specific handler for message. A
// message with no matching handler returns ErrNoHandler. A message matching
// multiple incomparable interfaces returns ErrAmbiguousHandler.
func (d Definition[S]) Dispatch(ctx Context, state S, message any) (DispatchResult[S], error) {
	messageType := reflect.TypeOf(message)
	if messageType == nil {
		return DispatchResult[S]{State: state}, fmt.Errorf("%w: <nil>", ErrNoHandler)
	}

	handler, err := d.selectHandler(messageType)
	if err != nil {
		return DispatchResult[S]{State: state}, err
	}
	return handler.invoke(ctx, state, message)
}

func (d Definition[S]) selectHandler(messageType reflect.Type) (registeredHandler[S], error) {
	var matches []registeredHandler[S]
	for _, handler := range d.handlers {
		if handler.messageType == messageType ||
			(handler.messageType.Kind() == reflect.Interface && messageType.AssignableTo(handler.messageType)) {
			matches = append(matches, handler)
		}
	}
	if len(matches) == 0 {
		return registeredHandler[S]{}, fmt.Errorf("%w: %s", ErrNoHandler, messageType)
	}

	for _, match := range matches {
		if match.messageType == messageType {
			return match, nil
		}
	}

	maximal := make([]registeredHandler[S], 0, len(matches))
	for _, candidate := range matches {
		isLessSpecific := false
		for _, other := range matches {
			if candidate.messageType != other.messageType &&
				other.messageType.Kind() == reflect.Interface &&
				candidate.messageType.Kind() == reflect.Interface &&
				other.messageType.AssignableTo(candidate.messageType) {
				isLessSpecific = true
				break
			}
		}
		if !isLessSpecific {
			maximal = append(maximal, candidate)
		}
	}
	if len(maximal) != 1 {
		matchedTypes := make([]string, len(maximal))
		for index, match := range maximal {
			matchedTypes[index] = match.messageType.String()
		}
		return registeredHandler[S]{}, fmt.Errorf("%w for %s: %v", ErrAmbiguousHandler, messageType, matchedTypes)
	}
	return maximal[0], nil
}
