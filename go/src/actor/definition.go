package actor

import (
	"encoding/json"
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
	// ErrDuplicateMessageTypeID reports multiple registrations for one mailbox
	// message type identifier.
	ErrDuplicateMessageTypeID = errors.New("actor mailbox message type ID is registered more than once")
	// ErrUnknownMessageTypeID reports a mailbox message with no decoder in the
	// definition.
	ErrUnknownMessageTypeID = errors.New("unknown actor mailbox message type ID")
	// ErrNoMailboxDecoder reports a handler that can dispatch in-process but has
	// no concrete message decoder for mailbox transport.
	ErrNoMailboxDecoder = errors.New("actor handler has no mailbox decoder")
)

// HandlerSpec is an immutable, typed registration entry for a Definition.
// Construct it with Command or Request rather than initializing it directly.
type HandlerSpec[S any] struct {
	kind        HandlerKind
	messageType reflect.Type
	typeID      string
	decode      func([]byte) (any, error)
	invoke      func(Context, S, any) (DispatchResult[S], error)
}

// DefinitionOptions configures one actor definition. Empty routing fields use
// the immutable process-wide actor configuration.
type DefinitionOptions struct {
	TypeName        string
	RouteVersion    string
	TypeNameEncoder TypeNameEncoder
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
	return commandWithTypeID(defaultMessageTypeID(messageType), fn)
}

// CommandWithTypeID registers a command with an explicit mailbox message type
// identifier. The identifier is a transport/schema discriminator used to
// decode the payload; handler selection still occurs by the decoded Go type.
// Use an explicit, versioned identifier when the message crosses a durable
// mailbox boundary.
func CommandWithTypeID[S, M any](typeID string, fn func(Context, S, M) S) HandlerSpec[S] {
	return commandWithTypeID(typeID, fn)
}

func commandWithTypeID[S, M any](typeID string, fn func(Context, S, M) S) HandlerSpec[S] {
	messageType := reflect.TypeOf((*M)(nil)).Elem()
	if fn == nil {
		return HandlerSpec[S]{}
	}
	spec := HandlerSpec[S]{
		kind:        CommandHandler,
		messageType: messageType,
		typeID:      typeID,
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
	if messageType.Kind() != reflect.Interface {
		spec.decode = jsonDecoder[M]
	}
	return spec
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
	return requestWithTypeID(defaultMessageTypeID(messageType), fn)
}

// RequestWithTypeID registers a sequenced request with an explicit mailbox
// message type identifier. The identifier is a transport/schema discriminator
// used only to decode the payload before Definition.Dispatch selects the
// handler by Go message type.
func RequestWithTypeID[S, M, R any](typeID string, fn func(Context, S, M) (R, error)) HandlerSpec[S] {
	return requestWithTypeID(typeID, fn)
}

func requestWithTypeID[S, M, R any](typeID string, fn func(Context, S, M) (R, error)) HandlerSpec[S] {
	messageType := reflect.TypeOf((*M)(nil)).Elem()
	if fn == nil {
		return HandlerSpec[S]{}
	}
	spec := HandlerSpec[S]{
		kind:        RequestHandler,
		messageType: messageType,
		typeID:      typeID,
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
	if messageType.Kind() != reflect.Interface {
		spec.decode = jsonDecoder[M]
	}
	return spec
}

func jsonDecoder[M any](payload []byte) (any, error) {
	var message M
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, err
	}
	return message, nil
}

func defaultMessageTypeID(messageType reflect.Type) string {
	return "go:" + messageType.PkgPath() + ":" + messageType.String()
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
	typeName string
	config   *resolvedConfig
	handlers []registeredHandler[S]
	byTypeID map[string]registeredHandler[S]
}

type registeredHandler[S any] struct {
	HandlerSpec[S]
}

// NewDefinition constructs an immutable handler registry. Registration order
// has no effect on dispatch. A concrete or interface message type may be
// registered only once in a definition.
func NewDefinition[S any](options DefinitionOptions, specs ...HandlerSpec[S]) (Definition[S], error) {
	if err := validateTypeName(options.TypeName); err != nil {
		return Definition[S]{}, err
	}
	config, err := resolveDefinitionConfig(options)
	if err != nil {
		return Definition[S]{}, err
	}
	handlers := make([]registeredHandler[S], 0, len(specs))
	seen := make(map[reflect.Type]struct{}, len(specs))
	seenTypeIDs := make(map[string]struct{}, len(specs))
	for index, spec := range specs {
		if spec.invoke == nil || spec.messageType == nil || spec.typeID == "" {
			return Definition[S]{}, fmt.Errorf("handler %d: %w", index, ErrInvalidHandlerSpec)
		}
		if spec.messageType.Kind() == reflect.Interface && spec.messageType.NumMethod() == 0 {
			return Definition[S]{}, fmt.Errorf("handler %d: empty interface message types are not allowed", index)
		}
		if _, exists := seen[spec.messageType]; exists {
			return Definition[S]{}, fmt.Errorf("message type %s: %w", spec.messageType, ErrDuplicateMessageType)
		}
		if _, exists := seenTypeIDs[spec.typeID]; exists {
			return Definition[S]{}, fmt.Errorf("message type ID %q: %w", spec.typeID, ErrDuplicateMessageTypeID)
		}
		seen[spec.messageType] = struct{}{}
		seenTypeIDs[spec.typeID] = struct{}{}
		handlers = append(handlers, registeredHandler[S]{HandlerSpec: spec})
	}
	byTypeID := make(map[string]registeredHandler[S], len(handlers))
	for _, handler := range handlers {
		byTypeID[handler.typeID] = handler
	}
	return Definition[S]{
		typeName: options.TypeName,
		config:   config,
		handlers: handlers,
		byTypeID: byTypeID,
	}, nil
}

func resolveDefinitionConfig(options DefinitionOptions) (*resolvedConfig, error) {
	configMu.Lock()
	defer configMu.Unlock()
	configFrozen = true
	if options.RouteVersion == "" && options.TypeNameEncoder == nil {
		return globalConfig, nil
	}
	return resolveConfigLocked(options.RouteVersion, options.TypeNameEncoder)
}

func resolveConfigLocked(routeVersion string, encoder TypeNameEncoder) (*resolvedConfig, error) {
	resolved := *globalConfig
	if routeVersion != "" {
		resolved.routeVersion = routeVersion
	}
	if encoder != nil {
		resolved.typeNameEncoder = encoder
	}
	if resolved.routeVersion == "" {
		return nil, errors.New("actor route version must not be empty")
	}
	if resolved.typeNameEncoder == nil {
		return nil, errors.New("actor type-name encoder must not be nil")
	}
	return &resolved, nil
}

// TypeName returns the stable actor definition type name.
func (d Definition[S]) TypeName() string {
	return d.typeName
}

// NewIdentity creates an actor identity owned by this definition.
func (d Definition[S]) NewIdentity(id string) (ActorIdentity, error) {
	identity := ActorIdentity{Type: d.typeName, ID: id}
	return identity, identity.Validate()
}

// RouteResolver returns a resolver using this definition's captured routing
// configuration.
func (d Definition[S]) RouteResolver() RouteResolver {
	return definitionRouteResolver{typeName: d.typeName, config: d.config}
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

// EncodeMessage creates a mailbox request for a concrete message. The type ID
// is used only to select the decoder when the request reaches the workflow;
// the workflow still dispatches the decoded value by its Go type.
func (d Definition[S]) EncodeMessage(message any) (MailboxRequest, error) {
	messageType := reflect.TypeOf(message)
	if messageType == nil {
		return MailboxRequest{}, fmt.Errorf("%w: <nil>", ErrNoHandler)
	}
	handler, err := d.selectHandler(messageType)
	if err != nil {
		return MailboxRequest{}, err
	}
	if handler.decode == nil {
		return MailboxRequest{}, fmt.Errorf("message type %s: %w", messageType, ErrNoMailboxDecoder)
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return MailboxRequest{}, fmt.Errorf("encode message type %s: %w", messageType, err)
	}
	return MailboxRequest{TypeID: handler.typeID, Payload: payload}, nil
}

// DecodeMessage decodes a mailbox request into the concrete Go message type
// registered for its transport/schema type ID. The returned value is intended
// to be passed to Dispatch; the type ID is not used for handler selection.
func (d Definition[S]) DecodeMessage(request MailboxRequest) (any, error) {
	handler, ok := d.byTypeID[request.TypeID]
	if !ok {
		return nil, fmt.Errorf("%q: %w", request.TypeID, ErrUnknownMessageTypeID)
	}
	if handler.decode == nil {
		return nil, fmt.Errorf("%q: %w", request.TypeID, ErrNoMailboxDecoder)
	}
	return handler.decode(request.Payload)
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
