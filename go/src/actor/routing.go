package actor

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	typeMailboxPrefix  = "temporal-actors/actors"
	legacyQueuePrefix  = "temporal-actors/v1/queue"
	fairnessKeyPrefix  = "ta/v1/"
	maxFairnessKeySize = 64
)

// Route contains the Temporal routing metadata for one actor instance.
//
// TaskQueue is the physical queue polled by workers. FairnessKey identifies
// the actor's virtual queue when multiple actors share a TaskQueue. PriorityKey
// and FairnessWeight are workflow-task scheduling metadata; zero values use
// Temporal's defaults.
type Route struct {
	TaskQueue      string
	FairnessKey    string
	FairnessWeight float32
	PriorityKey    int
}

// Validate checks route values before they are sent to Temporal.
func (r Route) Validate() error {
	if r.TaskQueue == "" {
		return errors.New("actor route task queue must not be empty")
	}
	if len(r.TaskQueue) > maxTemporalNameBytes {
		return fmt.Errorf("actor route task queue is %d bytes, maximum is %d", len(r.TaskQueue), maxTemporalNameBytes)
	}
	if len(r.FairnessKey) > maxFairnessKeySize {
		return fmt.Errorf("actor route fairness key is %d bytes, maximum is %d", len(r.FairnessKey), maxFairnessKeySize)
	}
	if r.FairnessWeight != 0 && (math.IsNaN(float64(r.FairnessWeight)) || r.FairnessWeight < 0.001 || r.FairnessWeight > 1000) {
		return fmt.Errorf("actor route fairness weight must be zero or between 0.001 and 1000, got %v", r.FairnessWeight)
	}
	if r.PriorityKey < 0 {
		return fmt.Errorf("actor route priority key must not be negative, got %d", r.PriorityKey)
	}
	return nil
}

// RouteResolver maps a logical actor identity to Temporal routing metadata.
// Implementations must be deterministic and stable for the lifetime of an
// actor instance. Changing a live actor's route requires an explicit migration
// strategy because existing Workflow Tasks may still be associated with the
// previous Task Queue.
type RouteResolver interface {
	Resolve(ActorIdentity) (Route, error)
}

// RouteResolverFunc adapts a function to RouteResolver.
type RouteResolverFunc func(ActorIdentity) (Route, error)

// Resolve implements RouteResolver.
func (f RouteResolverFunc) Resolve(identity ActorIdentity) (Route, error) {
	return f(identity)
}

// TypeMailboxResolver routes all actors of one type to one shared mailbox
// Task Queue. FairnessKey keeps actor instances as separate virtual queues.
type TypeMailboxResolver struct{}

// Resolve returns a shared actor-type mailbox route with equal fairness weight.
func (TypeMailboxResolver) Resolve(identity ActorIdentity) (Route, error) {
	return resolveTypeMailbox(identity, currentConfig())
}

type definitionRouteResolver struct {
	typeName string
	config   *resolvedConfig
}

func (r definitionRouteResolver) Resolve(identity ActorIdentity) (Route, error) {
	if identity.Type != r.typeName {
		return Route{}, fmt.Errorf("actor identity type %q does not match definition type %q", identity.Type, r.typeName)
	}
	return resolveTypeMailbox(identity, r.config)
}

func resolveTypeMailbox(identity ActorIdentity, config *resolvedConfig) (Route, error) {
	if err := identity.Validate(); err != nil {
		return Route{}, err
	}
	encodedType, err := config.typeNameEncoder(identity.Type)
	if err != nil {
		return Route{}, err
	}
	if encodedType == "" || strings.Contains(encodedType, "/") {
		return Route{}, errors.New("actor type-name encoder must return a non-empty queue-safe segment")
	}
	route := Route{
		TaskQueue:      fmt.Sprintf("temporal-actors/%s/actors/%s/mailbox", config.routeVersion, encodedType),
		FairnessKey:    actorFairnessKey(identity),
		FairnessWeight: 1,
	}
	return route, route.Validate()
}

// PerActorResolver preserves the original one-physical-queue-per-actor
// routing strategy. It still supplies a fairness key so the route can be used
// with the same workflow options and observability APIs as shared queues.
type PerActorResolver struct{}

// Resolve returns a dedicated Task Queue route for one actor instance.
func (PerActorResolver) Resolve(identity ActorIdentity) (Route, error) {
	if err := identity.Validate(); err != nil {
		return Route{}, err
	}
	route := Route{
		TaskQueue:      legacyActorQueue(identity),
		FairnessKey:    actorFairnessKey(identity),
		FairnessWeight: 1,
	}
	return route, route.Validate()
}

// DefaultRouteResolver returns the shared actor-type mailbox resolver.
func DefaultRouteResolver() RouteResolver {
	return TypeMailboxResolver{}
}

func legacyActorQueue(identity ActorIdentity) string {
	return fmt.Sprintf("%s/%s/%s", legacyQueuePrefix, encodeIdentityPart(identity.Type), encodeIdentityPart(identity.ID))
}

func actorFairnessKey(identity ActorIdentity) string {
	identityBytes := []byte(identity.Type + "\x00" + identity.ID)
	encoded := base64.RawURLEncoding.EncodeToString(identityBytes)
	key := fairnessKeyPrefix + encoded
	if len(key) <= maxFairnessKeySize {
		return key
	}

	digest := sha256.Sum256(identityBytes)
	return fairnessKeyPrefix + base64.RawURLEncoding.EncodeToString(digest[:])
}
