# Temporal Actors

Temporal Actors aims to make writing actors on top of [Temporal](https://temporal.io/) easy and painless.

The project will provide an Erlang-style actor model in which an actor can define multiple functions for handling messages. When a message arrives, the runtime will select the appropriate handler based on the handler's signature.

The runtime will also manage each actor's state. It will provide the current state to the selected message handler and use the updated state returned by that handler for subsequent messages.

## Running samples

The sample runner assumes a local Temporal development server is already running via `temporal server start-dev`. Start that in one terminal, then run a sample from the repository root in another:

```sh
./init run_sample go summation
```

The shorthand `./init go summation` is also supported. The Go samples connect to `localhost:7233` in the `default` namespace.

## Actor handler contract

The runtime groups handlers for one actor type into an immutable `actor.Definition[S]`. The definition owns handler registration; each actor instance owns its own explicit state and identity.

Commands use the state transition shape:

```go
func(actor.Context, S, M) S
```

Sequenced read requests use:

```go
func(actor.Context, S, M) (R, error)
```

Commands and requests are both intended to be processed through the actor's serialized mailbox. A request is not the same as a Temporal Query: it must observe all earlier mailbox messages before producing its result.

Handler selection is based on the Go message type, not on a handler name. Exact types take precedence over matching interfaces; more-specific interfaces take precedence over broader interfaces; ambiguous matches are errors. Marker interfaces are optional domain organization and are not required by the runtime.

Definitions are immutable after construction. Handler functions must be deterministic, replay-safe, and free of mutable closure or receiver-owned actor state. The explicit state argument and returned state are the only supported actor-state transition.

The runtime does not use handler names as its primary lookup key. If a future serialized mailbox needs a discriminator to decode a message, that discriminator is a wire-format concern; dispatch still uses the decoded Go message type. The `temporal-actors/v1` prefix in actor Workflow IDs and Task Queue names is versioned because those names are durable Temporal compatibility boundaries.

The common mailbox Update adapter is synchronous by contract. Actor handlers must not yield the Temporal scheduler, block on wall-clock or external operations, start Activities or child Workflows, use workflow channels, or start goroutines. Temporal supports more capable Update handlers, but those capabilities would violate this actor runtime's one-message-at-a-time invariant.
