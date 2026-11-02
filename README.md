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
