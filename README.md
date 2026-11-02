# Temporal Actors

Temporal Actors aims to make writing actors on top of [Temporal](https://temporal.io/) easy and painless.

The project will provide an Erlang-style actor model in which an actor can define multiple functions for handling messages. When a message arrives, the runtime will select the appropriate handler based on the handler's signature.

The runtime will also manage each actor's state. It will provide the current state to the selected message handler and use the updated state returned by that handler for subsequent messages.
