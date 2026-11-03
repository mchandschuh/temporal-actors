package actor

// Context contains stable, actor-scoped runtime metadata.
//
// Context deliberately does not contain actor state or the current message.
// The state transition is represented only by the handler's explicit
// (Context, state, message) -> nextState signature. A Context is constructed
// once for an actor workflow and reused for every message.
type Context struct {
	identity   ActorIdentity
	workflowID string
}

func newContext(identity ActorIdentity) Context {
	return Context{
		identity:   identity,
		workflowID: identity.WorkflowID(),
	}
}

// Identity returns the stable identity of the actor being executed.
func (c Context) Identity() ActorIdentity {
	return c.identity
}

// WorkflowID returns the Temporal Workflow ID conventionally associated with
// the actor identity.
func (c Context) WorkflowID() string {
	return c.workflowID
}
