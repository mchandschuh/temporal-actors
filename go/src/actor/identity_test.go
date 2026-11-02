package actor

import (
	"strings"
	"testing"
)

func TestActorIdentityNamesAreStableAndUnambiguous(t *testing.T) {
	identity := ActorIdentity{Type: "billing/account", ID: "customer/42"}

	workflowID := identity.WorkflowID()
	taskQueue := identity.TaskQueue()
	if workflowID == taskQueue {
		t.Fatal("workflow ID and task queue must be distinct")
	}
	if !strings.HasPrefix(workflowID, "temporal-actors/v1/workflow/") {
		t.Fatalf("unexpected workflow ID %q", workflowID)
	}
	if !strings.HasPrefix(taskQueue, "temporal-actors/v1/queue/") {
		t.Fatalf("unexpected task queue %q", taskQueue)
	}
	if strings.Contains(workflowID, "/billing/account/") || strings.Contains(taskQueue, "/customer/42") {
		t.Fatalf("identity components must be encoded: workflow=%q queue=%q", workflowID, taskQueue)
	}

	if (ActorIdentity{Type: "a/b", ID: "c"}).WorkflowID() == (ActorIdentity{Type: "a", ID: "b/c"}).WorkflowID() {
		t.Fatal("identity component boundaries must not collide")
	}
}

func TestActorIdentityValidation(t *testing.T) {
	for name, identity := range map[string]ActorIdentity{
		"missing type": {ID: "id"},
		"missing ID":   {Type: "type"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := identity.Validate(); err == nil {
				t.Fatal("expected invalid identity")
			}
		})
	}

	tooLong := ActorIdentity{Type: "summation", ID: strings.Repeat("x", 1000)}
	if err := tooLong.Validate(); err == nil {
		t.Fatal("expected oversized identity to be rejected")
	}
}

func TestWorkflowOptionsUseIdentityNames(t *testing.T) {
	identity := ActorIdentity{Type: "summation", ID: "account-42"}
	options, err := WorkflowOptions(identity)
	if err != nil {
		t.Fatal(err)
	}
	if options.ID != identity.WorkflowID() {
		t.Fatalf("got workflow ID %q, want %q", options.ID, identity.WorkflowID())
	}
	if options.TaskQueue != identity.TaskQueue() {
		t.Fatalf("got task queue %q, want %q", options.TaskQueue, identity.TaskQueue())
	}
}
