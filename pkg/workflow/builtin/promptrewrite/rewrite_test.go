package promptrewrite

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateRequest(t *testing.T) {
	if err := validateRequest(RewriteRequest{}); err == nil {
		t.Fatal("want error: workflowId required")
	}
	if err := validateRequest(RewriteRequest{WorkflowID: "batchAssistant"}); err == nil {
		t.Fatal("want error: instructions required")
	}
	ok := RewriteRequest{WorkflowID: "batchAssistant", NodeID: "agent1",
		Field: "systemMessageFlexible", Instructions: "add X"}
	if err := validateRequest(ok); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestBuildMessagesPreservesCurrentPrompt(t *testing.T) {
	sys, user := buildMessages("CURRENT-TEXT", "make it friendlier")
	if !strings.Contains(sys, "PRESERVE") {
		t.Fatal("meta system prompt must carry the PRESERVE rule")
	}
	if !strings.Contains(user, "CURRENT-TEXT") || !strings.Contains(user, "make it friendlier") {
		t.Fatal("user message must embed current prompt and instructions")
	}
}

func TestNodeSpecHasNoPorts(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	s := n.Spec()
	if len(s.InputPorts) != 0 || len(s.OutputPorts) != 0 {
		t.Fatal("prompt-rewrite node must declare no ports (loads unconnected)")
	}
}
