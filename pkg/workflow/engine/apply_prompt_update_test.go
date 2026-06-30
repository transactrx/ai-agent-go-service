package engine

import (
	"context"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestApplyPromptUpdate(t *testing.T) {
	// Minimal: a Workflow whose node implements Reconfigurable.
	rc := &reconfigurableFake{}
	e := &Engine{workflows: map[string]*Workflow{
		"wf1": {ID: "wf1", Nodes: map[string]node.Node{"agent1": rc}},
	}}
	if err := e.ApplyPromptUpdate("wf1", "agent1", "systemMessageFlexible", "new"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if rc.gotField != "systemMessageFlexible" || rc.gotValue != "new" {
		t.Fatalf("reconfigure not called: %+v", rc)
	}
	if err := e.ApplyPromptUpdate("nope", "agent1", "f", "v"); err == nil {
		t.Fatal("want unknown-workflow error")
	}
	if err := e.ApplyPromptUpdate("wf1", "nope", "f", "v"); err == nil {
		t.Fatal("want unknown-node error")
	}
}

type reconfigurableFake struct {
	gotField, gotValue string
}

func (r *reconfigurableFake) Spec() node.NodeSpec                      { return node.NodeSpec{} }
func (r *reconfigurableFake) Init(context.Context, node.NodeEnv) error { return nil }
func (r *reconfigurableFake) Close(context.Context) error              { return nil }
func (r *reconfigurableFake) Reconfigure(field, value string) error {
	r.gotField, r.gotValue = field, value
	return nil
}
