package promptadmin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// overridableFake is a minimal node.Node declaring one overridable field.
type overridableFake struct{}

func (overridableFake) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/overridable", Role: node.RoleTool,
		OverridableFields: []string{"systemMessageFlexible"}}
}
func (overridableFake) Init(context.Context, node.NodeEnv) error { return nil }
func (overridableFake) Close(context.Context) error             { return nil }

// newTestService builds a service over a hand-built engine. NewForTest is a
// small exported engine helper (engine.workflows is unexported, so an external
// package can't construct it directly) — least invasive way to seed workflows.
func newTestService(workflows map[string]*engine.Workflow) *service {
	return &service{deps: Deps{Engine: engine.NewForTest(workflows)}}
}

func TestNodeFieldOverridable(t *testing.T) {
	svc := newTestService(map[string]*engine.Workflow{
		"wf1": {ID: "wf1", Nodes: map[string]node.Node{"agent1": overridableFake{}}},
	})

	if _, err := svc.nodeFieldOverridable("wf1", "agent1", "systemMessageFlexible"); err != nil {
		t.Fatalf("declared overridable field: unexpected error %v", err)
	}
	if _, err := svc.nodeFieldOverridable("wf1", "agent1", "systemMessage"); err == nil {
		t.Fatal("undeclared field systemMessage: want error, got nil")
	}
	if _, err := svc.nodeFieldOverridable("wf1", "nope", "systemMessageFlexible"); err == nil {
		t.Fatal("unknown node: want error, got nil")
	}
	if _, err := svc.nodeFieldOverridable("nope", "agent1", "systemMessageFlexible"); err == nil {
		t.Fatal("unknown workflow: want error, got nil")
	}
}

func TestRawDefault_ReadsWorkflowJSONValue(t *testing.T) {
	svc := newTestService(map[string]*engine.Workflow{
		"wf1": {
			ID:    "wf1",
			Nodes: map[string]node.Node{"agent1": overridableFake{}},
			RawConfigs: map[string]json.RawMessage{
				"agent1": json.RawMessage(`{"systemMessage":"F","systemMessageFlexible":"D"}`),
			},
		},
	})

	if got := svc.rawDefault("wf1", "agent1", "systemMessageFlexible"); got != "D" {
		t.Fatalf("rawDefault systemMessageFlexible = %q, want D", got)
	}
	if got := svc.rawDefault("wf1", "agent1", "systemMessage"); got != "F" {
		t.Fatalf("rawDefault systemMessage = %q, want F", got)
	}
	if got := svc.rawDefault("nope", "agent1", "systemMessageFlexible"); got != "" {
		t.Fatalf("rawDefault unknown workflow = %q, want empty", got)
	}
	if got := svc.rawDefault("wf1", "nope", "systemMessageFlexible"); got != "" {
		t.Fatalf("rawDefault unknown node = %q, want empty", got)
	}
}
