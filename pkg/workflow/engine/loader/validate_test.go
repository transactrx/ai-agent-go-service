package loader_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/loader"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// stubNode implements every role-specific interface for the test scenarios.
type stubNode struct {
	role   node.Role
	ports  []node.PortSpec
	output []node.PortSpec
}

func (s stubNode) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/" + string(s.role), Role: s.role, InputPorts: s.ports, OutputPorts: s.output}
}
func (stubNode) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (stubNode) Close(_ context.Context) error                { return nil }
func (stubNode) Subscribe(_ context.Context, _ node.TriggerSink) error {
	return nil
}
func (stubNode) Stream(_ context.Context, _ node.LLMRequest, _ chan<- node.LLMEvent) error {
	return nil
}
func (stubNode) Load(_ context.Context, _ node.MemoryKey) ([]node.Message, error) { return nil, nil }
func (stubNode) Append(_ context.Context, _ node.MemoryKey, _ node.Turn) error    { return nil }
func (stubNode) ToolSpec() node.ToolSpec                                          { return node.ToolSpec{} }
func (stubNode) Invoke(_ context.Context, _ json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}
func (stubNode) FailurePolicy() node.FailurePolicy                                { return node.FailureSurfaceToLLM }
func (stubNode) RetryPolicy() node.RetryPolicy                                    { return nil }
func (stubNode) Process(_ context.Context, _ node.AgentInput, _ node.StreamSink) error {
	return nil
}
func (stubNode) Resolve(_ context.Context, _ node.PolicyRequest) (node.PolicyResult, error) {
	return node.PolicyResult{}, nil
}

func portIn(name string, peer node.Role, card node.Cardinality, required bool) node.PortSpec {
	return node.PortSpec{Name: name, Direction: node.PortIn, Cardinality: card, Required: required, PeerRole: peer}
}
func portOut(name string) node.PortSpec {
	return node.PortSpec{Name: name, Direction: node.PortOut, Cardinality: node.CardOne}
}

func TestValidateConnectionsHappyPath(t *testing.T) {
	nodes := map[string]node.Node{
		"trig":  stubNode{role: node.RoleTrigger, output: []node.PortSpec{portOut("main")}},
		"agent": stubNode{role: node.RoleAgent, ports: []node.PortSpec{
			portIn("main", "", node.CardOne, true),
			portIn("ai_languageModel", node.RoleLLM, node.CardOne, true),
		}, output: []node.PortSpec{portOut("main")}},
		"llm": stubNode{role: node.RoleLLM, output: []node.PortSpec{portOut("ai_languageModel")}},
	}
	conns := []loader.RawConn{
		{From: loader.Endpoint{Node: "trig", Port: "main"}, To: loader.Endpoint{Node: "agent", Port: "main"}},
		{From: loader.Endpoint{Node: "llm", Port: "ai_languageModel"}, To: loader.Endpoint{Node: "agent", Port: "ai_languageModel"}},
	}
	if errs := loader.ValidateConnections(nodes, conns); len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
}

func TestValidateMissingRequiredPort(t *testing.T) {
	nodes := map[string]node.Node{
		"agent": stubNode{role: node.RoleAgent, ports: []node.PortSpec{
			portIn("ai_languageModel", node.RoleLLM, node.CardOne, true),
		}},
	}
	errs := loader.ValidateConnections(nodes, nil)
	if len(errs) == 0 {
		t.Fatal("expected required-port error")
	}
	if !strings.Contains(errs[0].Error(), "ai_languageModel") {
		t.Fatalf("err: %v", errs[0])
	}
}

func TestValidateCardinalityOneRejectsTwo(t *testing.T) {
	nodes := map[string]node.Node{
		"agent": stubNode{role: node.RoleAgent, ports: []node.PortSpec{
			portIn("ai_languageModel", node.RoleLLM, node.CardOne, true),
		}},
		"a": stubNode{role: node.RoleLLM, output: []node.PortSpec{portOut("ai_languageModel")}},
		"b": stubNode{role: node.RoleLLM, output: []node.PortSpec{portOut("ai_languageModel")}},
	}
	conns := []loader.RawConn{
		{From: loader.Endpoint{Node: "a", Port: "ai_languageModel"}, To: loader.Endpoint{Node: "agent", Port: "ai_languageModel"}},
		{From: loader.Endpoint{Node: "b", Port: "ai_languageModel"}, To: loader.Endpoint{Node: "agent", Port: "ai_languageModel"}},
	}
	errs := loader.ValidateConnections(nodes, conns)
	if len(errs) == 0 {
		t.Fatal("expected cardinality error")
	}
}

func TestValidateUnknownPort(t *testing.T) {
	nodes := map[string]node.Node{
		"agent": stubNode{role: node.RoleAgent, ports: []node.PortSpec{portIn("main", "", node.CardOne, true)}},
		"a":     stubNode{role: node.RoleTrigger, output: []node.PortSpec{portOut("main")}},
	}
	conns := []loader.RawConn{
		{From: loader.Endpoint{Node: "a", Port: "main"}, To: loader.Endpoint{Node: "agent", Port: "wat"}},
	}
	errs := loader.ValidateConnections(nodes, conns)
	if len(errs) == 0 {
		t.Fatal("expected unknown-port error")
	}
}

func TestValidatePeerRoleMismatch(t *testing.T) {
	nodes := map[string]node.Node{
		"agent": stubNode{role: node.RoleAgent, ports: []node.PortSpec{
			portIn("ai_languageModel", node.RoleLLM, node.CardOne, true),
		}},
		"wrong": stubNode{role: node.RoleMemory, output: []node.PortSpec{portOut("ai_languageModel")}},
	}
	conns := []loader.RawConn{
		{From: loader.Endpoint{Node: "wrong", Port: "ai_languageModel"}, To: loader.Endpoint{Node: "agent", Port: "ai_languageModel"}},
	}
	errs := loader.ValidateConnections(nodes, conns)
	if len(errs) == 0 {
		t.Fatal("expected peer-role error")
	}
}

func TestValidateTopoCycleRejected(t *testing.T) {
	nodes := map[string]node.Node{
		"a": stubNode{role: node.RoleAgent, ports: []node.PortSpec{portIn("main", "", node.CardOne, true)}, output: []node.PortSpec{portOut("main")}},
		"b": stubNode{role: node.RoleAgent, ports: []node.PortSpec{portIn("main", "", node.CardOne, true)}, output: []node.PortSpec{portOut("main")}},
	}
	conns := []loader.RawConn{
		{From: loader.Endpoint{Node: "a", Port: "main"}, To: loader.Endpoint{Node: "b", Port: "main"}},
		{From: loader.Endpoint{Node: "b", Port: "main"}, To: loader.Endpoint{Node: "a", Port: "main"}},
	}
	_, err := loader.TopoOrder(nodes, conns)
	if err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestTopoOrderHappyPath(t *testing.T) {
	nodes := map[string]node.Node{
		"trig":  stubNode{role: node.RoleTrigger, output: []node.PortSpec{portOut("main")}},
		"agent": stubNode{role: node.RoleAgent, ports: []node.PortSpec{portIn("main", "", node.CardOne, true)}, output: []node.PortSpec{portOut("main")}},
	}
	conns := []loader.RawConn{
		{From: loader.Endpoint{Node: "trig", Port: "main"}, To: loader.Endpoint{Node: "agent", Port: "main"}},
	}
	order, err := loader.TopoOrder(nodes, conns)
	if err != nil {
		t.Fatal(err)
	}
	if order[0] != "trig" || order[1] != "agent" {
		t.Fatalf("order: %v", order)
	}
}
