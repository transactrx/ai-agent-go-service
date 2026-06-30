package loader

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestLoadOne_ParsesRetryPerNode_Smoke(t *testing.T) {
	// Unit-level: confirm rawNode.Retry captures the JSON block. This avoids
	// the need to spin up a full registry / factories for end-to-end loader
	// invocation; the full path is covered by integration tests in Task 21.
	raw := []byte(`{
		"id": "wf",
		"version": 1,
		"trigger": "t1",
		"nodes": [
			{"id":"t1","type":"trigger/nats-chat","config":{}, "retry": {"maxAttempts": 4}}
		],
		"connections": []
	}`)
	var w rawWorkflow
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(w.Nodes) != 1 {
		t.Fatalf("nodes: %d", len(w.Nodes))
	}
	if len(w.Nodes[0].Retry) == 0 {
		t.Fatalf("retry not captured by rawNode")
	}
}

func TestLoadOne_ParsesRetryOnTool_Smoke(t *testing.T) {
	raw := []byte(`{
		"id": "wf",
		"version": 1,
		"trigger": "t1",
		"nodes": [
			{"id":"t1","type":"trigger/nats-chat","config":{}},
			{
				"id":"os1",
				"type":"tool/opensearch",
				"config":{
					"host":"http://example",
					"userEnv":"X","passwordEnv":"Y",
					"toolDescription":"d",
					"allowedIndexPattern":"prod.*"
				},
				"retry": {"maxAttempts": 5, "backoff": "exponential"}
			}
		],
		"connections": []
	}`)
	var w rawWorkflow
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(w.Nodes) != 2 {
		t.Fatalf("nodes: %d", len(w.Nodes))
	}
	if len(w.Nodes[1].Retry) == 0 {
		t.Fatalf("retry not captured on tool/opensearch")
	}
}

// roleStub is a minimal node.Node test double whose Spec().Role is
// configurable. Used only for parseRetryByNode role-rejection tests.
type roleStub struct{ role node.Role }

func (s roleStub) Spec() node.NodeSpec                        { return node.NodeSpec{Role: s.role} }
func (roleStub) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (roleStub) Close(_ context.Context) error                { return nil }

func TestParseRetryByNode_AllowsTool(t *testing.T) {
	raws := []rawNode{
		{ID: "x", Type: "tool/x", Retry: []byte(`{"maxAttempts":3}`)},
	}
	instances := map[string]node.Node{"x": roleStub{role: node.RoleTool}}
	out, err := parseRetryByNode(raws, instances)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out["x"] == nil {
		t.Fatal("expected parsed policy")
	}
}

func TestParseRetryByNode_AllowsAgent(t *testing.T) {
	raws := []rawNode{
		{ID: "a", Type: "ai/agent", Retry: []byte(`{"maxAttempts":3}`)},
	}
	instances := map[string]node.Node{"a": roleStub{role: node.RoleAgent}}
	if _, err := parseRetryByNode(raws, instances); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestParseRetryByNode_AllowsLLM(t *testing.T) {
	raws := []rawNode{
		{ID: "l", Type: "ai/bedrock", Retry: []byte(`{"maxAttempts":3}`)},
	}
	instances := map[string]node.Node{"l": roleStub{role: node.RoleLLM}}
	if _, err := parseRetryByNode(raws, instances); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestParseRetryByNode_RejectsMemory(t *testing.T) {
	raws := []rawNode{
		{ID: "m", Type: "memory/postgres", Retry: []byte(`{"maxAttempts":3}`)},
	}
	instances := map[string]node.Node{"m": roleStub{role: node.RoleMemory}}
	_, err := parseRetryByNode(raws, instances)
	if err == nil {
		t.Fatal("expected rejection error")
	}
	if !strings.Contains(err.Error(), "retry is not supported for role=memory") {
		t.Errorf("error message: %v", err)
	}
}

func TestParseRetryByNode_RejectsTrigger(t *testing.T) {
	raws := []rawNode{
		{ID: "t", Type: "trigger/nats-chat", Retry: []byte(`{"maxAttempts":3}`)},
	}
	instances := map[string]node.Node{"t": roleStub{role: node.RoleTrigger}}
	_, err := parseRetryByNode(raws, instances)
	if err == nil || !strings.Contains(err.Error(), "role=trigger") {
		t.Errorf("got %v", err)
	}
}

func TestParseRetryByNode_RejectsPolicy(t *testing.T) {
	raws := []rawNode{
		{ID: "p", Type: "policy/x", Retry: []byte(`{"maxAttempts":3}`)},
	}
	instances := map[string]node.Node{"p": roleStub{role: node.RolePolicy}}
	_, err := parseRetryByNode(raws, instances)
	if err == nil || !strings.Contains(err.Error(), "role=policy") {
		t.Errorf("got %v", err)
	}
}

func TestParseRetryByNode_SkipsNodesWithoutRetry(t *testing.T) {
	raws := []rawNode{
		{ID: "a", Type: "tool/x"},
		{ID: "b", Type: "memory/x"}, // memory but no retry block → no rejection
	}
	instances := map[string]node.Node{
		"a": roleStub{role: node.RoleTool},
		"b": roleStub{role: node.RoleMemory},
	}
	out, err := parseRetryByNode(raws, instances)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty map, got %d", len(out))
	}
}

func TestParseRetryByNode_PropagatesParseError(t *testing.T) {
	raws := []rawNode{
		{ID: "x", Type: "tool/x", Retry: []byte(`{"maxAttempts":99}`)}, // out of range
	}
	instances := map[string]node.Node{"x": roleStub{role: node.RoleTool}}
	_, err := parseRetryByNode(raws, instances)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "maxAttempts") {
		t.Errorf("got %v", err)
	}
}
