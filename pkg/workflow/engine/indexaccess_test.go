package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// iaTrigger records the order of SetIndexAccess and Subscribe.
type iaTrigger struct {
	mu  sync.Mutex
	log []string
}

func (*iaTrigger) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/ia-trigger", Role: node.RoleTrigger,
		OutputPorts: []node.PortSpec{{Name: "main", Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (*iaTrigger) Init(context.Context, node.NodeEnv) error { return nil }
func (*iaTrigger) Close(context.Context) error              { return nil }
func (t *iaTrigger) SetIndexAccess(a node.IndexAccess) {
	t.mu.Lock()
	t.log = append(t.log, "set:"+string(a.Kind()))
	t.mu.Unlock()
}
func (t *iaTrigger) Subscribe(context.Context, node.TriggerSink) error {
	t.mu.Lock()
	t.log = append(t.log, "subscribe")
	t.mu.Unlock()
	return nil
}
func (t *iaTrigger) events() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.log...)
}

type iaAgent struct{}

func (iaAgent) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/ia-agent", Role: node.RoleAgent,
		InputPorts:  []node.PortSpec{{Name: "main", Direction: node.PortIn, Cardinality: node.CardOne, Required: true}},
		OutputPorts: []node.PortSpec{{Name: "main", Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (iaAgent) Init(context.Context, node.NodeEnv) error { return nil }
func (iaAgent) Close(context.Context) error              { return nil }
func (iaAgent) Process(_ context.Context, _ node.AgentInput, sink node.StreamSink) error {
	return sink.Close(context.Background(), node.StreamEvent{Type: node.StreamComplete, Data: []byte(`{"finalText":"ok"}`)})
}

type iaEngTool struct{ pattern string }

func (iaEngTool) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/ia-tool", Role: node.RoleTool,
		InputPorts: []node.PortSpec{{Name: node.PortPolicy, Direction: node.PortIn, Cardinality: node.CardOne, Required: true, PeerRole: node.RolePolicy}}}
}
func (iaEngTool) Init(context.Context, node.NodeEnv) error { return nil }
func (iaEngTool) Close(context.Context) error              { return nil }
func (t iaEngTool) AllowedIndexPattern() string            { return t.pattern }

type iaEngPolicy struct {
	fn   string
	fail bool
}

func (iaEngPolicy) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/ia-policy", Role: node.RolePolicy,
		OutputPorts: []node.PortSpec{{Name: node.PortPolicy, Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (iaEngPolicy) Init(context.Context, node.NodeEnv) error { return nil }
func (iaEngPolicy) Close(context.Context) error              { return nil }
func (iaEngPolicy) Resolve(context.Context, node.PolicyRequest) (node.PolicyResult, error) {
	return node.PolicyResult{}, nil
}
func (p iaEngPolicy) FunctionForIndex(context.Context, string) (string, error) {
	if p.fail {
		return "", errors.New("no powerlineindexconfig row")
	}
	return p.fn, nil
}

const iaIndexed = `{
  "id": "wf", "version": 1, "trigger": "trig",
  "nodes": [
    {"id": "trig",  "type": "test/ia-trigger", "config": {}},
    {"id": "agent", "type": "test/ia-agent", "config": {}},
    {"id": "tool",  "type": "test/ia-tool", "config": {"pattern": "dev.cpe-*"}},
    {"id": "pol",   "type": "test/ia-policy", "config": %s}
  ],
  "connections": [
    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}},
    {"from": {"node": "pol", "port": "policy"}, "to": {"node": "tool", "port": "policy"}}
  ]
}`

const iaPlain = `{
  "id": "wf", "version": 1, "trigger": "trig",
  "nodes": [
    {"id": "trig",  "type": "test/ia-trigger", "config": {}},
    {"id": "agent", "type": "test/ia-agent", "config": {}}
  ],
  "connections": [
    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}
  ]
}`

// loadIA loads one workflow and returns it, its trigger and the engine log.
func loadIA(t *testing.T, wfJSON string) (*engine.Workflow, *iaTrigger, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wf.json"), []byte(wfJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	trig := &iaTrigger{}
	reg := node.NewRegistry()
	_ = reg.Register("test/ia-trigger", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return trig, nil }))
	_ = reg.Register("test/ia-agent", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return iaAgent{}, nil }))
	_ = reg.Register("test/ia-tool", node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
		var c struct {
			Pattern string `json:"pattern"`
		}
		_ = json.Unmarshal(raw, &c)
		return iaEngTool{pattern: c.Pattern}, nil
	}))
	_ = reg.Register("test/ia-policy", node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
		var c struct {
			Function string `json:"function"`
			Fail     bool   `json:"fail"`
		}
		_ = json.Unmarshal(raw, &c)
		return iaEngPolicy{fn: c.Function, fail: c.Fail}, nil
	}))
	var buf bytes.Buffer
	eng, err := engine.New(engine.Config{Source: engine.NewFilesystemSource(dir), Registry: reg, Logger: log.New(&buf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	wf, ok := eng.Workflow("wf")
	if !ok {
		t.Fatalf("workflow did not load; log:\n%s", buf.String())
	}
	return wf, trig, buf.String()
}

func TestEngineResolvesBoundAccessBeforeTheTriggerSubscribes(t *testing.T) {
	wf, trig, logs := loadIA(t, fmt.Sprintf(iaIndexed, `{"function":"FN-1"}`))
	if wf.IndexAccess.Status != node.IndexAccessBound || wf.IndexAccess.FunctionID != "FN-1" || wf.IndexAccess.Index != "dev.cpe-*" {
		t.Fatalf("IndexAccess = %+v", wf.IndexAccess)
	}
	if got := trig.events(); len(got) != 2 || got[0] != "set:bound" || got[1] != "subscribe" {
		t.Fatalf("trigger events = %v, want [set:bound subscribe]", got)
	}
	if !strings.Contains(logs, "engine: workflow=wf index=dev.cpe-* access=bound function=FN-1") {
		t.Fatalf("missing load log line; log:\n%s", logs)
	}
}

func TestEngineLoadsAnUnboundWorkflowAndTellsTheTrigger(t *testing.T) {
	wf, trig, logs := loadIA(t, fmt.Sprintf(iaIndexed, `{"fail":true}`))
	if wf.IndexAccess.Status != node.IndexAccessUnbound || !strings.Contains(wf.IndexAccess.Cause, "no powerlineindexconfig row") {
		t.Fatalf("IndexAccess = %+v", wf.IndexAccess)
	}
	if got := trig.events(); len(got) != 2 || got[0] != "set:unbound" {
		t.Fatalf("trigger events = %v", got)
	}
	if !strings.Contains(logs, "access=unbound") {
		t.Fatalf("missing unbound log line; log:\n%s", logs)
	}
}

func TestEngineWorkflowWithoutAnIndexIsNone(t *testing.T) {
	wf, trig, _ := loadIA(t, iaPlain)
	if wf.IndexAccess.Kind() != node.IndexAccessNone {
		t.Fatalf("IndexAccess = %+v", wf.IndexAccess)
	}
	if got := trig.events(); len(got) != 2 || got[0] != "set:none" {
		t.Fatalf("trigger events = %v", got)
	}
}
