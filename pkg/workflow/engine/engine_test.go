package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeTrigger emits no events on its own; tests call SinkFn directly.
type fakeTrigger struct {
	sink node.TriggerSink
}

func (fakeTrigger) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "test/trigger", Role: node.RoleTrigger,
		OutputPorts: []node.PortSpec{{Name: "main", Direction: node.PortOut, Cardinality: node.CardOne}},
	}
}
func (fakeTrigger) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (fakeTrigger) Close(_ context.Context) error                { return nil }
func (f *fakeTrigger) Subscribe(_ context.Context, sink node.TriggerSink) error {
	f.sink = sink
	return nil
}

// fakeAgent emits a single complete event to its sink.
type fakeAgent struct {
	processed sync.WaitGroup
}

func (fakeAgent) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "test/agent", Role: node.RoleAgent,
		InputPorts:  []node.PortSpec{{Name: "main", Direction: node.PortIn, Cardinality: node.CardOne, Required: true}},
		OutputPorts: []node.PortSpec{{Name: "main", Direction: node.PortOut, Cardinality: node.CardOne}},
	}
}
func (fakeAgent) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (fakeAgent) Close(_ context.Context) error                { return nil }
func (a *fakeAgent) Process(_ context.Context, _ node.AgentInput, sink node.StreamSink) error {
	defer a.processed.Done()
	if sink == nil {
		return errors.New("nil sink")
	}
	return sink.Close(context.Background(), node.StreamEvent{Type: node.StreamComplete, Data: []byte(`{"finalText":"ok"}`)})
}

func TestEngineLoadsWorkflowAndRoutesTriggerEventToAgent(t *testing.T) {
	dir := t.TempDir()
	wfJSON := `{
	  "id": "wf1",
	  "version": 1,
	  "trigger": "trig",
	  "nodes": [
	    {"id": "trig",  "type": "test/trigger", "config": {}},
	    {"id": "agent", "type": "test/agent",   "config": {}}
	  ],
	  "connections": [
	    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}
	  ]
	}`
	if err := os.WriteFile(filepath.Join(dir, "wf1.json"), []byte(wfJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	reg := node.NewRegistry()
	trig := &fakeTrigger{}
	ag := &fakeAgent{}
	_ = reg.Register("test/trigger", node.FactoryFunc(func(_ json.RawMessage) (node.Node, error) { return trig, nil }))
	_ = reg.Register("test/agent", node.FactoryFunc(func(_ json.RawMessage) (node.Node, error) { return ag, nil }))

	logger := log.New(io.Discard, "", 0)
	eng, err := engine.New(engine.Config{
		Source:   engine.NewFilesystemSource(dir),
		Registry: reg,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := eng.Workflow("wf1"); !ok {
		t.Fatal("wf1 not registered")
	}

	// Drive a synthetic trigger event through the sink.
	ag.processed.Add(1)
	sinkRecorder := &recordingSink{}
	if err := trig.sink.Emit(context.Background(), node.TriggerEvent{
		RequestID:  "r1",
		SessionID:  "s1",
		Body:       []byte(`{"message":"hi"}`),
		StreamSink: sinkRecorder,
	}); err != nil {
		t.Fatal(err)
	}
	ag.processed.Wait()

	if !sinkRecorder.closed {
		t.Fatal("agent did not close sink")
	}
}

// writeFile writes content to dir/name, failing the test on error.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// newTestEngine builds an Engine over a FilesystemSource rooted at dir, with
// the fake trigger/agent node types registered.
func newTestEngine(t *testing.T, dir string) *engine.Engine {
	t.Helper()
	return newTestEngineWithLogger(t, dir, log.New(io.Discard, "", 0))
}

// newTestEngineWithLogger is like newTestEngine but lets the caller supply
// (and later inspect) the logger — used to assert on log output.
func newTestEngineWithLogger(t *testing.T, dir string, logger *log.Logger) *engine.Engine {
	t.Helper()
	reg := node.NewRegistry()
	_ = reg.Register("test/trigger", node.FactoryFunc(func(_ json.RawMessage) (node.Node, error) { return &fakeTrigger{}, nil }))
	_ = reg.Register("test/agent", node.FactoryFunc(func(_ json.RawMessage) (node.Node, error) { return &fakeAgent{}, nil }))
	eng, err := engine.New(engine.Config{
		Source:   engine.NewFilesystemSource(dir),
		Registry: reg,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

const testBaseWorkflow = `{
  "id": "base", "version": 1, "trigger": "t1",
  "nodes": [
    {"id": "t1", "type": "test/trigger", "config": {"mode": "streaming"}},
    {"id": "a1", "type": "test/agent", "config": {"greeting": "bf"}}
  ],
  "connections": [{"from": {"node": "t1", "port": "main"}, "to": {"node": "a1", "port": "main"}}]
}`

func TestLoadAllResolvesDerivedWorkflow(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "base.json", testBaseWorkflow)
	writeFile(t, dir, "derived.json", `{
	  "id": "derived", "extends": "base",
	  "nodes": [
	    {"id": "t1", "config": {"mode": "single"}},
	    {"id": "a1", "config": {"greeting": "df"}}
	  ]
	}`)
	eng := newTestEngine(t, dir)
	if err := eng.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := eng.Workflow("base"); !ok {
		t.Fatal("base workflow missing")
	}
	if _, ok := eng.Workflow("derived"); !ok {
		t.Fatal("derived workflow missing")
	}
}

func TestLoadAllDerivedFailuresAreIsolated(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "base.json", testBaseWorkflow)
	writeFile(t, dir, "orphan.json", `{"id": "orphan", "extends": "missing"}`)
	writeFile(t, dir, "chained.json", `{"id": "chained", "extends": "orphan"}`)

	var logBuf bytes.Buffer
	eng := newTestEngineWithLogger(t, dir, log.New(&logBuf, "", 0))
	_ = eng.LoadAll(context.Background())
	if _, ok := eng.Workflow("base"); !ok {
		t.Fatal("healthy base must load despite broken derived siblings")
	}
	if _, ok := eng.Workflow("orphan"); ok {
		t.Fatal("workflow with missing base must not register")
	}
	if _, ok := eng.Workflow("chained"); ok {
		t.Fatal("chained extends must not register")
	}

	logOutput := logBuf.String()
	const wantMissingBase = `extends "missing": base workflow not found`
	if !strings.Contains(logOutput, wantMissingBase) {
		t.Fatalf("log output missing exact missing-base message %q; got:\n%s", wantMissingBase, logOutput)
	}
	const wantChained = `extends "orphan": base is itself derived (chained extends is not supported)`
	if !strings.Contains(logOutput, wantChained) {
		t.Fatalf("log output missing exact chained-extends message %q; got:\n%s", wantChained, logOutput)
	}
}

type recordingSink struct {
	mu     sync.Mutex
	events []node.StreamEvent
	closed bool
	term   node.StreamEvent
}

func (s *recordingSink) Send(_ context.Context, evt node.StreamEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}
func (s *recordingSink) Close(_ context.Context, term node.StreamEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.term = term
	return nil
}
