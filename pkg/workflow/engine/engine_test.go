package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
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
