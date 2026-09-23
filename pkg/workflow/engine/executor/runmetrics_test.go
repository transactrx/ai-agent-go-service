package executor

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// streamingAgent emits a tool call, a delta, then the terminator.
type streamingAgent struct {
	term node.StreamEvent
}

func (a *streamingAgent) Spec() node.NodeSpec                      { return node.NodeSpec{Type: "ai/agent", Role: node.RoleAgent} }
func (a *streamingAgent) Init(context.Context, node.NodeEnv) error { return nil }
func (a *streamingAgent) Close(context.Context) error              { return nil }
func (a *streamingAgent) Process(ctx context.Context, _ node.AgentInput, sink node.StreamSink) error {
	_ = sink.Send(ctx, node.StreamEvent{Type: node.StreamToolCall, Data: []byte(`{"name":"T"}`)})
	_ = sink.Send(ctx, node.StreamEvent{Type: node.StreamDelta, Data: []byte(`{"text":"hi"}`)})
	return sink.Close(ctx, a.term)
}

type nopSink struct {
	mu     sync.Mutex
	events int
	term   node.StreamEvent
}

func (s *nopSink) Send(context.Context, node.StreamEvent) error { s.mu.Lock(); s.events++; s.mu.Unlock(); return nil }
func (s *nopSink) Close(_ context.Context, t node.StreamEvent) error {
	s.mu.Lock()
	s.term = t
	s.mu.Unlock()
	return nil
}

// fakeClock advances 10ms per reading.
func fakeClock() func() time.Time {
	var mu sync.Mutex
	t := time.Unix(0, 0)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t = t.Add(10 * time.Millisecond)
		return t
	}
}

func runWithMetrics(t *testing.T, term node.StreamEvent) (string, *nopSink) {
	t.Helper()
	var buf bytes.Buffer
	wf := &Workflow{ID: "wfX", Nodes: map[string]node.Node{"agent1": &streamingAgent{term: term}}, TopoOrder: []string{"agent1"}}
	ex := New(wf, log.New(&buf, "", 0), nil)
	ex.clock = fakeClock()
	sink := &nopSink{}
	if err := ex.Run(context.Background(), node.TriggerEvent{
		Body: []byte(`{"message":"hi"}`), SessionID: "s1", RequestID: "r1", StreamSink: sink,
	}); err != nil {
		t.Fatal(err)
	}
	return buf.String(), sink
}

func TestRunLogsRunMetricOnComplete(t *testing.T) {
	out, sink := runWithMetrics(t, node.StreamEvent{Type: node.StreamComplete, Data: []byte(`{"finalText":"hi","messageStop":"end_turn"}`)})
	if sink.events != 2 || sink.term.Type != node.StreamComplete {
		t.Fatalf("events not forwarded unchanged: events=%d term=%s", sink.events, sink.term.Type)
	}
	for _, want := range []string{"RUN_METRIC event=run.done", "workflow=wfX", "request=r1", "session=s1",
		"end=complete", "stop=end_turn", "tools=1", "events=3", "first_event_ms=", "first_text_ms=", " ms="} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q missing %q", out, want)
		}
	}
	if strings.Contains(out, "first_text_ms=0 ") || strings.Contains(out, " ms=0") {
		t.Fatalf("timings not measured: %q", out)
	}
}

func TestRunLogsRunMetricOnError(t *testing.T) {
	out, _ := runWithMetrics(t, node.StreamEvent{Type: node.StreamError, Data: []byte(`{"code":"llm-error","message":"boom"}`)})
	if !strings.Contains(out, "end=error") || !strings.Contains(out, "stop=llm-error") {
		t.Fatalf("error run not logged: %q", out)
	}
}
