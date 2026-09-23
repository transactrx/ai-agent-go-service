// pkg/rsassistant/relay_test.go
package rsassistant

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

type recorded struct {
	kind string
	a, b string
	c    string
	code int
	any  any
}

type recorder struct{ evs []recorded }

func (r *recorder) Text(d string) { r.evs = append(r.evs, recorded{kind: "text", a: d}) }
func (r *recorder) ToolUse(id, name string, in any) {
	r.evs = append(r.evs, recorded{kind: "toolUse", a: id, b: name, any: in})
}
func (r *recorder) ToolResult(id, name, res, e string) {
	r.evs = append(r.evs, recorded{kind: "toolResult", a: id, b: name, c: res + "|" + e})
}
func (r *recorder) Data(kind string, p any) {
	r.evs = append(r.evs, recorded{kind: "data", a: kind, any: p})
}
func (r *recorder) Status(t string) { r.evs = append(r.evs, recorded{kind: "status", a: t}) }
func (r *recorder) Done(stop string, _ *wire.Usage) {
	r.evs = append(r.evs, recorded{kind: "done", a: stop})
}
func (r *recorder) Error(msg string, code int) {
	r.evs = append(r.evs, recorded{kind: "error", a: msg, code: code})
}

func ev(t natsstream.StreamEventType, seq int, data string) natsstream.StreamEvent {
	return natsstream.StreamEvent{Type: t, Sequence: seq, Data: json.RawMessage(data)}
}

func TestRelayMapsFullStream(t *testing.T) {
	out := &recorder{}
	r := newRelay()
	r.apply(ev(natsstream.StreamStart, 0, `{"sessionId":"s1","workflowId":"wf","requestId":"r1"}`), out)
	r.apply(ev(natsstream.StreamDelta, 1, `{"text":"Today "}`), out)
	r.apply(ev(natsstream.StreamToolCall, 2, `{"toolUseId":"t1","name":"OpenSearchQuery","input":{"indexPath":"x"}}`), out)
	r.apply(ev(natsstream.StreamToolResult, 3, `{"toolUseId":"t1","output":{"hits":3},"isError":false}`), out)
	r.apply(ev(natsstream.StreamAttachment, 4, `{"toolUseId":"t2","kind":"image","payload":{"imageUrl":"https://s3/chart.png"}}`), out)
	r.apply(ev(natsstream.StreamDelta, 5, `{"text":"1,204 claims."}`), out)
	r.apply(ev(natsstream.StreamComplete, 6, `{"finalText":"Today 1,204 claims.","messageStop":"end_turn"}`), out)

	kinds := []string{}
	for _, e := range out.evs {
		kinds = append(kinds, e.kind)
	}
	want := "text,toolUse,toolResult,data,text,text,done"
	if got := strings.Join(kinds, ","); got != want {
		t.Fatalf("event kinds = %s, want %s", got, want)
	}
	if out.evs[1].b != "OpenSearchQuery" || out.evs[2].b != "OpenSearchQuery" {
		t.Fatalf("tool name must be carried from tool_call to tool_result: %+v", out.evs[1:3])
	}
	if !strings.Contains(out.evs[2].c, `{"hits":3}`) {
		t.Fatalf("tool result text = %q", out.evs[2].c)
	}
	if !strings.Contains(out.evs[4].a, "https://s3/chart.png") {
		t.Fatalf("attachment url must be emitted as text, got %q", out.evs[4].a)
	}
	if out.evs[6].a != wire.StopEndTurn {
		t.Fatalf("stop = %q", out.evs[6].a)
	}
	if !r.terminated() || r.count() != 7 {
		t.Fatalf("terminated=%v count=%d", r.terminated(), r.count())
	}
}

func TestRelayCompleteWithoutDeltasEmitsFinalText(t *testing.T) {
	out := &recorder{}
	r := newRelay()
	r.apply(ev(natsstream.StreamStart, 0, `{}`), out)
	r.apply(ev(natsstream.StreamComplete, 1, `{"finalText":"only final","messageStop":"max_tokens"}`), out)
	if len(out.evs) != 2 || out.evs[0].kind != "text" || out.evs[0].a != "only final" || out.evs[1].a != wire.StopMaxTokens {
		t.Fatalf("events = %+v", out.evs)
	}
}

func TestRelayToolResultErrorAndErrorEvent(t *testing.T) {
	out := &recorder{}
	r := newRelay()
	r.apply(ev(natsstream.StreamToolResult, 1, `{"toolUseId":"t9","output":"boom","isError":true}`), out)
	if out.evs[0].c != "boom|boom" {
		t.Fatalf("isError must fill toolErr, got %q", out.evs[0].c)
	}
	r.apply(ev(natsstream.StreamError, 2, `{"code":"executor_failed","message":"upstream down"}`), out)
	last := out.evs[len(out.evs)-1]
	if last.kind != "error" || last.code != wire.CodeUpstream || !strings.Contains(last.a, "executor_failed") || !strings.Contains(last.a, "upstream down") {
		t.Fatalf("error mapping = %+v", last)
	}
	if !r.terminated() {
		t.Fatal("error must terminate the relay")
	}
}

func TestRelayFinishWithoutTerminatorEmitsError(t *testing.T) {
	out := &recorder{}
	r := newRelay()
	r.apply(ev(natsstream.StreamDelta, 1, `{"text":"partial"}`), out)
	r.finish(errors.New("natsstream: timed out after 1s"), out)
	last := out.evs[len(out.evs)-1]
	if last.kind != "error" || last.code != wire.CodeUpstream || !strings.Contains(last.a, "timed out") {
		t.Fatalf("finish = %+v", last)
	}
	// finish after a proper terminator must be a no-op
	out2 := &recorder{}
	r2 := newRelay()
	r2.apply(ev(natsstream.StreamComplete, 0, `{"finalText":"x","messageStop":"end_turn"}`), out2)
	r2.finish(nil, out2)
	if len(out2.evs) != 2 { // text(x) + done
		t.Fatalf("finish after complete must not emit, got %+v", out2.evs)
	}
}

func TestGate(t *testing.T) {
	verified := agent.Identity{UserID: "u1", AccountID: "a1", Verified: true}
	observed := agent.Identity{UserID: "u1", AccountID: "a1", Verified: false}
	empty := agent.Identity{IDT: "tok"}

	if err := gate(verified, true); err != nil {
		t.Fatalf("verified must pass strict: %v", err)
	}
	if err := gate(observed, true); err == nil {
		t.Fatal("unverified must fail strict")
	}
	if err := gate(observed, false); err != nil {
		t.Fatalf("unverified with account+user passes observe: %v", err)
	}
	if err := gate(empty, false); err == nil {
		t.Fatal("empty account/user must fail even in observe")
	}
	if err := gate(agent.Identity{UserID: "u", Verified: true}, true); err == nil {
		t.Fatal("missing account must fail")
	}
}

func TestMessageTextJoinsTextBlocks(t *testing.T) {
	m := wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "a"}, {Text: ""}, {Text: "b"}}}
	if got := messageText(m); got != "a\nb" {
		t.Fatalf("messageText = %q", got)
	}
}

func TestMapStop(t *testing.T) {
	if mapStop("end_turn") != wire.StopEndTurn || mapStop("") != wire.StopEndTurn || mapStop("max_tokens") != wire.StopMaxTokens || mapStop("tool_use") != wire.StopEndTurn {
		t.Fatal("mapStop table wrong")
	}
}
