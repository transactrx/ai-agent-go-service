package natschat

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// newTestTrigger builds a trigger through the real Factory so config defaults
// apply, without running Init (no NATS in unit tests).
func newTestTrigger(t *testing.T, cfgJSON string) *natsChatTrigger {
	t.Helper()
	n, err := Factory.New(json.RawMessage(cfgJSON))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	return n.(*natsChatTrigger)
}

func TestResolveResponseMode(t *testing.T) {
	cases := []struct {
		name      string
		cfg       string
		requested string
		want      string
		wantErr   bool
	}{
		{"flag off, no field -> config default", `{"responseMode":"streaming"}`, "", "streaming", false},
		{"flag off, field sent -> silently ignored", `{"responseMode":"streaming"}`, "single", "streaming", false},
		{"flag off, invalid field -> still ignored", `{"responseMode":"streaming"}`, "bogus", "streaming", false},
		{"flag on, no field -> config default", `{"responseMode":"single","allowResponseModeOverride":true}`, "", "single", false},
		{"flag on, override to streaming", `{"responseMode":"single","allowResponseModeOverride":true}`, "streaming", "streaming", false},
		{"flag on, override to single", `{"responseMode":"streaming","allowResponseModeOverride":true}`, "single", "single", false},
		{"flag on, invalid -> error", `{"responseMode":"single","allowResponseModeOverride":true}`, "bogus", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTestTrigger(t, tc.cfg)
			got, err := tr.resolveResponseMode(tc.requested)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got mode %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("mode = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestChatRequestBodyParsesResponseMode(t *testing.T) {
	var b chatRequestBody
	if err := json.Unmarshal([]byte(`{"message":"q","responseMode":"single"}`), &b); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if b.ResponseMode != "single" {
		t.Errorf("ResponseMode = %q, want %q", b.ResponseMode, "single")
	}
}

// completeSink closes the request's StreamSink with a fixed answer —
// reaching it proves handle() dispatched to the SINGLE path.
func completeSink(text string) sinkFunc {
	return func(ctx context.Context, evt node.TriggerEvent) error {
		return evt.StreamSink.Close(ctx, node.StreamEvent{
			Type: node.StreamComplete,
			Data: natsstream.MustJSON(natsstream.CompletePayload{FinalText: text, MessageStop: "end_turn"}),
		})
	}
}

func handleReadyTrigger(t *testing.T, cfgJSON string, sink sinkFunc) *natsChatTrigger {
	t.Helper()
	tr := newTestTrigger(t, cfgJSON)
	tr.logger = log.New(io.Discard, "", 0)
	tr.requestTimeout = 2 * time.Second
	tr.sink = sink
	return tr
}

func identityHeaders() nats.Header {
	return nats.Header{"X-Account-Id": {"a1"}, "X-User-Id": {"u1"}}
}

func TestHandleDispatchesSingleByDefaultAndMintsSession(t *testing.T) {
	tr := handleReadyTrigger(t, `{"responseMode":"single"}`, completeSink("ok"))
	msg := testMsg(`{"message":"q"}`)
	msg.Header = identityHeaders()
	if res := tr.handle(msg); res != nil {
		t.Fatalf("handle: %+v", res)
	}
	var resp singleResponseBody
	if err := json.Unmarshal(msg.ResponseBody, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.FinalText != "ok" {
		t.Errorf("finalText = %q", resp.FinalText)
	}
	if resp.SessionID == "" {
		t.Error("expected server-minted sessionId in single reply")
	}
}

func TestHandleOverrideIgnoredWhenFlagOff(t *testing.T) {
	// Body asks for streaming but the workflow did not opt in: the single
	// config default must win. (This is the webapp-safety property inverted:
	// a config that doesn't opt in can never change modes per request.)
	tr := handleReadyTrigger(t, `{"responseMode":"single"}`, completeSink("ok"))
	msg := testMsg(`{"message":"q","responseMode":"streaming"}`)
	msg.Header = identityHeaders()
	if res := tr.handle(msg); res != nil {
		t.Fatalf("handle: %+v", res)
	}
	if len(msg.ResponseBody) == 0 {
		t.Fatal("expected single-mode reply body (streaming would leave it empty)")
	}
}

func TestHandleOverrideToSingleWhenFlagOn(t *testing.T) {
	tr := handleReadyTrigger(t, `{"responseMode":"streaming","allowResponseModeOverride":true}`, completeSink("ok"))
	msg := testMsg(`{"message":"q","responseMode":"single"}`)
	msg.Header = identityHeaders()
	if res := tr.handle(msg); res != nil {
		t.Fatalf("handle: %+v", res)
	}
	if len(msg.ResponseBody) == 0 {
		t.Fatal("expected single-mode reply body")
	}
}

func TestHandleInvalidOverrideRejectedWhenFlagOn(t *testing.T) {
	tr := handleReadyTrigger(t, `{"responseMode":"single","allowResponseModeOverride":true}`,
		sinkFunc(func(ctx context.Context, evt node.TriggerEvent) error {
			t.Fatal("sink must not be reached on invalid responseMode")
			return nil
		}))
	msg := testMsg(`{"message":"q","responseMode":"bogus"}`)
	msg.Header = identityHeaders()
	res := tr.handle(msg)
	if res == nil {
		t.Fatal("expected 400 validation error")
	}
	if res.Status != 400 {
		t.Errorf("Status = %d, want 400", res.Status)
	}
}
