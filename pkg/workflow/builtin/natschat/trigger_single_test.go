package natschat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/errcode"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// sinkFunc adapts a func to node.TriggerSink for tests.
type sinkFunc func(ctx context.Context, evt node.TriggerEvent) error

func (f sinkFunc) Emit(ctx context.Context, evt node.TriggerEvent) error { return f(ctx, evt) }

func newSingleTestTrigger(t *testing.T) *natsChatTrigger {
	t.Helper()
	tr := newTestTrigger(t, `{"responseMode":"single"}`)
	tr.logger = log.New(io.Discard, "", 0)
	tr.requestTimeout = 2 * time.Second
	return tr
}

func testMsg(body string) *nats_service.NatsMessage {
	return &nats_service.NatsMessage{
		MessageId:      "m1",
		Header:         nats.Header{},
		ResponseHeader: nats.Header{},
		Body:           []byte(body),
	}
}

func TestHandleSingleHappyPath(t *testing.T) {
	tr := newSingleTestTrigger(t)
	sink := sinkFunc(func(ctx context.Context, evt node.TriggerEvent) error {
		if evt.StreamSink == nil {
			t.Fatal("StreamSink must be non-nil in single mode (nil would panic the agent loop)")
		}
		_ = evt.StreamSink.Send(ctx, node.StreamEvent{Type: node.StreamDelta, Data: json.RawMessage(`{"text":"hi"}`)})
		return evt.StreamSink.Close(ctx, node.StreamEvent{
			Type: node.StreamComplete,
			Data: natsstream.MustJSON(natsstream.CompletePayload{FinalText: "hi there", MessageStop: "end_turn"}),
		})
	})
	msg := testMsg(`{"message":"q"}`)
	res := tr.handleSingle(msg, chatRequestBody{Message: "q", SessionID: "s1"}, identity.Identity{}, sink)
	if res != nil {
		t.Fatalf("handleSingle error: %+v", res)
	}
	var resp singleResponseBody
	if err := json.Unmarshal(msg.ResponseBody, &resp); err != nil {
		t.Fatalf("unmarshal ResponseBody: %v", err)
	}
	if resp.SessionID != "s1" || resp.FinalText != "hi there" || resp.StopReason != "end_turn" {
		t.Errorf("resp = %+v", resp)
	}
}

func TestHandleSingleAgentError(t *testing.T) {
	tr := newSingleTestTrigger(t)
	sink := sinkFunc(func(ctx context.Context, evt node.TriggerEvent) error {
		return evt.StreamSink.Close(ctx, node.StreamEvent{
			Type: node.StreamError,
			Data: natsstream.MustJSON(natsstream.ErrorPayload{Code: "llm-error", Message: "model unavailable"}),
		})
	})
	res := tr.handleSingle(testMsg(`{"message":"q"}`), chatRequestBody{Message: "q", SessionID: "s1"}, identity.Identity{}, sink)
	if res == nil {
		t.Fatal("expected error result")
	}
	if res.Status != 500 {
		t.Errorf("Status = %d, want 500", res.Status)
	}
}

func TestHandleSingleMidStreamTimeout(t *testing.T) {
	tr := newSingleTestTrigger(t)
	sink := sinkFunc(func(ctx context.Context, evt node.TriggerEvent) error {
		return evt.StreamSink.Close(ctx, node.StreamEvent{
			Type: node.StreamError,
			Data: natsstream.MustJSON(natsstream.ErrorPayload{Code: "timeout", Message: "deadline"}),
		})
	})
	res := tr.handleSingle(testMsg(`{"message":"q"}`), chatRequestBody{Message: "q", SessionID: "s1"}, identity.Identity{}, sink)
	if res == nil {
		t.Fatal("expected error result")
	}
	// NatsServiceError.Status is hardcoded to 500 by nats_service.NewServerError
	// regardless of the apiStatusCode argument (see vendored error-types.go);
	// ApiStatusCode is the field the caller-defined status actually lands in.
	if res.ApiStatusCode != 504 {
		t.Errorf("ApiStatusCode = %d, want 504", res.ApiStatusCode)
	}
	if res.InternalErr != "timeout" {
		t.Errorf("InternalErr = %q, want %q", res.InternalErr, "timeout")
	}
}

func TestHandleSingleEmitErrorWithoutClose(t *testing.T) {
	tr := newSingleTestTrigger(t)
	sink := sinkFunc(func(ctx context.Context, evt node.TriggerEvent) error {
		return context.DeadlineExceeded // executor failed before the agent ever ran
	})
	res := tr.handleSingle(testMsg(`{"message":"q"}`), chatRequestBody{Message: "q", SessionID: "s1"}, identity.Identity{}, sink)
	if res == nil {
		t.Fatal("expected error result")
	}
}

// TestHandleSingleTerminatorBeatsEmitErr pins the documented "prefer
// terminator over Emit's wrapped error" ordering in handleSingle: when a sink
// delivers a valid complete terminator through the collectSink AND Emit
// itself returns a non-nil error, the terminator's success reply wins.
func TestHandleSingleTerminatorBeatsEmitErr(t *testing.T) {
	tr := newSingleTestTrigger(t)
	sink := sinkFunc(func(ctx context.Context, evt node.TriggerEvent) error {
		_ = evt.StreamSink.Close(ctx, node.StreamEvent{
			Type: node.StreamComplete,
			Data: natsstream.MustJSON(natsstream.CompletePayload{FinalText: "hi there", MessageStop: "end_turn"}),
		})
		return errors.New("emit wrapped error after terminator")
	})
	msg := testMsg(`{"message":"q"}`)
	res := tr.handleSingle(msg, chatRequestBody{Message: "q", SessionID: "s1"}, identity.Identity{}, sink)
	if res != nil {
		t.Fatalf("handleSingle = %+v, want nil (terminator must win over Emit's wrapped error)", res)
	}
	var resp singleResponseBody
	if err := json.Unmarshal(msg.ResponseBody, &resp); err != nil {
		t.Fatalf("unmarshal ResponseBody: %v", err)
	}
	if resp.FinalText != "hi there" || resp.StopReason != "end_turn" {
		t.Errorf("resp = %+v", resp)
	}
}

// TestHandleSingleCancelledWithLiveCtxIsNot504 guards the Task 3 conditional
// in toReply: Cancelled only maps to 504 when the request ctx has already
// expired. With a live ctx, a client-driven cancel must fall through to the
// generic 500 branch carrying code "cancelled" — not be mistaken for a
// server-side timeout.
func TestHandleSingleCancelledWithLiveCtxIsNot504(t *testing.T) {
	tr := newSingleTestTrigger(t)
	sink := sinkFunc(func(ctx context.Context, evt node.TriggerEvent) error {
		return evt.StreamSink.Close(ctx, node.StreamEvent{
			Type: node.StreamError,
			Data: natsstream.MustJSON(natsstream.ErrorPayload{Code: errcode.Cancelled, Message: "client cancelled"}),
		})
	})
	res := tr.handleSingle(testMsg(`{"message":"q"}`), chatRequestBody{Message: "q", SessionID: "s1"}, identity.Identity{}, sink)
	if res == nil {
		t.Fatal("expected error result")
	}
	if res.ApiStatusCode != 500 {
		t.Errorf("ApiStatusCode = %d, want 500 (cancelled with a live ctx must not map to 504)", res.ApiStatusCode)
	}
	if res.InternalErr != errcode.Cancelled {
		t.Errorf("InternalErr = %q, want %q", res.InternalErr, errcode.Cancelled)
	}
}

func TestHandleSingleTimeoutWhenSinkNeverCloses(t *testing.T) {
	tr := newSingleTestTrigger(t)
	tr.requestTimeout = 50 * time.Millisecond
	sink := sinkFunc(func(ctx context.Context, evt node.TriggerEvent) error {
		return nil // pathological: Emit returns nil but nobody closes the sink
	})
	start := time.Now()
	res := tr.handleSingle(testMsg(`{"message":"q"}`), chatRequestBody{Message: "q", SessionID: "s1"}, identity.Identity{}, sink)
	if res == nil {
		t.Fatal("expected timeout error")
	}
	if time.Since(start) > time.Second {
		t.Errorf("timeout took too long: %s", time.Since(start))
	}
}
