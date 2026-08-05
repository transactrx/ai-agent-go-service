package natschat

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func completeEvt(text, stop string) node.StreamEvent {
	return node.StreamEvent{
		Type: node.StreamComplete,
		Data: natsstream.MustJSON(natsstream.CompletePayload{FinalText: text, MessageStop: stop}),
	}
}

func TestCollectSinkCompleteBuildsSingleResponse(t *testing.T) {
	ctx := context.Background()
	cs := newCollectSink("sess-1")

	// deltas and tool chatter are discarded; attachments are kept
	_ = cs.Send(ctx, node.StreamEvent{Type: node.StreamDelta, Data: json.RawMessage(`{"text":"par"}`)})
	_ = cs.Send(ctx, node.StreamEvent{Type: node.StreamToolCall, Data: json.RawMessage(`{"name":"x"}`)})
	_ = cs.Send(ctx, node.StreamEvent{Type: node.StreamAttachment, Data: json.RawMessage(`{"toolUseId":"t1","kind":"table","payload":{"rows":1}}`)})

	if err := cs.Close(ctx, completeEvt("the answer", "end_turn")); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := <-cs.done
	if r.errCode != "" {
		t.Fatalf("unexpected error result: %s %s", r.errCode, r.errMessage)
	}
	var resp singleResponseBody
	if err := json.Unmarshal(r.body, &resp); err != nil {
		t.Fatalf("unmarshal reply: %v", err)
	}
	if resp.SessionID != "sess-1" || resp.FinalText != "the answer" || resp.StopReason != "end_turn" {
		t.Errorf("resp = %+v", resp)
	}
	if len(resp.Attachments) != 1 {
		t.Fatalf("attachments = %d, want 1", len(resp.Attachments))
	}
}

func TestCollectSinkNoAttachmentsMarshalsEmptyArray(t *testing.T) {
	ctx := context.Background()
	cs := newCollectSink("s")
	_ = cs.Close(ctx, completeEvt("a", "end_turn"))
	r := <-cs.done
	if string(r.body) == "" || !json.Valid(r.body) {
		t.Fatalf("bad body: %q", r.body)
	}
	// attachments must be [] not null — machine callers parse this strictly
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(r.body, &raw)
	if string(raw["attachments"]) != "[]" {
		t.Errorf(`attachments = %s, want []`, raw["attachments"])
	}
}

func TestCollectSinkErrorTerminator(t *testing.T) {
	ctx := context.Background()
	cs := newCollectSink("s")
	err := cs.Close(ctx, node.StreamEvent{
		Type: node.StreamError,
		Data: natsstream.MustJSON(natsstream.ErrorPayload{Code: "llm-error", Message: "model unavailable"}),
	})
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	r := <-cs.done
	if r.errCode != "llm-error" || r.errMessage != "model unavailable" {
		t.Errorf("result = %+v", r)
	}
}

func TestCollectSinkCloseIdempotentAndSendAfterCloseIgnored(t *testing.T) {
	ctx := context.Background()
	cs := newCollectSink("s")
	_ = cs.Close(ctx, completeEvt("a", "end_turn"))
	if err := cs.Close(ctx, completeEvt("b", "end_turn")); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := cs.Send(ctx, node.StreamEvent{Type: node.StreamAttachment, Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Send after Close: %v", err)
	}
	<-cs.done // exactly one result
	select {
	case r := <-cs.done:
		t.Fatalf("second result delivered: %+v", r)
	default:
	}
}
