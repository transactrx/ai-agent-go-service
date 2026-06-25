package node

import (
	"context"
	"encoding/json"
	"testing"
)

func TestAttachmentEmitterRoundTrip(t *testing.T) {
	got := []ToolAttachment{}
	emitter := AttachmentEmitterFunc(func(_ context.Context, a ToolAttachment) error {
		got = append(got, a)
		return nil
	})
	ctx := WithAttachmentEmitter(context.Background(), emitter)

	e, ok := EmitterFromContext(ctx)
	if !ok || e == nil {
		t.Fatal("EmitterFromContext returned no emitter")
	}
	if err := e.Attach(ctx, ToolAttachment{Kind: "quickchart", Payload: json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if len(got) != 1 || got[0].Kind != "quickchart" || string(got[0].Payload) != `{"x":1}` {
		t.Fatalf("emitter saw %+v", got)
	}
}

func TestEmitterFromContextEmptyReturnsFalse(t *testing.T) {
	_, ok := EmitterFromContext(context.Background())
	if ok {
		t.Fatal("expected no emitter on a bare context")
	}
}
