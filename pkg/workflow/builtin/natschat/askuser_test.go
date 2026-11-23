package natschat

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/ask"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Only the exact lowercase key with the literal true sets the flag; the
// typed body never decodes it (non-bool values can't fail the request).
func TestAskUserFlag(t *testing.T) {
	cases := map[string]bool{
		`{"message":"q","askUser":true}`:   true,
		`{"message":"q","askUser":false}`:  false,
		`{"message":"q","askUser":"true"}`: false,
		`{"message":"q","AskUser":true}`:   false,
		`{"message":"q","ASKUSER":true}`:   false,
		`{"message":"q"}`:                  false,
		`not json`:                         false,
	}
	for in, want := range cases {
		if got := askUserFlag([]byte(in)); got != want {
			t.Errorf("%s: got %t", in, got)
		}
	}
	var b chatRequestBody
	if err := json.Unmarshal([]byte(`{"message":"q","askUser":true,"AskUser":"yes"}`), &b); err != nil || b.AskUser {
		t.Fatalf("typed decode must ignore askUser: %+v %v", b, err)
	}
}

// Without askUser, or on an endpoint not enabled for it, the run ctx carries
// no extra tool: today's behavior.
func TestStreamRunContextWithoutAskUser(t *testing.T) {
	ctx := streamRunContext(context.Background(), chatRequestBody{Message: "q"}, true)
	if got := node.RunClientTools(ctx); got != nil {
		t.Fatalf("run tools = %v", got)
	}
	ctx = streamRunContext(context.Background(), chatRequestBody{Message: "q", AskUser: true}, false)
	if got := node.RunClientTools(ctx); got != nil {
		t.Fatalf("not enabled: run tools = %v", got)
	}
}

// askUser offers request_user_input: library name, description + guidance,
// input schema.
func TestStreamRunContextAskUserOffersTool(t *testing.T) {
	ctx := streamRunContext(context.Background(), chatRequestBody{Message: "q", AskUser: true}, true)
	got := node.RunClientTools(ctx)
	if len(got) != 1 || got[0].Name != ask.ToolName {
		t.Fatalf("run tools = %+v", got)
	}
	if !strings.HasPrefix(got[0].Description, ask.ToolDescription()) || !strings.Contains(got[0].Description, ask.Guidance) {
		t.Fatalf("description = %q", got[0].Description)
	}
	want, _ := json.Marshal(ask.ToolInputSchema())
	if string(got[0].InputSchema) != string(want) {
		t.Fatalf("schema = %s", got[0].InputSchema)
	}
}

// Single mode has no tool-result path: askUser is ignored there.
func TestHandleSingleIgnoresAskUser(t *testing.T) {
	var offered []node.ToolSpec
	sink := func(ctx context.Context, evt node.TriggerEvent) error {
		offered = node.RunClientTools(ctx)
		return completeSink("ok")(ctx, evt)
	}
	tr := handleReadyTrigger(t, `{"responseMode":"single"}`, sink)
	msg := testMsg(`{"message":"q","askUser":true}`)
	msg.Header = identityHeaders()
	if res := tr.handle(msg); res != nil {
		t.Fatalf("handle: %+v", res)
	}
	if offered != nil {
		t.Fatalf("single mode offered %v", offered)
	}
}

// A non-enabled endpoint drops the askUser key from the body templates see;
// other bodies are left byte for byte.
func TestStripAskUser(t *testing.T) {
	if got := string(stripAskUser([]byte(`{"message":"q","askUser":true,"sessionId":"s"}`))); got != `{"message":"q","sessionId":"s"}` {
		t.Fatalf("stripped = %s", got)
	}
	// Every key Go's case-insensitive matching would map to the flag goes.
	if got := string(stripAskUser([]byte(`{"AskUser":1,"ASKUSER":"x","askuser":null,"message":"q"}`))); got != `{"message":"q"}` {
		t.Fatalf("case variants: %s", got)
	}
	for _, in := range []string{`{"message":"q", "sessionId":"s"}`, `not json`, `{"message":"askUser"}`} {
		if got := string(stripAskUser([]byte(in))); got != in {
			t.Fatalf("%s changed to %s", in, got)
		}
	}
}

// The single path of a non-enabled endpoint never sees askUser in the body.
func TestHandleStripsAskUserWhenNotEnabled(t *testing.T) {
	var body string
	sink := func(ctx context.Context, evt node.TriggerEvent) error {
		body = string(evt.Body)
		return completeSink("ok")(ctx, evt)
	}
	tr := handleReadyTrigger(t, `{"responseMode":"single"}`, sink)
	msg := testMsg(`{"message":"q","askUser":true}`)
	msg.Header = identityHeaders()
	if res := tr.handle(msg); res != nil {
		t.Fatalf("handle: %+v", res)
	}
	if strings.Contains(body, "askUser") {
		t.Fatalf("body = %s", body)
	}
	tr.EnableAskUser()
	msg = testMsg(`{"message":"q","askUser":true}`)
	msg.Header = identityHeaders()
	_ = tr.handle(msg)
	if !strings.Contains(body, "askUser") {
		t.Fatalf("enabled endpoint keeps the body as sent: %s", body)
	}
}

// A non-enabled endpoint accepts any askUser value as at 57c96b6 (no 400)
// and strips every case variant.
func TestHandleNonBoolAskUserNotEnabled(t *testing.T) {
	var body string
	sink := func(ctx context.Context, evt node.TriggerEvent) error {
		body = string(evt.Body)
		return completeSink("ok")(ctx, evt)
	}
	tr := handleReadyTrigger(t, `{"responseMode":"single"}`, sink)
	for _, in := range []string{`{"message":"q","askUser":"yes"}`, `{"message":"q","AskUser":{"x":1}}`, `{"message":"q","ASKUSER":true}`} {
		msg := testMsg(in)
		msg.Header = identityHeaders()
		if res := tr.handle(msg); res != nil {
			t.Fatalf("%s: handle: %+v", in, res)
		}
		if strings.Contains(strings.ToLower(body), "askuser") {
			t.Fatalf("%s: body = %s", in, body)
		}
	}
}
