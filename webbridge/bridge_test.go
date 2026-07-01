package webbridge

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/nats-io/nats.go"
)

// fakeAuth is the test double for the Authenticator seam. It replaces the
// host's session/IDT coupling: Identify returns a canned identity, and
// OutboundMessage builds the NATS message directly (attaching the live
// credential it holds per session via warmed) so runOneTurn tests can assert
// the wire headers without a real IDT registry.
type fakeAuth struct {
	id Identity
	// warmed maps sessionID -> credential token, mimicking the host's IDT
	// registry. OutboundMessage stamps X-TRX-IDT from it (omitted when empty).
	warmed map[string]string
}

func (f fakeAuth) Identify(*fiber.Ctx) (Identity, error) { return f.id, nil }
func (f fakeAuth) WarmCredential(*fiber.Ctx, string)     {}
func (f fakeAuth) OutboundMessage(sessionID, subject string, headers map[string]string, body []byte) *nats.Msg {
	m := nats.NewMsg(subject)
	for k, v := range headers {
		m.Header.Set(k, v)
	}
	if cred := f.warmed[sessionID]; cred != "" {
		m.Header.Set("X-TRX-IDT", cred)
	}
	m.Data = body
	return m
}
func (f fakeAuth) Release(string) {}

// newTestBridge builds a bridge under test with the fake seams.
func newTestBridge(warmed map[string]string) *bridge {
	return &bridge{
		auth:         fakeAuth{id: Identity{AccountID: "a", UserID: "u"}, warmed: warmed},
		authz:        AllowAll{},
		natsBasePath: "trx.test",
		logger:       log.New(io.Discard, "", 0), // silence ws-pump-trace/IDT_METRIC noise in test output
	}
}

type fakeSub struct {
	events  chan streamEvent
	cancels int
	closed  bool

	// for tool-result forwarding tests
	mu              sync.Mutex
	publishedToolID string
	publishedResult []byte
	toolPrefix      string
}

func (f *fakeSub) Events() <-chan streamEvent { return f.events }
func (f *fakeSub) Cancel() error              { f.cancels++; return nil }
func (f *fakeSub) Close() error               { f.closed = true; return nil }
func (f *fakeSub) ToolResultPrefix() string   { return f.toolPrefix }
func (f *fakeSub) PublishToolResult(toolCallID string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishedToolID = toolCallID
	f.publishedResult = payload
	return nil
}

func newFakeSubBuffered(events []streamEvent) *fakeSub {
	ch := make(chan streamEvent, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	return &fakeSub{events: ch}
}

type fakeWS struct {
	mu     sync.Mutex
	frames []wsFrame
}

func (w *fakeWS) WriteJSON(v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var f wsFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	w.frames = append(w.frames, f)
	return nil
}

func (w *fakeWS) Frames() []wsFrame {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]wsFrame, len(w.frames))
	copy(out, w.frames)
	return out
}

type errorWS struct{}

var errFakeWriter = errors.New("fake writer error")

func (e *errorWS) WriteJSON(v any) error { return errFakeWriter }

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// TestPumpEventsForwardsAllEvents: pump writes one frame per event and stops
// on the terminator without calling Cancel.
func TestPumpEventsForwardsAllEvents(t *testing.T) {
	events := []streamEvent{
		{Type: streamStart, Sequence: 0, Data: mustJSON(map[string]string{"sessionId": "S1", "workflowId": "powerlineSearch"})},
		{Type: streamDelta, Sequence: 1, Data: mustJSON(map[string]string{"text": "hello"})},
		{Type: streamComplete, Sequence: 2, Data: mustJSON(map[string]string{"finalText": "hello", "messageStop": "end_turn"})},
	}
	sub := newFakeSubBuffered(events)
	w := &fakeWS{}
	closed := make(chan struct{})
	clientFrames := make(chan ClientFrame)
	b := newTestBridge(nil)

	b.pumpEvents(w, sub, "req-1", closed, clientFrames)

	frames := w.Frames()
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	if frames[0].Event != "start" || frames[1].Event != "delta" || frames[2].Event != "complete" {
		t.Errorf("unexpected event order: %s, %s, %s", frames[0].Event, frames[1].Event, frames[2].Event)
	}
	var startBody map[string]string
	if err := json.Unmarshal(frames[0].Data, &startBody); err != nil {
		t.Fatalf("start data not json: %v", err)
	}
	if startBody["requestId"] != "req-1" {
		t.Errorf("start.requestId = %q, want req-1", startBody["requestId"])
	}
	if sub.cancels != 0 {
		t.Errorf("cancel should not be called on normal terminator, got %d", sub.cancels)
	}
}

// TestPumpEventsCancelsOnClientClose: closing the closed channel mid-stream
// triggers Cancel and exits the pump.
func TestPumpEventsCancelsOnClientClose(t *testing.T) {
	sub := &fakeSub{events: make(chan streamEvent)}
	w := &fakeWS{}
	closed := make(chan struct{})
	clientFrames := make(chan ClientFrame)
	b := newTestBridge(nil)

	done := make(chan struct{})
	go func() {
		b.pumpEvents(w, sub, "req-2", closed, clientFrames)
		close(done)
	}()

	close(closed)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pumpEvents did not exit after closed signal")
	}
	if sub.cancels != 1 {
		t.Errorf("expected 1 cancel, got %d", sub.cancels)
	}
}

// TestPumpEventsKeepaliveDuringIdle: while no agent events flow, pumpEvents
// emits periodic "ping" keepalive frames so the proxy doesn't idle-close the WS
// during a slow post-tool generation.
func TestPumpEventsKeepaliveDuringIdle(t *testing.T) {
	old := wsKeepaliveInterval
	wsKeepaliveInterval = 10 * time.Millisecond
	defer func() { wsKeepaliveInterval = old }()

	sub := &fakeSub{events: make(chan streamEvent)} // never emits an event
	w := &fakeWS{}
	closed := make(chan struct{})
	clientFrames := make(chan ClientFrame)
	b := newTestBridge(nil)

	done := make(chan struct{})
	go func() {
		b.pumpEvents(w, sub, "req-ka", closed, clientFrames)
		close(done)
	}()

	time.Sleep(60 * time.Millisecond) // ~6 keepalive ticks
	close(closed)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pumpEvents did not exit after closed signal")
	}

	pings := 0
	for _, f := range w.Frames() {
		if f.Event == "ping" {
			pings++
		}
	}
	if pings == 0 {
		t.Fatal("expected at least one keepalive ping frame during idle, got 0")
	}
	if sub.cancels != 1 {
		t.Errorf("expected 1 cancel on client close, got %d", sub.cancels)
	}
}

// TestPumpEventsStopsOnWriterError: writer error triggers Cancel and exits.
func TestPumpEventsStopsOnWriterError(t *testing.T) {
	events := []streamEvent{
		{Type: streamStart, Sequence: 0, Data: mustJSON(map[string]string{"sessionId": "S1"})},
	}
	sub := newFakeSubBuffered(events)
	closed := make(chan struct{})
	clientFrames := make(chan ClientFrame)
	b := newTestBridge(nil)

	b.pumpEvents(&errorWS{}, sub, "req-3", closed, clientFrames)

	if sub.cancels != 1 {
		t.Errorf("expected 1 cancel after writer error, got %d", sub.cancels)
	}
}

// TestBuildWSFrameStartRewritesRequestId: the start payload is replaced with
// startEventOut carrying the bridge-generated requestId; other events pass
// through unchanged.
func TestBuildWSFrameStartRewritesRequestId(t *testing.T) {
	startEvt := streamEvent{
		Type: streamStart, Sequence: 0,
		Data: mustJSON(startPayload{SessionID: "S1", WorkflowID: "powerlineSearch", RequestID: "agent-req-id"}),
	}
	frame := buildWSFrame(startEvt, "bridge-req-id")
	var out startEventOut
	if err := json.Unmarshal(frame.Data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.RequestId != "bridge-req-id" {
		t.Errorf("requestId = %q, want bridge-req-id", out.RequestId)
	}
	if out.SessionId != "S1" {
		t.Errorf("sessionId = %q, want S1", out.SessionId)
	}

	deltaEvt := streamEvent{
		Type: streamDelta, Sequence: 1,
		Data: mustJSON(map[string]string{"text": "hi"}),
	}
	deltaFrame := buildWSFrame(deltaEvt, "bridge-req-id")
	if string(deltaFrame.Data) != string(deltaEvt.Data) {
		t.Errorf("non-start frame data should pass through untouched")
	}
}

func TestResolveWorkflowID(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},                       // empty → rejected by caller
		{"sampleWorkflow", "sampleWorkflow"},
		{"valid-id_1", "valid-id_1"},
		{"bad.subject", ""},            // subject-illegal chars → "" → rejected
		{"wild*card", ""},
		{"a>b", ""},
		{" spaces ", ""},
	}
	for _, tc := range cases {
		if got := resolveWorkflowID(tc.in); got != tc.want {
			t.Errorf("resolveWorkflowID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestPumpEventsForwardsToolResultViaNATS: a tool_result_from_client frame
// arriving mid-stream is forwarded via sub.PublishToolResult.
func TestPumpEventsForwardsToolResultViaNATS(t *testing.T) {
	sub := &fakeSub{
		events:     make(chan streamEvent),
		toolPrefix: "stream.results",
	}
	w := &fakeWS{}
	closed := make(chan struct{})
	clientFrames := make(chan ClientFrame)
	b := newTestBridge(nil)

	resultPayload := mustJSON(map[string]string{"answer": "yes"})

	done := make(chan struct{})
	go func() {
		b.pumpEvents(w, sub, "req-tr", closed, clientFrames)
		close(done)
	}()

	// Send the tool result frame; pumpEvents will call PublishToolResult.
	clientFrames <- ClientFrame{
		Event: ClientEventToolResult,
		ToolResult: &ToolResultData{
			ToolCallID: "call-abc",
			Result:     resultPayload,
		},
	}

	// Now send a cancel frame to make pumpEvents exit.
	clientFrames <- ClientFrame{Event: ClientEventCancel}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pumpEvents did not exit")
	}

	sub.mu.Lock()
	gotID := sub.publishedToolID
	gotResult := sub.publishedResult
	sub.mu.Unlock()

	if gotID != "call-abc" {
		t.Errorf("PublishToolResult toolCallID = %q, want call-abc", gotID)
	}
	if string(gotResult) != string(resultPayload) {
		t.Errorf("PublishToolResult payload = %s, want %s", gotResult, resultPayload)
	}
}

// resetTokenStore clears the global token store between tests.
func resetTokenStore(t *testing.T) {
	t.Helper()
	tokenStore.Lock()
	tokenStore.m = map[string]tokenEntry{}
	tokenStore.Unlock()
}

func TestConsumeTokenReturnsSessionID(t *testing.T) {
	resetTokenStore(t)
	tokenStore.Lock()
	tokenStore.m["t1"] = tokenEntry{
		accountId: "acct",
		userId:    "user",
		userName:  "Ada Lovelace",
		timeZone:  "America/New_York",
		sessionID: "gf-sess-1",
		expires:   time.Now().Add(time.Minute),
	}
	tokenStore.Unlock()

	accountId, userId, userName, timeZone, sessionID, ok := consumeToken("t1")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if accountId != "acct" || userId != "user" || sessionID != "gf-sess-1" {
		t.Errorf("consumeToken returned (%q, %q, %q); want (acct, user, gf-sess-1)",
			accountId, userId, sessionID)
	}
	if userName != "Ada Lovelace" || timeZone != "America/New_York" {
		t.Errorf("consumeToken name/tz = (%q, %q); want (Ada Lovelace, America/New_York)", userName, timeZone)
	}
}

func TestRunOneTurn_OutboundMsgHasIDT(t *testing.T) {
	var captured *nats.Msg
	orig := doStreamingRequest
	doStreamingRequest = func(msg *nats.Msg) (streamSubscription, error) {
		captured = msg
		return newFakeSubBuffered([]streamEvent{{Type: streamComplete, Sequence: 0}}), nil
	}
	t.Cleanup(func() { doStreamingRequest = orig })

	// Warm the fake's registry so OutboundMessage stamps the live credential
	// for this session — the seam equivalent of CaptureIDT.
	b := newTestBridge(map[string]string{"gf-sess-x": "IDT-x"})

	clientFrames := make(chan ClientFrame)
	closed := make(chan struct{})
	close(closed)
	ws := &fakeWS{}
	_, _ = b.runOneTurn(ws, "acct", "user", "Ada Lovelace", "America/New_York", "gf-sess-x", "hello", "sid-1", "powerlineSearch", nil, clientFrames, closed)

	if captured == nil {
		t.Fatal("expected streamer to be called")
	}
	if got := captured.Header.Get("X-TRX-IDT"); got != "IDT-x" {
		t.Errorf("X-TRX-IDT: got %q", got)
	}
	if got := captured.Header.Get("X-Account-Id"); got != "acct" {
		t.Errorf("X-Account-Id: got %q want acct", got)
	}
	if got := captured.Header.Get("X-User-Id"); got != "user" {
		t.Errorf("X-User-Id: got %q want user", got)
	}
	if got := captured.Header.Get("X-User-Name"); got != "Ada Lovelace" {
		t.Errorf("X-User-Name: got %q want Ada Lovelace", got)
	}
	if got := captured.Header.Get("X-Time-Zone"); got != "America/New_York" {
		t.Errorf("X-Time-Zone: got %q want America/New_York", got)
	}
}

// TestPumpEvents_TerminalErrorReturnsFatal: a streamError whose body carries a
// 401/403 status (synthesised from a nats-service auth response on the wire)
// must cause pumpEvents to return fatal=true and emit a session-expired
// wsFrame so the UI has a stable code to render.
func TestPumpEvents_TerminalErrorReturnsFatal(t *testing.T) {
	natsErr := mustJSON(map[string]any{
		"status":        403,
		"errorMessage":  "IDT validation failed: TOKEN_REVOKED",
		"apiStatusCode": 1001,
	})
	events := []streamEvent{
		{Type: streamError, Sequence: 0, Data: natsErr},
	}
	sub := newFakeSubBuffered(events)
	w := &fakeWS{}
	closed := make(chan struct{})
	clientFrames := make(chan ClientFrame)
	b := newTestBridge(nil)

	_, fatal := b.pumpEvents(w, sub, "req-fatal", closed, clientFrames)
	if !fatal {
		t.Fatal("expected fatal=true for 403 stream error")
	}
	frames := w.Frames()
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames (raw error + session-expired), got %d", len(frames))
	}
	if frames[0].Event != "error" {
		t.Errorf("first frame event = %q, want error", frames[0].Event)
	}
	// Second frame is the synthesised session-expired error.
	if frames[1].Event != "error" {
		t.Errorf("second frame event = %q, want error", frames[1].Event)
	}
	var body map[string]string
	if err := json.Unmarshal(frames[1].Data, &body); err != nil {
		t.Fatalf("session-expired frame body not json: %v", err)
	}
	if body["code"] != "session-expired" {
		t.Errorf("session-expired frame code = %q, want session-expired", body["code"])
	}
}

// TestPumpEvents_StreamError5xxNotFatal: a streamError with a 5xx status is
// treated as transient (could be a backend hiccup) — fatal=false, no
// session-expired follow-up frame, multi-turn loop continues.
func TestPumpEvents_StreamError5xxNotFatal(t *testing.T) {
	natsErr := mustJSON(map[string]any{
		"status":       500,
		"errorMessage": "internal error",
	})
	events := []streamEvent{
		{Type: streamError, Sequence: 0, Data: natsErr},
	}
	sub := newFakeSubBuffered(events)
	w := &fakeWS{}
	closed := make(chan struct{})
	clientFrames := make(chan ClientFrame)
	b := newTestBridge(nil)

	_, fatal := b.pumpEvents(w, sub, "req-5xx", closed, clientFrames)
	if fatal {
		t.Fatal("expected fatal=false for 500 stream error")
	}
	if got := len(w.Frames()); got != 1 {
		t.Fatalf("expected 1 frame (raw error only), got %d", got)
	}
}

// TestParseStreamMessage_NonOKStatusBecomesError: a nats-service error response
// has the "status" header set and no _Stream_Event header — parseStreamMessage
// must synthesise a streamError event carrying the body verbatim.
func TestParseStreamMessage_NonOKStatusBecomesError(t *testing.T) {
	m := &nats.Msg{
		Header: nats.Header{
			"status": []string{"403"},
		},
		Data: []byte(`{"status":403,"errorMessage":"denied"}`),
	}
	evt, err := parseStreamMessage(m)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if evt.Type != streamError {
		t.Errorf("event type = %q, want %q", evt.Type, streamError)
	}
	if string(evt.Data) != `{"status":403,"errorMessage":"denied"}` {
		t.Errorf("body not passed through verbatim: %s", evt.Data)
	}
}

func TestRunOneTurn_IDTOff_NoIDTHeaders(t *testing.T) {
	var captured *nats.Msg
	orig := doStreamingRequest
	doStreamingRequest = func(msg *nats.Msg) (streamSubscription, error) {
		captured = msg
		return newFakeSubBuffered([]streamEvent{{Type: streamComplete, Sequence: 0}}), nil
	}
	t.Cleanup(func() { doStreamingRequest = orig })

	// No warmed credential for this session, so OutboundMessage suppresses the
	// X-TRX-IDT header — preserves the documented "credential off → none on the
	// wire" contract.
	b := newTestBridge(nil)
	clientFrames := make(chan ClientFrame)
	closed := make(chan struct{})
	close(closed)
	ws := &fakeWS{}
	_, _ = b.runOneTurn(ws, "acct", "user", "", "", "gf-sess-off", "hi", "sid-1", "powerlineSearch", nil, clientFrames, closed)

	if captured == nil {
		t.Fatal("expected streamer to be called")
	}
	if v := captured.Header.Get("X-TRX-IDT"); v != "" {
		t.Errorf("X-TRX-IDT should be absent when credential is empty, got %q", v)
	}
}
