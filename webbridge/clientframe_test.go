package webbridge

import "testing"

func TestParseClientFrameUserMessage(t *testing.T) {
	raw := []byte(`{"event":"user_message","data":{"text":"hi"}}`)
	f, err := ParseClientFrame(raw)
	if err != nil {
		t.Fatalf("ParseClientFrame: %v", err)
	}
	if f.Event != ClientEventUserMessage {
		t.Fatalf("event: %q", f.Event)
	}
	if f.UserMessage == nil || f.UserMessage.Text != "hi" {
		t.Fatalf("data: %+v", f.UserMessage)
	}
}

// TestParseClientFrameUserMessageCarriesSessionId verifies sessionId in the
// typed user_message envelope is preserved. Without this, every WS reconnect
// (the Webix engine opens a new WS per send) loses chat memory because
// initialSessionID stays empty and the API trigger mints a fresh UUID.
func TestParseClientFrameUserMessageCarriesSessionId(t *testing.T) {
	raw := []byte(`{"event":"user_message","data":{"text":"hi","sessionId":"S-abc"}}`)
	f, err := ParseClientFrame(raw)
	if err != nil {
		t.Fatalf("ParseClientFrame: %v", err)
	}
	if f.UserMessage == nil || f.UserMessage.SessionId != "S-abc" {
		t.Fatalf("sessionId: %+v", f.UserMessage)
	}
}

// TestParseClientFrameUserMessageCarriesTimeZone verifies the browser timezone
// on the frame is preserved — the reliable channel after the portal proxy strips
// the token POST body. Forwarded downstream as the X-Time-Zone header.
func TestParseClientFrameUserMessageCarriesTimeZone(t *testing.T) {
	raw := []byte(`{"event":"user_message","data":{"text":"hi","timeZone":"America/New_York"}}`)
	f, err := ParseClientFrame(raw)
	if err != nil {
		t.Fatalf("ParseClientFrame: %v", err)
	}
	if f.UserMessage == nil || f.UserMessage.TimeZone != "America/New_York" {
		t.Fatalf("timeZone: %+v", f.UserMessage)
	}
}

func TestParseClientFrameToolResult(t *testing.T) {
	raw := []byte(`{"event":"tool_result_from_client","data":{"toolCallId":"tc1","result":{"approved":true}}}`)
	f, err := ParseClientFrame(raw)
	if err != nil {
		t.Fatalf("ParseClientFrame: %v", err)
	}
	if f.Event != ClientEventToolResult {
		t.Fatalf("event: %q", f.Event)
	}
	if f.ToolResult == nil || f.ToolResult.ToolCallID != "tc1" || string(f.ToolResult.Result) == "" {
		t.Fatalf("tool result: %+v", f.ToolResult)
	}
}

func TestParseClientFrameCancel(t *testing.T) {
	f, err := ParseClientFrame([]byte(`{"event":"cancel","data":{}}`))
	if err != nil || f.Event != ClientEventCancel {
		t.Fatalf("got %+v %v", f, err)
	}
}

func TestParseClientFrameUnknownEventReturnsError(t *testing.T) {
	if _, err := ParseClientFrame([]byte(`{"event":"nope"}`)); err == nil {
		t.Fatal("expected error for unknown event")
	}
}
