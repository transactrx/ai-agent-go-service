package webbridge

import (
	"encoding/json"
	"errors"
	"fmt"
)

type ClientEvent string

const (
	ClientEventUserMessage ClientEvent = "user_message"
	ClientEventToolResult  ClientEvent = "tool_result_from_client"
	ClientEventCancel      ClientEvent = "cancel"
)

// ClientFrame is the typed envelope the WS handler reads from the client.
// Only one of UserMessage / ToolResult is set per frame.
type ClientFrame struct {
	Event       ClientEvent
	UserMessage *UserMessageData
	ToolResult  *ToolResultData
}

// UserMessageData is the payload for ClientEventUserMessage. SessionId is
// optional but load-bearing for chat memory: when the client engine opens a
// fresh WS per send (Webix), this is the only place the existing session id
// flows from localStorage back to the API trigger — without it, the trigger
// mints a new uuid and the agent's memory loads zero history.
type UserMessageData struct {
	Text        string             `json:"text"`
	SessionId   string             `json:"sessionId,omitempty"`
	WorkflowId  string             `json:"workflowId,omitempty"`
	IndexName   string             `json:"indexName,omitempty"`
	// TimeZone is the browser's IANA zone, sent on the frame (not just the token
	// POST body) because the portal proxy can strip the token body. Forwarded to
	// the chat workflow as the X-Time-Zone header.
	TimeZone    string             `json:"timeZone,omitempty"`
	Attachments []ClientAttachment `json:"attachments,omitempty"`
}

// ClientAttachment carries a previously-uploaded file/image by URL plus the
// raw bytes inline. The bytes are needed by the agent on the API side (the
// API is NATS-only and can't HTTP-fetch the upload URL) to feed Bedrock as
// native image/document content blocks. Go's encoding/json transparently
// base64-decodes the wire `data` string into the []byte field.
type ClientAttachment struct {
	URL       string `json:"url"`
	MediaType string `json:"mediaType"`
	Filename  string `json:"filename"`
	Size      int64  `json:"size"`
	Data      []byte `json:"data,omitempty"`
}

// ToolResultData is the payload for ClientEventToolResult. Carries the result
// the client widget shipped back for a previously-emitted client-only tool call.
type ToolResultData struct {
	ToolCallID string          `json:"toolCallId"`
	Result     json.RawMessage `json:"result"`
}

type rawFrame struct {
	Event ClientEvent     `json:"event"`
	Data  json.RawMessage `json:"data"`
}

// ParseClientFrame turns the raw WS bytes into a typed ClientFrame. Returns
// an error for unknown event types or malformed data.
func ParseClientFrame(b []byte) (ClientFrame, error) {
	var rf rawFrame
	if err := json.Unmarshal(b, &rf); err != nil {
		return ClientFrame{}, fmt.Errorf("clientframe: parse: %w", err)
	}
	out := ClientFrame{Event: rf.Event}
	switch rf.Event {
	case ClientEventUserMessage:
		var d UserMessageData
		if len(rf.Data) > 0 {
			if err := json.Unmarshal(rf.Data, &d); err != nil {
				return ClientFrame{}, fmt.Errorf("clientframe: user_message data: %w", err)
			}
		}
		out.UserMessage = &d
	case ClientEventToolResult:
		var d ToolResultData
		if err := json.Unmarshal(rf.Data, &d); err != nil {
			return ClientFrame{}, fmt.Errorf("clientframe: tool_result data: %w", err)
		}
		if d.ToolCallID == "" {
			return ClientFrame{}, errors.New("clientframe: tool_result missing toolCallId")
		}
		out.ToolResult = &d
	case ClientEventCancel:
		// no data needed
	default:
		return ClientFrame{}, fmt.Errorf("clientframe: unknown event %q", rf.Event)
	}
	return out, nil
}
