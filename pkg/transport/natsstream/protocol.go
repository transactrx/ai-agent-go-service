package natsstream

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

// Header names (underscore-prefixed per nats-service convention).
const (
	HdrStreamEvent         = "_Stream_Event"
	HdrStreamSequence      = "_Stream_Sequence"
	HdrStreamId            = "_Stream_Id"
	HdrStreamCancelSubject  = "_Stream_Cancel_Subject"
	// HdrStreamToolResultPrefix announces the subject prefix on start. Clients
	// publish to "<prefix>.<toolCallId>" to deliver tool results from the user
	// back to a suspended agent loop.
	HdrStreamToolResultPrefix = "_Stream_Tool_Result_Prefix"
	HdrMessageId              = "_Message_Id"
	HdrStatus              = "status"
)

// StreamEventType enumerates wire-level event names. Mirrors node.StreamEventType
// but lives here so the transport package has no dependency on node.
type StreamEventType string

const (
	StreamStart      StreamEventType = "start"
	StreamDelta      StreamEventType = "delta"
	StreamThought    StreamEventType = "thought"
	StreamToolCall   StreamEventType = "tool_call"
	StreamToolResult StreamEventType = "tool_result"
	StreamComplete   StreamEventType = "complete"
	StreamError      StreamEventType = "error"
	StreamAttachment StreamEventType = "attachment"
)

// StreamEvent is one wire-level event passed between server and client.
type StreamEvent struct {
	Type     StreamEventType
	Sequence int
	StreamID string
	Data     json.RawMessage
	Header   map[string]string
}

// StartPayload is the JSON body of the first event (sequence 0).
type StartPayload struct {
	SessionID  string `json:"sessionId"`
	WorkflowID string `json:"workflowId"`
	RequestID  string `json:"requestId"`
}

// CompletePayload is the JSON body of the success terminator.
type CompletePayload struct {
	FinalText   string `json:"finalText"`
	MessageStop string `json:"messageStop"`
}

// ErrorPayload is the JSON body of the error terminator.
type ErrorPayload struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Internal string `json:"internal,omitempty"`
}

// MustJSON marshals v or panics. Used internally for terminator payload encoding
// where the inputs are tiny structs we authored (not user data).
func MustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("natsstream: marshal: %v", err))
	}
	return b
}

// ParseStreamMessage extracts a StreamEvent from a *nats.Msg arriving on the
// client subscription. Returns an error on malformed headers.
func ParseStreamMessage(m *nats.Msg) (StreamEvent, error) {
	if m == nil {
		return StreamEvent{}, errors.New("natsstream: nil msg")
	}
	evtName := m.Header.Get(HdrStreamEvent)
	if evtName == "" {
		return StreamEvent{}, errors.New("natsstream: missing _Stream_Event")
	}
	seqStr := m.Header.Get(HdrStreamSequence)
	seq, err := strconv.Atoi(seqStr)
	if err != nil {
		return StreamEvent{}, fmt.Errorf("natsstream: bad _Stream_Sequence %q: %w", seqStr, err)
	}
	hdrs := map[string]string{}
	for k, v := range m.Header {
		if len(v) > 0 {
			hdrs[k] = v[0]
		}
	}
	return StreamEvent{
		Type:     StreamEventType(evtName),
		Sequence: seq,
		StreamID: m.Header.Get(HdrStreamId),
		Data:     json.RawMessage(m.Data),
		Header:   hdrs,
	}, nil
}
