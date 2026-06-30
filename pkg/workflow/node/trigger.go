package node

import (
	"context"
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
)

// Trigger nodes initiate workflow runs. Engine calls Subscribe once after Init;
// each event the trigger emits drives one workflow run via sink.Emit.
type Trigger interface {
	Node
	Subscribe(ctx context.Context, sink TriggerSink) error
}

// TriggerSink is how a trigger hands events to the executor.
type TriggerSink interface {
	Emit(ctx context.Context, evt TriggerEvent) error
}

// TriggerEvent is one logical request — a chat message, a cron tick, etc.
type TriggerEvent struct {
	RequestID  string
	Identity   identity.Identity
	SessionID  string
	Body       json.RawMessage
	Headers    map[string]string
	StreamSink StreamSink   // non-nil when responseMode == "streaming"
	Reply      ReplyFunc    // non-nil when responseMode == "single"
	Done       <-chan struct{}
}

// ReplyFunc is the single-response callback for non-streaming workflows.
type ReplyFunc func(ctx context.Context, body []byte, headers map[string]string) error

// StreamSink is the executor-facing handle through which agent events flow
// back to the trigger's transport (NATS reply inbox in cycle 1).
type StreamSink interface {
	Send(ctx context.Context, evt StreamEvent) error
	Close(ctx context.Context, terminator StreamEvent) error
}

// StreamEvent is one event in a streaming response. Terminator events
// (StreamComplete / StreamError) MUST be sent via Close, never Send.
type StreamEvent struct {
	Type     StreamEventType
	Sequence int
	Data     json.RawMessage
	Header   map[string]string
}

// StreamEventType enumerates the wire-protocol event names. See spec §5.1.
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
