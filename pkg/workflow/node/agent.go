package node

import "context"

// Agent orchestrates the LLM↔tool loop. The executor owns invoking Process
// once per TriggerEvent; the agent emits stream events through sink.
type Agent interface {
	Node
	Process(ctx context.Context, in AgentInput, sink StreamSink) error
}

// AgentInput is the per-request payload passed to Agent.Process.
type AgentInput struct {
	Message     string
	SessionID   string
	UserID      string
	RequestID   string
	Headers     map[string]string
	RenderCtx   RenderCtx
	Attachments []Attachment
}

// Attachment is one uploaded file accompanying a user message. Data holds raw
// bytes; the agent converts it into the appropriate ContentBlock (image or
// document) based on MediaType.
type Attachment struct {
	URL       string
	MediaType string
	Filename  string
	Size      int64
	Data      []byte
}
