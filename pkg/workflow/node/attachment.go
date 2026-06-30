package node

import (
	"context"
	"encoding/json"
)

// ToolAttachment is a piece of presentation data the tool wants attached to
// the stream without showing it to the LLM — opaque client-rendered payload
// shipped alongside the tool_result block. The tool's tool_result block (the
// return value of Invoke) is what the LLM sees; attachments flow on the
// stream sink in parallel.
type ToolAttachment struct {
	Kind    string          // tool-defined discriminator for the client renderer
	Payload json.RawMessage // opaque to the engine; rendered by the client
}

// AttachmentEmitter is the narrow capability tools see — only attachment
// events, no access to the underlying StreamSink. Agent supplies a concrete
// implementation in context before each Invoke.
type AttachmentEmitter interface {
	Attach(ctx context.Context, a ToolAttachment) error
}

// AttachmentEmitterFunc is the func-style adapter for tests and simple
// implementations.
type AttachmentEmitterFunc func(ctx context.Context, a ToolAttachment) error

// Attach implements AttachmentEmitter.
func (f AttachmentEmitterFunc) Attach(ctx context.Context, a ToolAttachment) error {
	return f(ctx, a)
}

type attachmentEmitterKey struct{}

// WithAttachmentEmitter returns a derived context carrying e. Agent loop
// installs this before invoking each tool.
func WithAttachmentEmitter(ctx context.Context, e AttachmentEmitter) context.Context {
	return context.WithValue(ctx, attachmentEmitterKey{}, e)
}

// EmitterFromContext returns the emitter stored in ctx, or false if absent.
// Tools that emit attachments should treat absence as a no-op (skip the
// attach), not as an error — they may run in non-streaming contexts.
func EmitterFromContext(ctx context.Context) (AttachmentEmitter, bool) {
	e, ok := ctx.Value(attachmentEmitterKey{}).(AttachmentEmitter)
	return e, ok
}
