package engine

import "context"

// EngineTracer is the cycle-1 seam for OpenTelemetry. Cycle 1 wires NoOpTracer.
type EngineTracer interface {
	StartSpan(ctx context.Context, name string) (context.Context, Span)
}

// Span ends a tracer span; cycle-1 NoOp returns a NoOpSpan.
type Span interface {
	End()
	SetError(err error)
	SetAttribute(key string, value any)
}

// NoOpTracer is the cycle-1 default.
type NoOpTracer struct{}

func (NoOpTracer) StartSpan(ctx context.Context, _ string) (context.Context, Span) {
	return ctx, noOpSpan{}
}

type noOpSpan struct{}

func (noOpSpan) End()                          {}
func (noOpSpan) SetError(error)                {}
func (noOpSpan) SetAttribute(string, any)      {}
