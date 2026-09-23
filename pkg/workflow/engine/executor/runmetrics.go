package executor

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// runTimer wraps a run's StreamSink to measure it: time to the first event,
// time to the first text delta, total duration, event count, and how the run
// ended. The executor logs one RUN_METRIC line per run from it. It forwards
// every call unchanged.
type runTimer struct {
	inner node.StreamSink
	start time.Time
	clock func() time.Time

	mu         sync.Mutex
	events     int
	tools      int
	firstEvent time.Duration
	firstText  time.Duration
	end        string // complete | error | "" (never closed)
	stop       string // messageStop on complete, error code on error
}

func newRunTimer(inner node.StreamSink, clock func() time.Time) *runTimer {
	return &runTimer{inner: inner, start: clock(), clock: clock}
}

func (r *runTimer) mark(evt node.StreamEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	since := r.clock().Sub(r.start)
	r.events++
	if r.events == 1 {
		r.firstEvent = since
	}
	if evt.Type == node.StreamDelta && r.firstText == 0 {
		r.firstText = since
	}
	if evt.Type == node.StreamToolCall {
		r.tools++
	}
}

func (r *runTimer) Send(ctx context.Context, evt node.StreamEvent) error {
	r.mark(evt)
	return r.inner.Send(ctx, evt)
}

func (r *runTimer) Close(ctx context.Context, term node.StreamEvent) error {
	r.mark(term)
	r.mu.Lock()
	switch term.Type {
	case node.StreamComplete:
		r.end = "complete"
		var p struct {
			MessageStop string `json:"messageStop"`
		}
		_ = json.Unmarshal(term.Data, &p)
		r.stop = p.MessageStop
	case node.StreamError:
		r.end = "error"
		var p struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(term.Data, &p)
		r.stop = p.Code
	default:
		r.end = string(term.Type)
	}
	r.mu.Unlock()
	return r.inner.Close(ctx, term)
}

// logDone writes the per-run line. Identity is deliberately left out; the
// request and session ids correlate with the trigger's own log lines.
func (r *runTimer) logDone(e *Executor, evt node.TriggerEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	end := r.end
	if end == "" {
		end = "unclosed"
	}
	e.logger.Printf("RUN_METRIC event=run.done workflow=%s request=%s session=%s end=%s stop=%s tools=%d events=%d first_event_ms=%d first_text_ms=%d ms=%d",
		e.wf.ID, evt.RequestID, evt.SessionID, end, r.stop, r.tools, r.events,
		r.firstEvent.Milliseconds(), r.firstText.Milliseconds(), r.clock().Sub(r.start).Milliseconds())
}
