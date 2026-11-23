package natsstream

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// ToolResultHost subscribes to a per-stream subject prefix and resolves
// per-tool-call waits. Subject layout:
//
//	<basePath>_.tool_result.<streamId>.<toolCallId>
//
// One host per StreamSession. The agent loop (via NodeEnv.AwaitClientToolResult)
// asks for a particular toolCallId; this host subscribes (one-shot) to the
// matching subject and unblocks when a message arrives.
//
// Once Listen is called (StreamSession does it at creation, before the start
// event announces the prefix) the host holds one subscription on
// "<prefix>.>" and buffers results that arrive before the loop waits for
// them (the loop only waits after the model stream ends). Per toolCallId the
// first message wins, as before. Without Listen, WaitForToolResult falls back
// to a one-shot subscription.
type ToolResultHost struct {
	nc       *nats.Conn
	basePath string
	streamID string
	logger   *log.Logger

	mu      sync.Mutex
	sub     *nats.Subscription
	early   map[string][]byte      // arrived before anyone waited
	waiters map[string]chan []byte // buffered 1
	done    map[string]bool        // first message already taken
}

// maxEarly bounds the early-result buffer per stream.
const maxEarly = 64

// NewToolResultHost binds a host to a single StreamSession's lifetime.
func NewToolResultHost(nc *nats.Conn, basePath, streamID string, logger *log.Logger) *ToolResultHost {
	return &ToolResultHost{nc: nc, basePath: basePath, streamID: streamID, logger: logger}
}

// Prefix is the subject prefix announced to clients. They append "." + toolCallId
// to publish results.
func (h *ToolResultHost) Prefix() string {
	return fmt.Sprintf("%s_.tool_result.%s", h.basePath, h.streamID)
}

// SubjectFor returns the full subject for a specific tool call.
func (h *ToolResultHost) SubjectFor(toolCallID string) string {
	return h.Prefix() + "." + toolCallID
}

// Listen subscribes once to every tool result of this stream and buffers
// early ones. Idempotent.
func (h *ToolResultHost) Listen() error {
	if h.nc == nil {
		return fmt.Errorf("toolresult: nil nats conn")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sub != nil {
		return nil
	}
	sub, err := h.nc.Subscribe(h.Prefix()+".>", h.onResult)
	if err != nil {
		return fmt.Errorf("toolresult: subscribe %s.>: %w", h.Prefix(), err)
	}
	h.sub = sub
	h.early = map[string][]byte{}
	h.waiters = map[string]chan []byte{}
	h.done = map[string]bool{}
	return nil
}

// Stop drops the subscription and anything buffered. Called when the
// session ends.
func (h *ToolResultHost) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sub != nil {
		_ = h.sub.Unsubscribe()
	}
	h.sub = nil
	h.early, h.waiters, h.done = nil, nil, nil
}

func (h *ToolResultHost) onResult(m *nats.Msg) {
	id := strings.TrimPrefix(m.Subject, h.Prefix()+".")
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sub == nil || h.done[id] {
		return // stopped, or first already won
	}
	if w, ok := h.waiters[id]; ok {
		delete(h.waiters, id)
		h.done[id] = true
		w <- m.Data
		return
	}
	if _, dup := h.early[id]; dup {
		return
	}
	if len(h.early) >= maxEarly {
		if h.logger != nil {
			h.logger.Printf("toolresult: early buffer full, dropping %s", id)
		}
		return
	}
	h.early[id] = m.Data
}

// WaitForToolResult returns the first payload published on
// SubjectFor(toolCallID) — including one that arrived before this call when
// the host listens — or an error on ctx cancel or timeout.
func (h *ToolResultHost) WaitForToolResult(ctx context.Context, toolCallID string, timeout time.Duration) ([]byte, error) {
	if h.nc == nil {
		return nil, fmt.Errorf("toolresult: nil nats conn")
	}
	h.mu.Lock()
	if h.sub != nil {
		if data, ok := h.early[toolCallID]; ok {
			delete(h.early, toolCallID)
			h.done[toolCallID] = true
			h.mu.Unlock()
			return data, nil
		}
		w := make(chan []byte, 1)
		h.waiters[toolCallID] = w
		h.mu.Unlock()
		return h.await(ctx, toolCallID, w, timeout)
	}
	h.mu.Unlock()
	subj := h.SubjectFor(toolCallID)

	msgs := make(chan *nats.Msg, 1)
	sub, err := h.nc.Subscribe(subj, func(m *nats.Msg) {
		select {
		case msgs <- m:
		default:
			// drop — only the first matters
		}
	})
	if err != nil {
		return nil, fmt.Errorf("toolresult: subscribe %s: %w", subj, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("toolresult: timeout after %s waiting for %s", timeout, toolCallID)
	case m := <-msgs:
		return m.Data, nil
	}
}

// await waits on a listening host's waiter channel.
func (h *ToolResultHost) await(ctx context.Context, toolCallID string, w chan []byte, timeout time.Duration) ([]byte, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var err error
	select {
	case data := <-w:
		return data, nil
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		err = fmt.Errorf("toolresult: timeout after %s waiting for %s", timeout, toolCallID)
	}
	h.mu.Lock()
	if h.waiters != nil && h.waiters[toolCallID] == w {
		delete(h.waiters, toolCallID)
	}
	h.mu.Unlock()
	select {
	case data := <-w: // delivered while we gave up: still first, use it
		return data, nil
	default:
		return nil, err
	}
}
