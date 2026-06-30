package natsstream

import (
	"context"
	"fmt"
	"log"
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
type ToolResultHost struct {
	nc       *nats.Conn
	basePath string
	streamID string
	logger   *log.Logger
}

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

// WaitForToolResult subscribes one-shot to SubjectFor(toolCallID) and returns
// the first message's payload, or an error on ctx cancel or timeout.
func (h *ToolResultHost) WaitForToolResult(ctx context.Context, toolCallID string, timeout time.Duration) ([]byte, error) {
	if h.nc == nil {
		return nil, fmt.Errorf("toolresult: nil nats conn")
	}
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
