package natschat

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/errcode"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// collectSink implements node.StreamSink for responseMode "single". Instead of
// forwarding events to a reply inbox it keeps the little a one-shot caller
// needs — attachment payloads; the final text arrives whole in the complete
// terminator — and delivers exactly one singleResult on done at Close. Because
// the executor receives a non-nil sink, the executor and agent loop run the
// single path unchanged.
type collectSink struct {
	sessionID string

	mu          sync.Mutex
	closed      bool
	attachments []json.RawMessage

	done chan singleResult
}

// singleResult is what handleSingle turns into the one NATS reply.
type singleResult struct {
	body       []byte // response JSON when errCode == ""
	errCode    string
	errMessage string
}

// singleResponseBody is the wire shape of a single-mode success reply.
type singleResponseBody struct {
	SessionID   string            `json:"sessionId"`
	FinalText   string            `json:"finalText"`
	StopReason  string            `json:"stopReason"`
	Attachments []json.RawMessage `json:"attachments"`
}

func newCollectSink(sessionID string) *collectSink {
	return &collectSink{sessionID: sessionID, done: make(chan singleResult, 1)}
}

// Send keeps attachment payloads and discards everything else: deltas
// re-arrive aggregated in the terminator, and tool_call/tool_result chatter
// has no meaning to a caller that only wants the answer.
func (c *collectSink) Send(_ context.Context, evt node.StreamEvent) error {
	if evt.Type != node.StreamAttachment {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.attachments = append(c.attachments, evt.Data)
	return nil
}

// Close resolves the request exactly once; later calls are no-ops.
func (c *collectSink) Close(_ context.Context, term node.StreamEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true

	if term.Type == node.StreamComplete {
		var p natsstream.CompletePayload
		if err := json.Unmarshal(term.Data, &p); err != nil {
			c.done <- singleResult{errCode: errcode.ExecutorFailed, errMessage: "malformed complete payload: " + err.Error()}
			return nil
		}
		atts := c.attachments
		if atts == nil {
			atts = []json.RawMessage{}
		}
		body, err := json.Marshal(singleResponseBody{
			SessionID:   c.sessionID,
			FinalText:   p.FinalText,
			StopReason:  p.MessageStop,
			Attachments: atts,
		})
		if err != nil {
			c.done <- singleResult{errCode: errcode.ExecutorFailed, errMessage: "marshal single response: " + err.Error()}
			return nil
		}
		c.done <- singleResult{body: body}
		return nil
	}

	var p natsstream.ErrorPayload
	_ = json.Unmarshal(term.Data, &p)
	if p.Code == "" {
		p.Code = errcode.ExecutorFailed
	}
	if p.Message == "" {
		p.Message = "workflow failed"
	}
	c.done <- singleResult{errCode: p.Code, errMessage: p.Message}
	return nil
}
