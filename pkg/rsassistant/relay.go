package rsassistant

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

// emitter is the subset of *agent.Stream the relay needs. Kept as an
// interface so the mapping is unit-testable with a recorder.
type emitter interface {
	Text(delta string)
	ToolUse(toolUseID, toolName string, input any)
	ToolResult(toolUseID, toolName, result, toolErr string)
	Data(kind string, payload any)
	Status(text string)
	Done(stopReason string, usage *wire.Usage)
	Error(message string, code int)
}

// Engine stream payloads (builtin/agent/loop.go, natsstream/protocol.go).
type deltaPayload struct {
	Text string `json:"text"`
}
type toolCallPayload struct {
	ToolUseID string          `json:"toolUseId"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
}
type toolResultPayload struct {
	ToolUseID string          `json:"toolUseId"`
	Output    json.RawMessage `json:"output"`
	IsError   bool            `json:"isError"`
}
type attachmentPayload struct {
	ToolUseID string          `json:"toolUseId"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
}

// relay converts one engine stream into nats-agent stream calls.
type relay struct {
	toolNames map[string]string // toolUseId -> tool name (tool_result carries no name)
	sawDelta  bool
	done      bool
	n         int
}

func newRelay() *relay { return &relay{toolNames: map[string]string{}} }

func (r *relay) terminated() bool { return r.done }
func (r *relay) count() int       { return r.n }

// apply maps one engine event onto the emitter (spec §3.6 table).
func (r *relay) apply(ev natsstream.StreamEvent, out emitter) {
	if r.done {
		return
	}
	r.n++
	switch ev.Type {
	case natsstream.StreamStart:
		// nothing to emit; nats-agent already sent its own start event
	case natsstream.StreamDelta:
		var p deltaPayload
		_ = json.Unmarshal(ev.Data, &p)
		if p.Text != "" {
			r.sawDelta = true
			out.Text(p.Text)
		}
	case natsstream.StreamThought:
		var p deltaPayload
		_ = json.Unmarshal(ev.Data, &p)
		if p.Text != "" {
			out.Status(p.Text)
		}
	case natsstream.StreamToolCall:
		var p toolCallPayload
		_ = json.Unmarshal(ev.Data, &p)
		r.toolNames[p.ToolUseID] = p.Name
		out.ToolUse(p.ToolUseID, p.Name, p.Input)
	case natsstream.StreamToolResult:
		var p toolResultPayload
		_ = json.Unmarshal(ev.Data, &p)
		res := rawToText(p.Output)
		errText := ""
		if p.IsError {
			errText = res
		}
		out.ToolResult(p.ToolUseID, r.toolNames[p.ToolUseID], res, errText)
	case natsstream.StreamAttachment:
		var p attachmentPayload
		_ = json.Unmarshal(ev.Data, &p)
		out.Data("attachment", p.Payload)
		if u := attachmentURL(p.Payload); u != "" {
			out.Text("\n" + u + "\n")
		}
	case natsstream.StreamComplete:
		var p natsstream.CompletePayload
		_ = json.Unmarshal(ev.Data, &p)
		if !r.sawDelta && p.FinalText != "" {
			out.Text(p.FinalText)
		}
		out.Done(mapStop(p.MessageStop), nil)
		r.done = true
	case natsstream.StreamError:
		var p natsstream.ErrorPayload
		_ = json.Unmarshal(ev.Data, &p)
		msg := strings.TrimSpace(p.Code + ": " + p.Message)
		if msg == ":" {
			msg = "workflow error"
		}
		out.Error(msg, wire.CodeUpstream)
		r.done = true
	}
}

// finish is called once the engine channel closes. If no terminator was
// relayed, the consult ends with an upstream error (timeout, closed conn).
func (r *relay) finish(streamErr error, out emitter) {
	if r.done {
		return
	}
	msg := "workflow stream ended without completion"
	if streamErr != nil {
		msg += ": " + streamErr.Error()
	}
	out.Error(msg, wire.CodeUpstream)
	r.done = true
}

// rawToText renders a tool output for the nats-agent toolResult string.
func rawToText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// attachmentURL extracts a url/imageUrl string from an attachment payload.
func attachmentURL(raw json.RawMessage) string {
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return ""
	}
	for _, k := range []string{"url", "imageUrl"} {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// mapStop converts the engine's Bedrock-style messageStop to nats-agent's.
func mapStop(messageStop string) string {
	if messageStop == "max_tokens" {
		return wire.StopMaxTokens
	}
	return wire.StopEndTurn
}

// gate is the bridge's own identity check (spec §3.4 gate 3). It is
// independent of env: the engine is never called without an identity-derived
// account and user; in strict mode the identity must also be Verified.
func gate(id agent.Identity, strict bool) error {
	if strict && !id.Verified {
		return errors.New("identity not verified")
	}
	if strings.TrimSpace(id.AccountID) == "" || strings.TrimSpace(id.UserID) == "" {
		return errors.New("identity did not resolve account and user")
	}
	return nil
}

// messageText joins the text blocks of a nats-agent message.
func messageText(m wire.Message) string {
	parts := make([]string, 0, len(m.Content))
	for _, b := range m.Content {
		if b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}
