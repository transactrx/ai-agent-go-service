package node

import (
	"context"
	"encoding/json"
)

// LLMProvider streams Anthropic-shaped events from a model backend.
type LLMProvider interface {
	Node
	Stream(ctx context.Context, req LLMRequest, out chan<- LLMEvent) error
}

// LLMRequest is the provider-agnostic request envelope. The agent assembles
// this from history + system + connected tool peers.
type LLMRequest struct {
	System      string
	Messages    []Message
	Tools       []ToolSpec
	MaxTokens   int
	Temperature *float64
	Stop        []string

	// ToolChoiceName, when non-empty, forces the model to call this tool
	// (Anthropic tool_choice {"type":"tool","name":...}). Production agent
	// requests leave it empty; the bedrock auto-update validation probe is
	// the only setter.
	ToolChoiceName string
}

// LLMEvent is the unit of stream output from a provider. The agent translates
// these into StreamEvents and tool invocations.
type LLMEvent struct {
	Kind    LLMEventKind
	Delta   string
	Stop    string // populated when Kind == LLMMessageStop ("end_turn", "tool_use", ...)
	ToolUse *LLMToolUse
	Error   error
}

// LLMEventKind enumerates the event types a provider emits.
type LLMEventKind string

const (
	LLMTextDelta    LLMEventKind = "text_delta"
	LLMToolUseStart LLMEventKind = "tool_use_start"
	LLMToolUseDelta LLMEventKind = "tool_use_delta"
	LLMToolUseStop  LLMEventKind = "tool_use_stop"
	LLMMessageStop  LLMEventKind = "message_stop"
	LLMError        LLMEventKind = "error"
)

// LLMToolUse accumulates a single tool_use block across start/delta/stop events.
type LLMToolUse struct {
	ID        string
	Name      string
	InputJSON json.RawMessage
}
