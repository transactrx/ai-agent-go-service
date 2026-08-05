package bedrock

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Claude 5-generation models reject forced tool_choice unless thinking is
// explicitly disabled — the probe/health-check envelope must carry it.
func TestPayloadThinkingDisabledForGen5(t *testing.T) {
	req := node.LLMRequest{
		MaxTokens:      64,
		ToolChoiceName: "echo",
		Tools:          []node.ToolSpec{{Name: "echo", Description: "d", InputSchema: json.RawMessage(`{}`)}},
		Messages:       []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "ping"}}}},
	}
	cfg := Config{MaxTokens: 64, AnthropicVersion: defaultAnthropicVersion}
	for _, id := range []string{
		"us.anthropic.claude-opus-5",
		"anthropic.claude-sonnet-5",
		"us.anthropic.claude-sonnet-5-20260101-v1:0",
	} {
		payload, err := buildAnthropicPayload(req, cfg, id)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if !strings.Contains(string(payload), `"thinking":{"type":"disabled"}`) {
			t.Errorf("%s: envelope missing thinking:disabled: %s", id, payload)
		}
		if !strings.Contains(string(payload), `"tool_choice"`) {
			t.Errorf("%s: tool_choice lost", id)
		}
	}
}

// SAFETY: below generation 5 (and for unparseable IDs) the envelope must be
// byte-identical to the pre-change output — no thinking field at all.
func TestPayloadByteIdenticalBelowGen5(t *testing.T) {
	req := node.LLMRequest{
		MaxTokens: 64,
		Messages:  []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "hi"}}}},
	}
	cfg := Config{MaxTokens: 64, AnthropicVersion: defaultAnthropicVersion}
	for _, id := range []string{
		"us.anthropic.claude-opus-4-7",
		"anthropic.claude-opus-4-8",
		"us.anthropic.claude-sonnet-4-6",
		"claude-3-5-sonnet",      // legacy, unparseable by parseModelID
		"arn:aws:bedrock:custom", // unparseable
	} {
		payload, err := buildAnthropicPayload(req, cfg, id)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if strings.Contains(string(payload), "thinking") {
			t.Errorf("%s: envelope must not contain thinking: %s", id, payload)
		}
	}
}

// The last-known-good retry may cross a generation boundary, so the payload is
// rebuilt per attempt with that attempt's model ID.
func TestStreamRetryRebuildsPayloadPerModel(t *testing.T) {
	fake := &fakeInvoker{errs: []error{validationErr(), errors.New("still down")}}
	b := &bedrockLLM{
		cfg:           Config{MaxTokens: 16, AnthropicVersion: defaultAnthropicVersion},
		model:         "us.anthropic.claude-opus-5",
		lastKnownGood: "us.anthropic.claude-opus-4-8",
		client:        fake,
	}
	out := make(chan node.LLMEvent, 8)
	if err := b.Stream(context.Background(), probeReq(), out); err == nil {
		t.Fatal("expected error")
	}
	if len(fake.bodies) != 2 {
		t.Fatalf("calls = %d, want 2", len(fake.bodies))
	}
	if !strings.Contains(string(fake.bodies[0]), `"thinking"`) {
		t.Errorf("first (gen5) body missing thinking: %s", fake.bodies[0])
	}
	if strings.Contains(string(fake.bodies[1]), `"thinking"`) {
		t.Errorf("second (gen4 lkg) body must not contain thinking: %s", fake.bodies[1])
	}
}
