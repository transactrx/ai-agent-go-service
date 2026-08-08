package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// TestAgentEmptyToolResultPadding asserts that a successful tool invocation
// which returns blank/empty/null JSON is padded before it reaches the LLM.
// Bedrock rejects user-message content blocks with empty content, so a tool
// result of "" or null must never be forwarded verbatim.
func TestAgentEmptyToolResultPadding(t *testing.T) {
	cases := []struct {
		name string
		out  json.RawMessage
	}{
		{name: "no bytes", out: json.RawMessage("")},
		{name: "empty JSON string", out: json.RawMessage(`""`)},
		{name: "JSON null", out: json.RawMessage("null")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tool := &fakeTool{name: "T", out: tc.out}
			a := &agentNode{
				cfg:        Config{MaxIterations: 5, SystemMessage: "sys"},
				env:        testNodeEnv{},
				workflowID: "wf",
				tools:      []node.Tool{tool},
				llm: &fakeLLM{scripts: [][]node.LLMEvent{
					{
						{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "T", InputJSON: []byte(`{}`)}},
						{Kind: node.LLMMessageStop, Stop: "tool_use"},
					},
					{
						{Kind: node.LLMTextDelta, Delta: "done"},
						{Kind: node.LLMMessageStop, Stop: "end_turn"},
					},
				}},
			}
			sink := &recordingSink{}
			if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
				t.Fatal(err)
			}

			var toolResultEvent *node.StreamEvent
			for i := range sink.events {
				if sink.events[i].Type == node.StreamToolResult {
					toolResultEvent = &sink.events[i]
				}
			}
			if toolResultEvent == nil {
				t.Fatal("no tool_result stream event captured")
			}

			var payload struct {
				Output json.RawMessage `json:"output"`
			}
			if err := json.Unmarshal(toolResultEvent.Data, &payload); err != nil {
				t.Fatalf("unmarshal tool_result event: %v", err)
			}

			var got string
			if err := json.Unmarshal(payload.Output, &got); err != nil {
				t.Fatalf("tool_result output is not a JSON string: %s: %v", payload.Output, err)
			}
			if got != "(tool returned no output)" {
				t.Fatalf("empty tool output not padded: %q", got)
			}
		})
	}
}
