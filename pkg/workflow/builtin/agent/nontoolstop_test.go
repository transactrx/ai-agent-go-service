package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// TestAgentNonToolStopWithoutToolCallsCompletes covers a model turn that ends
// with a stop reason other than end_turn/tool_use (max_tokens, stop_sequence)
// and no tool calls. The loop must complete with the text it has instead of
// appending an empty tool_result user message, which Bedrock rejects with
// "user messages must have non-empty content".
func TestAgentNonToolStopWithoutToolCallsCompletes(t *testing.T) {
	for _, stop := range []string{"max_tokens", "stop_sequence"} {
		t.Run(stop, func(t *testing.T) {
			llm := &fakeLLM{scripts: [][]node.LLMEvent{{
				{Kind: node.LLMTextDelta, Delta: "partial answer"},
				{Kind: node.LLMMessageStop, Stop: stop},
			}}}
			a := &agentNode{
				cfg:        Config{MaxIterations: 5, SystemMessage: "sys"},
				env:        testNodeEnv{},
				workflowID: "wf",
				llm:        llm,
			}
			sink := &recordingSink{}
			if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
				t.Fatal(err)
			}
			if llm.calls != 1 {
				t.Fatalf("LLM called %d times, want 1 (no second call with an empty user message)", llm.calls)
			}
			if sink.term.Type != node.StreamComplete {
				t.Fatalf("terminator = %s (%s), want complete", sink.term.Type, sink.term.Data)
			}
			var p struct {
				FinalText   string `json:"finalText"`
				MessageStop string `json:"messageStop"`
			}
			_ = json.Unmarshal(sink.term.Data, &p)
			if p.FinalText != "partial answer" || p.MessageStop != stop {
				t.Fatalf("complete = %+v, want finalText=partial answer messageStop=%s", p, stop)
			}
		})
	}
}
