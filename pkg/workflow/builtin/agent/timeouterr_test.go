package agent

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// TestAgent_LLMError_ClassifiesDeadlineAndCancel asserts that an LLM stream
// error wrapping context.DeadlineExceeded (a mid-call workflow deadline) or
// context.Canceled (a client disconnect) surfaces to the stream as
// "timeout"/"cancelled" respectively, not the generic "llm-error" code.
// Task 3 depends on this classification to decide retry/backoff behavior.
func TestAgent_LLMError_ClassifiesDeadlineAndCancel(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantCode string
	}{
		{
			name:     "deadline exceeded mid-call",
			err:      fmt.Errorf("bedrock: %w", context.DeadlineExceeded),
			wantCode: "timeout",
		},
		{
			name:     "client cancelled mid-call",
			err:      fmt.Errorf("bedrock: %w", context.Canceled),
			wantCode: "cancelled",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			llm := &flakeyLLM{preErr: tc.err}
			a := &agentNode{
				cfg:        Config{MaxIterations: 3, SystemMessage: "sys"},
				env:        testNodeEnv{},
				workflowID: "wf",
				llm:        llm,
			}
			sink := &recordingSink{}
			_ = a.Process(context.Background(), node.AgentInput{Message: "x", SessionID: "s"}, sink)

			if sink.term.Type != node.StreamError {
				t.Fatalf("expected StreamError, got %+v", sink.term)
			}
			if !strings.Contains(string(sink.term.Data), `"code":"`+tc.wantCode+`"`) {
				t.Errorf("expected code %q; got %s", tc.wantCode, sink.term.Data)
			}
		})
	}
}
