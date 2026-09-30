package bedrock

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// message_start carries input tokens, message_delta carries output tokens
// and the stop reason; both must land in anthropicStats.
func TestHandleAnthropicChunkStats(t *testing.T) {
	chunks := []string{
		`{"type":"message_start","message":{"id":"m1","usage":{"input_tokens":1200,"output_tokens":1}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":80}}`,
	}
	out := make(chan node.LLMEvent, 8)
	st := &anthropicStats{}
	for _, c := range chunks {
		if err := handleAnthropicChunkStats(context.Background(), []byte(c), map[int]*node.LLMToolUse{}, out, st); err != nil {
			t.Fatal(err)
		}
	}
	if !st.hasUsage || st.inputTokens != 1200 || st.outputTokens != 80 || st.stop != "end_turn" {
		t.Fatalf("stats = %+v", *st)
	}
	// nil stats must still be accepted (existing callers).
	if err := handleAnthropicChunk(context.Background(), []byte(chunks[0]), map[int]*node.LLMToolUse{}, out); err != nil {
		t.Fatal(err)
	}
}

// A failed invoke still logs one llm-timing line naming the model and error.
func TestStreamLogsTimingOnInvokeError(t *testing.T) {
	var buf bytes.Buffer
	fake := &fakeInvoker{errs: []error{validationErr()}}
	b := &bedrockLLM{
		cfg:    Config{Model: "x", MaxTokens: 16, AnthropicVersion: defaultAnthropicVersion},
		model:  "us.anthropic.claude-opus-5-5",
		client: fake,
		logger: log.New(&buf, "", 0),
		wfID:   "wf1", nodeID: "n1",
	}
	out := make(chan node.LLMEvent, 8)
	if err := b.Stream(context.Background(), probeReq(), out); err == nil {
		t.Fatal("expected error")
	}
	line := ""
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(l, "llm-timing ") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no llm-timing line in:\n%s", buf.String())
	}
	for _, want := range []string{"wf=wf1", "node=n1", "provider=ai/bedrock", "model=us.anthropic.claude-opus-5-5", "ttfb_ms=-", "in_tok=-", `err="`} {
		if !strings.Contains(line, want) {
			t.Fatalf("line %q lacks %q", line, want)
		}
	}
}
