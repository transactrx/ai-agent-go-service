package node

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

func TestLLMTimingString(t *testing.T) {
	start := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	full := LLMTiming{
		Provider: "ai/inference-gateway", Model: "us.anthropic.claude-opus-5-5",
		Start: start, FirstEvent: start.Add(350 * time.Millisecond), End: start.Add(2 * time.Second),
		HasUsage: true, InputTokens: 1200, OutputTokens: 80, Stop: "end_turn",
	}
	want := "provider=ai/inference-gateway model=us.anthropic.claude-opus-5-5 ttfb_ms=350 total_ms=2000 in_tok=1200 out_tok=80 stop=end_turn err=-"
	if got := full.String(); got != want {
		t.Fatalf("String() =\n%s\nwant\n%s", got, want)
	}
	failed := LLMTiming{Provider: "ai/bedrock", Start: start, End: start.Add(time.Second), Err: errors.New("boom \"x\"")}
	want = `provider=ai/bedrock model=- ttfb_ms=- total_ms=1000 in_tok=- out_tok=- stop=- err="boom \"x\""`
	if got := failed.String(); got != want {
		t.Fatalf("String() =\n%s\nwant\n%s", got, want)
	}
}

func TestLogLLMTiming(t *testing.T) {
	var buf bytes.Buffer
	lg := log.New(&buf, "", 0)
	LogLLMTiming(lg, "wf1", "n1", LLMTiming{Provider: "p", Start: time.Now(), End: time.Now()})
	if !strings.HasPrefix(buf.String(), "llm-timing wf=wf1 node=n1 provider=p ") {
		t.Fatalf("unexpected line: %q", buf.String())
	}
	LogLLMTiming(nil, "wf1", "n1", LLMTiming{}) // must not panic
}
