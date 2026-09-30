package node

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// LLMTiming is the per-call measurement every LLMProvider logs at the end of
// Stream (spec 2026-09-25-inference-gateway-llm §3.4). It exists so the direct
// Bedrock path and the inferenceGateway path can be compared line for line.
type LLMTiming struct {
	Provider string
	Model    string // resolved id actually invoked; "" when unknown

	Start      time.Time
	FirstEvent time.Time // zero when nothing was received
	End        time.Time

	HasUsage     bool
	InputTokens  int
	OutputTokens int

	Stop string
	Err  error
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// String renders the key=value tail of the log line (everything after node=).
func (t LLMTiming) String() string {
	ttfb := "-"
	if !t.FirstEvent.IsZero() && !t.Start.IsZero() {
		ttfb = strconv.FormatInt(t.FirstEvent.Sub(t.Start).Milliseconds(), 10)
	}
	total := "-"
	if !t.End.IsZero() && !t.Start.IsZero() {
		total = strconv.FormatInt(t.End.Sub(t.Start).Milliseconds(), 10)
	}
	in, out := "-", "-"
	if t.HasUsage {
		in = strconv.Itoa(t.InputTokens)
		out = strconv.Itoa(t.OutputTokens)
	}
	errs := "-"
	if t.Err != nil {
		errs = strconv.Quote(t.Err.Error())
	}
	return fmt.Sprintf("provider=%s model=%s ttfb_ms=%s total_ms=%s in_tok=%s out_tok=%s stop=%s err=%s",
		dash(t.Provider), dash(t.Model), ttfb, total, in, out, dash(t.Stop), errs)
}

// LogLLMTiming writes the llm-timing line. A nil logger is a no-op so
// providers can call it unconditionally.
func LogLLMTiming(logger *log.Logger, wfID, nodeID string, t LLMTiming) {
	if logger == nil {
		return
	}
	logger.Printf("llm-timing wf=%s node=%s %s", wfID, nodeID, t.String())
}
