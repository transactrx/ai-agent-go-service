package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/render"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// renderingEnv is a NodeEnv whose Render uses the REAL engine renderer (the
// production catalog), unlike testNodeEnv whose Render is pass-through. This
// lets the test exercise the actual loop.go render path end-to-end.
type renderingEnv struct{ testNodeEnv }

func (renderingEnv) Render(s string, c node.RenderCtx) (string, error) {
	return render.NewRenderer().Render(s, c)
}

// capturingLLM records the System prompt it receives, then emits a minimal
// final answer so the agent turn completes.
type capturingLLM struct{ gotSystem string }

func (capturingLLM) Spec() node.NodeSpec                          { return node.NodeSpec{Role: node.RoleLLM} }
func (capturingLLM) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (capturingLLM) Close(_ context.Context) error                { return nil }
func (l *capturingLLM) Stream(_ context.Context, req node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	l.gotSystem = req.System
	out <- node.LLMEvent{Kind: node.LLMTextDelta, Delta: "ok"}
	out <- node.LLMEvent{Kind: node.LLMMessageStop, Stop: "end_turn"}
	return nil
}

// TestDynamicFixedPromptRendersInjectedContext is the integration test for the
// dynamic-fixed-prompt feature: it runs the REAL agent loop with the REAL
// renderer over a fixed prompt containing the new placeholders, feeding the
// RenderCtx exactly as executor.Run pins it (UserName/TimeZone from Identity,
// IndexMapping from the MappingProvider). It asserts the system prompt the LLM
// actually receives has every placeholder expanded with the injected values.
//
// (The live _mapping fetch is covered in pkg/workflow/builtin/opensearch; the
// executor's MappingProvider discovery + RenderCtx pinning in
// pkg/workflow/engine/executor. This closes the gap between those: the merged
// fixed prompt truly renders the injected context.)
func TestDynamicFixedPromptRendersInjectedContext(t *testing.T) {
	const fixed = "# Rules\nBe careful.\n\n" +
		"The user asking is {{userName}} (time zone {{userTimeZone}}). " +
		"Their current local time is {{nowLocal}} ({{nowLocalWeekday}}). " +
		"Current UTC time is {{nowUtc}} UTC ({{nowUtcWeekday}}).\n\n" +
		"Current index field mapping (fetched live from the most recent index before today):\n" +
		"{{indexMapping}}"

	llm := &capturingLLM{}
	a := &agentNode{
		cfg:        Config{MaxIterations: 5, SystemMessageFixed: fixed},
		env:        renderingEnv{},
		workflowID: "wf",
		llm:        llm,
	}
	a.setFlex("") // merged = fixed
	a.mem = &fakeMem{}

	// RenderCtx as executor.Run builds it: 02:30 UTC on the 18th == 22:30 EDT on
	// the 17th. IndexMapping is what the opensearch MappingProvider would return.
	rc := node.RenderCtx{
		Now:          time.Date(2026, 6, 18, 2, 30, 0, 0, time.UTC),
		UserName:     "Ada Lovelace",
		TimeZone:     "America/New_York",
		IndexMapping: `{"dev.cpe-2026-06-17":{"mappings":{"properties":{"bin":{"type":"text"}}}}}`,
	}

	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1", RenderCtx: rc}, sink); err != nil {
		t.Fatalf("Process: %v", err)
	}

	sys := llm.gotSystem
	wants := []string{
		"Ada Lovelace",         // {{userName}}
		"America/New_York",     // {{userTimeZone}}
		"2026-06-17 22:30",     // {{nowLocal}} (user tz)
		"Wednesday",            // {{nowLocalWeekday}} (local day)
		"2026-06-18 02:30",     // {{nowUtc}} (UTC retained)
		`"dev.cpe-2026-06-17"`, // {{indexMapping}} index key
		`"bin"`,                // {{indexMapping}} field
	}
	for _, w := range wants {
		if !strings.Contains(sys, w) {
			t.Errorf("system prompt missing %q\n---\n%s", w, sys)
		}
	}
	// Proves expansion actually happened (no raw placeholders survived).
	for _, ph := range []string{"{{userName}}", "{{nowLocal}}", "{{indexMapping}}", "{{userTimeZone}}"} {
		if strings.Contains(sys, ph) {
			t.Errorf("placeholder %s was not expanded", ph)
		}
	}
}
