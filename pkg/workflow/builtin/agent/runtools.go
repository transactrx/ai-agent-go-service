package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// runClientTool is a client-only tool offered for one run only
// (node.WithRunClientTools). The loop never invokes it: its result comes
// from the client, like a ClientUITool's.
type runClientTool struct{ spec node.ToolSpec }

func (t runClientTool) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "run/client-tool", Role: node.RoleTool}
}
func (runClientTool) Init(context.Context, node.NodeEnv) error { return nil }
func (runClientTool) Close(context.Context) error              { return nil }
func (t runClientTool) ToolSpec() node.ToolSpec                { return t.spec }
func (runClientTool) FailurePolicy() node.FailurePolicy        { return node.FailureSurfaceToLLM }
func (runClientTool) RetryPolicy() node.RetryPolicy            { return nil }
func (runClientTool) ClientOnly() bool                         { return true }
func (runClientTool) Invoke(context.Context, json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("run client tool: answered by the client")
}

// runTools returns the run's extra client-only tools whose names no
// connected tool already uses (nil = none).
func (a *agentNode) runTools(ctx context.Context) []node.Tool {
	var out []node.Tool
	for _, s := range node.RunClientTools(ctx) {
		if s.Name == "" || a.lookupTool(s.Name) != nil {
			continue
		}
		out = append(out, runClientTool{spec: s})
	}
	return out
}

// findTool looks up a connected tool, then the run's extra tools.
func findTool(connected node.Tool, run []node.Tool, name string) node.Tool {
	if connected != nil {
		return connected
	}
	for _, t := range run {
		if t.ToolSpec().Name == name {
			return t
		}
	}
	return nil
}

// stopAwaitingInput is the messageStop of a turn ended by a parked ask.
const stopAwaitingInput = "awaiting_input"

type runResult struct {
	parked bool
	err    string
}

// probeRunResult reads the client's control keys on a run tool's result:
// _error (the client could not ask: a tool error) and _parked (no answer
// within the live wait: end the turn now; the answer comes on a later turn).
func probeRunResult(out json.RawMessage) runResult {
	var p struct {
		Parked bool   `json:"_parked"`
		Error  string `json:"_error"`
	}
	_ = json.Unmarshal(out, &p)
	return runResult{parked: p.Parked, err: p.Error}
}

// parkedNote is the assistant line saved with the parked turn, so the resume
// turn's model sees the original request and what it asked.
func parkedNote(t runClientTool, input json.RawMessage) node.Message {
	var in struct {
		Title string `json:"title"`
	}
	_ = json.Unmarshal(input, &in)
	text := "I asked the user (" + t.spec.Name + ") and am waiting for their answer."
	if title := strings.TrimSpace(in.Title); title != "" {
		text = "I asked the user: " + title + ". Waiting for their answer."
	}
	return node.Message{Role: node.AssistantMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: text}}}
}
