package rsassistant

import (
	"fmt"
	"sort"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/natschat"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
)

// ModeStreaming is the trigger/nats-chat responseMode the bridge requires.
const ModeStreaming = "streaming"

// clientOnlyMarker is the method set node.ClientUITool adds over node.Tool.
// Checking the marker structurally keeps this package independent of the
// full Tool method set while matching the agent loop's own test
// (builtin/agent/loop.go: `tool.(node.ClientUITool); ok && cu.ClientOnly()`).
type clientOnlyMarker interface{ ClientOnly() bool }

// Eligibility decides whether a loaded workflow can be published to RSAssistant.
// It returns the chat endpoint and an empty reason when eligible, or a
// human-readable reason otherwise. Rules (spec §3.2):
//  1. trigger implements natschat.ChatEndpoint
//  2. responseMode is streaming, or the body may override it to streaming
//  3. no node is a client-only UI tool (RSAssistant cannot answer them)
func Eligibility(wf *engine.Workflow) (natschat.ChatEndpoint, string) {
	if wf == nil || wf.Trigger == nil {
		return nil, "workflow has no trigger"
	}
	ep, ok := wf.Trigger.(natschat.ChatEndpoint)
	if !ok {
		return nil, "trigger is not trigger/nats-chat"
	}
	if ep.ResponseMode() != ModeStreaming && !ep.AllowsResponseModeOverride() {
		return nil, fmt.Sprintf("cannot stream: responseMode=%q and allowResponseModeOverride=false", ep.ResponseMode())
	}
	ids := make([]string, 0, len(wf.Nodes))
	for id := range wf.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic reason
	for _, id := range ids {
		if cu, ok := wf.Nodes[id].(clientOnlyMarker); ok && cu.ClientOnly() {
			return nil, fmt.Sprintf("has client-only UI tool node %q (%s)", id, wf.Nodes[id].Spec().Type)
		}
	}
	return ep, ""
}
