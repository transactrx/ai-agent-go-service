package rsassistant

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeChatTrigger implements node.Trigger and natschat.ChatEndpoint.
type fakeChatTrigger struct {
	mode     string
	override bool
}

func (fakeChatTrigger) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "trigger/nats-chat", Role: node.RoleTrigger}
}
func (fakeChatTrigger) Init(context.Context, node.NodeEnv) error          { return nil }
func (fakeChatTrigger) Close(context.Context) error                       { return nil }
func (fakeChatTrigger) Subscribe(context.Context, node.TriggerSink) error { return nil }
func (fakeChatTrigger) ChatSubject() string                               { return "trx.test.wf" }
func (f fakeChatTrigger) ResponseMode() string                            { return f.mode }
func (f fakeChatTrigger) AllowsResponseModeOverride() bool                { return f.override }
func (fakeChatTrigger) RequestTimeout() time.Duration                     { return time.Minute }

// otherTrigger is a node.Trigger that is not a chat endpoint.
type otherTrigger struct{}

func (otherTrigger) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "trigger/other", Role: node.RoleTrigger}
}
func (otherTrigger) Init(context.Context, node.NodeEnv) error          { return nil }
func (otherTrigger) Close(context.Context) error                       { return nil }
func (otherTrigger) Subscribe(context.Context, node.TriggerSink) error { return nil }

// plainNode is any non-UI node.
type plainNode struct{}

func (plainNode) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "tool/opensearch", Role: node.RoleTool}
}
func (plainNode) Init(context.Context, node.NodeEnv) error { return nil }
func (plainNode) Close(context.Context) error              { return nil }

// uiNode carries the ClientOnly marker exactly like node.ClientUITool.
type uiNode struct {
	plainNode
	clientOnly bool
}

func (u uiNode) ClientOnly() bool { return u.clientOnly }

func wfWith(trig node.Trigger, nodes map[string]node.Node) *engine.Workflow {
	if nodes == nil {
		nodes = map[string]node.Node{}
	}
	nodes["trigger1"] = trig
	return &engine.Workflow{ID: "wf", Trigger: trig, Nodes: nodes}
}

func TestEligibilityStreamingWithoutUITools(t *testing.T) {
	ep, reason := Eligibility(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"os": plainNode{}}))
	if reason != "" || ep == nil {
		t.Fatalf("expected eligible, got reason=%q ep=%v", reason, ep)
	}
}

func TestEligibilitySingleWithOverride(t *testing.T) {
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "single", override: true}, nil))
	if reason != "" {
		t.Fatalf("single+override must be eligible, got %q", reason)
	}
}

func TestEligibilitySingleWithoutOverride(t *testing.T) {
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "single"}, nil))
	if !strings.Contains(reason, "cannot stream") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestEligibilityRejectsClientOnlyUITool(t *testing.T) {
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{
		"os":       plainNode{},
		"confirm1": uiNode{clientOnly: true},
	}))
	if !strings.Contains(reason, "confirm1") {
		t.Fatalf("reason must name the UI node, got %q", reason)
	}
}

func TestEligibilityAllowsClientOnlyFalse(t *testing.T) {
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"x": uiNode{clientOnly: false}}))
	if reason != "" {
		t.Fatalf("ClientOnly()==false must not block, got %q", reason)
	}
}

func TestEligibilityRejectsNonChatTrigger(t *testing.T) {
	_, reason := Eligibility(wfWith(otherTrigger{}, nil))
	if !strings.Contains(reason, "trigger/nats-chat") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestEligibilityNilWorkflow(t *testing.T) {
	if _, reason := Eligibility(nil); reason == "" {
		t.Fatal("nil workflow must be ineligible")
	}
}
