// Package agent implements ai/agent — orchestrates the LLM↔tool loop and
// emits stream events to the trigger's sink.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Config validated by Factory.
//
// The system prompt is split in two parts:
//   - systemMessageFixed: the LOCKED backbone (security/privacy/tool
//     contracts). Never admin-editable.
//   - systemMessageFlexible: the admin-tunable domain knowledge. Its JSON
//     value is the DEFAULT; a prompt-store override (DB) wins when present.
//
// Legacy `systemMessage` is accepted as an alias for systemMessageFixed so
// old workflow JSONs keep loading.
type Config struct {
	MaxIterations               int    `json:"maxIterations,omitempty"`
	SystemMessageFixed          string `json:"systemMessageFixed,omitempty"`
	SystemMessageFlexible       string `json:"systemMessageFlexible,omitempty"`
	SystemMessage               string `json:"systemMessage,omitempty"` // legacy alias for systemMessageFixed
	SurfaceThoughts             bool   `json:"surfaceThoughts,omitempty"`
	HumanResponseTimeoutSeconds int    `json:"humanResponseTimeoutSeconds,omitempty"`
}

const defaultMaxIterations = 15

// flexibleField is the only admin-overridable agent config field.
const flexibleField = "systemMessageFlexible"

// Factory builds an agent node.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("ai/agent: parse config: %w", err)
	}
	if cfg.SystemMessageFixed == "" {
		cfg.SystemMessageFixed = cfg.SystemMessage // legacy alias
	}
	if cfg.SystemMessageFixed == "" {
		return nil, fmt.Errorf("ai/agent: systemMessageFixed (or legacy systemMessage) is required")
	}
	if cfg.MaxIterations == 0 {
		cfg.MaxIterations = defaultMaxIterations
	}
	if cfg.MaxIterations < 1 || cfg.MaxIterations > 50 {
		return nil, fmt.Errorf("ai/agent: maxIterations must be between 1 and 50")
	}
	a := &agentNode{
		cfg:                   cfg,
		maxEmptyAnswerRetries: resolveMaxEmptyAnswerRetries(),
		maxToolRetries:        resolveMaxToolRetries(),
	}
	a.setFlex(cfg.SystemMessageFlexible)
	return a, nil
})

type agentNode struct {
	cfg        Config
	env        node.NodeEnv
	workflowID string
	llm        node.LLMProvider
	mem        node.Memory
	tools      []node.Tool

	// maxEmptyAnswerRetries is the empty-answer re-prompt budget, resolved from
	// AICHAT_MAX_EMPTY_ANSWER_RETRIES (default 3, hard cap 5). See loop.go.
	maxEmptyAnswerRetries int
	// maxToolRetries is the per-tool error-retry cap per turn, resolved from
	// AICHAT_MAX_TOOL_RETRIES (default 3, hard cap 10). See loop.go.
	maxToolRetries int

	// promptMu guards merged. Process reads via systemPrompt(); Reconfigure
	// (hot prompt update broadcast) writes. In-flight turns keep the prompt
	// they started with.
	promptMu sync.RWMutex
	merged   string
}

// Spec returns the node's metadata.
func (a *agentNode) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "ai/agent",
		Role: node.RoleAgent,
		InputPorts: []node.PortSpec{
			{Name: node.PortMain, Direction: node.PortIn, Cardinality: node.CardOne, Required: true},
			{Name: node.PortAILanguageModel, Direction: node.PortIn, Cardinality: node.CardOne, Required: true, PeerRole: node.RoleLLM},
			{Name: node.PortAIMemory, Direction: node.PortIn, Cardinality: node.CardZeroOrOne, PeerRole: node.RoleMemory},
			{Name: node.PortAITool, Direction: node.PortIn, Cardinality: node.CardZeroOrMany, PeerRole: node.RoleTool},
		},
		OutputPorts: []node.PortSpec{
			{Name: node.PortMain, Direction: node.PortOut, Cardinality: node.CardOne},
		},
		AllowedTemplates:  []string{"systemMessageFixed", "systemMessageFlexible", "systemMessage"},
		OverridableFields: []string{flexibleField},
	}
}

// Init resolves peers via NodeEnv.Peer.
func (a *agentNode) Init(_ context.Context, env node.NodeEnv) error {
	a.env = env
	a.workflowID = env.WorkflowID()

	llmPeers, err := env.Peer(node.PortAILanguageModel)
	if err != nil {
		return err
	}
	if len(llmPeers) != 1 {
		return fmt.Errorf("ai/agent: exactly one ai_languageModel peer required, got %d", len(llmPeers))
	}
	llm, ok := llmPeers[0].(node.LLMProvider)
	if !ok {
		return fmt.Errorf("ai/agent: ai_languageModel peer must implement LLMProvider")
	}
	a.llm = llm

	memPeers, _ := env.Peer(node.PortAIMemory)
	if len(memPeers) == 1 {
		m, ok := memPeers[0].(node.Memory)
		if !ok {
			return fmt.Errorf("ai/agent: ai_memory peer must implement Memory")
		}
		a.mem = m
	}

	toolPeers, _ := env.Peer(node.PortAITool)
	for _, p := range toolPeers {
		tool, ok := p.(node.Tool)
		if !ok {
			return fmt.Errorf("ai/agent: ai_tool peer must implement Tool")
		}
		a.tools = append(a.tools, tool)
	}
	return nil
}

func (a *agentNode) Close(_ context.Context) error { return nil }

// lookupTool finds a connected tool by spec name.
func (a *agentNode) lookupTool(name string) node.Tool {
	for _, t := range a.tools {
		if t.ToolSpec().Name == name {
			return t
		}
	}
	return nil
}

// setFlex recomputes the merged system prompt: flexible knowledge first,
// FIXED backbone LAST (later instructions carry more weight; the fixed part
// opens with a precedence statement).
func (a *agentNode) setFlex(flex string) {
	a.promptMu.Lock()
	defer a.promptMu.Unlock()
	if flex == "" {
		a.merged = a.cfg.SystemMessageFixed
		return
	}
	a.merged = flex + "\n\n" + a.cfg.SystemMessageFixed
}

// systemPrompt returns the current merged prompt.
func (a *agentNode) systemPrompt() string {
	a.promptMu.RLock()
	defer a.promptMu.RUnlock()
	return a.merged
}

// Reconfigure applies a hot update for an overridable field. Only
// systemMessageFlexible is overridable; systemMessageFixed is locked.
func (a *agentNode) Reconfigure(field, value string) error {
	if field != flexibleField {
		return fmt.Errorf("ai/agent: field %q is not overridable", field)
	}
	a.setFlex(value)
	return nil
}
