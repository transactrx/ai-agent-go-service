// Package clientui implements client-side UI tools (tool/ui-confirm,
// tool/ui-pick-one, tool/ui-human-input). These tools are never invoked
// server-side; the agent loop short-circuits on ClientOnly()==true and
// delegates to the client via AwaitClientToolResult.
package clientui

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Config is the common shape every clientui tool accepts.
type Config struct {
	ToolName        string `json:"toolName"`
	ToolDescription string `json:"toolDescription"`
}

func parseConfig(raw json.RawMessage) (Config, error) {
	var cfg Config
	if len(raw) == 0 {
		return cfg, fmt.Errorf("clientui: empty config")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("clientui: parse config: %w", err)
	}
	if cfg.ToolName == "" {
		return cfg, fmt.Errorf("clientui: toolName is required")
	}
	if cfg.ToolDescription == "" {
		return cfg, fmt.Errorf("clientui: toolDescription is required")
	}
	return cfg, nil
}

// base provides the Node/Tool/ClientUITool boilerplate for all clientui tools.
// Each concrete tool embeds *base and supplies its own ToolSpec at construction.
type base struct {
	node.BaseTool
	nodeType string
	cfg      Config
	spec     node.ToolSpec
}

func (b *base) Spec() node.NodeSpec {
	return node.NodeSpec{Type: b.nodeType, Role: node.RoleTool}
}
func (b *base) Init(_ context.Context, env node.NodeEnv) error {
	b.InitRetry(env)
	return nil
}
func (b *base) Close(_ context.Context) error                { return nil }
func (b *base) FailurePolicy() node.FailurePolicy            { return node.FailureSurfaceToLLM }
func (b *base) ToolSpec() node.ToolSpec                      { return b.spec }
func (b *base) ClientOnly() bool                             { return true }

// Invoke is a poison method — the agent loop must have short-circuited through
// AwaitClientToolResult before reaching here. If it does get called, that's a
// bug in the loop; surface a clear diagnostic.
func (b *base) Invoke(_ context.Context, _ json.RawMessage) (json.RawMessage, error) {
	return nil, fmt.Errorf("%s: Invoke called on a client-only tool — agent loop should have short-circuited via AwaitClientToolResult", b.cfg.ToolName)
}
