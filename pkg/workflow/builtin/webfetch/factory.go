package webfetch

import (
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// NodeType is the workflow-JSON "type" key for this node.
const NodeType = "tool/web-fetch"

// Factory builds a tool/web-fetch node from its config block.
var Factory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("tool/web-fetch config: %w", err)
		}
	}
	if cfg.ToolDescription == "" {
		return nil, fmt.Errorf("tool/web-fetch: toolDescription is required")
	}
	return &Tool{cfg: cfg}, nil
})
