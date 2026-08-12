package postgresquery

import (
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// NodeType is the workflow-JSON "type" key for this node.
const NodeType = "tool/postgres-query"

// Factory builds a tool/postgres-query node from its config block.
var Factory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("tool/postgres-query config: %w", err)
		}
	}
	if cfg.ToolDescription == "" {
		return nil, fmt.Errorf("tool/postgres-query: toolDescription is required")
	}
	return &Tool{cfg: cfg}, nil
})
