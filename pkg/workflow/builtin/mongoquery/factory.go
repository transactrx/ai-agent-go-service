package mongoquery

import (
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// NodeType is the workflow-JSON "type" key for this node.
const NodeType = "tool/mongo-query"

// Factory builds a tool/mongo-query node from its config block.
var Factory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("tool/mongo-query config: %w", err)
		}
	}
	if cfg.ToolDescription == "" {
		return nil, fmt.Errorf("tool/mongo-query: toolDescription is required")
	}
	return &Tool{cfg: cfg}, nil
})
