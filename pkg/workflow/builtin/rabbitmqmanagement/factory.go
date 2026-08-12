package rabbitmqmanagement

import (
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// NodeType is the workflow-JSON "type" key for this node.
const NodeType = "tool/rabbitmq-management"

// Factory builds a tool/rabbitmq-management node from its config block.
// Credential fields (BaseURL/Username/Password) are NOT read from JSON here;
// they are resolved from env vars in Tool.Init.
var Factory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("tool/rabbitmq-management config: %w", err)
		}
	}
	if cfg.ToolDescription == "" {
		return nil, fmt.Errorf("tool/rabbitmq-management: toolDescription is required")
	}
	return &Tool{cfg: cfg}, nil
})
