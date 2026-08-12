package promptrewrite

import (
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Factory builds an admin/prompt-rewrite node from its config block.
var Factory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("admin/prompt-rewrite config: %w", err)
		}
	}
	return &Node{cfg: cfg}, nil
})
