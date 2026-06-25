package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// ConfirmFactory builds a tool/ui-confirm node. The LLM calls this tool with
// a single prompt argument; the client responds with {approved: bool}.
var ConfirmFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-confirm",
		cfg:      cfg,
		spec: node.ToolSpec{
			Name:        cfg.ToolName,
			Description: cfg.ToolDescription,
			InputSchema: json.RawMessage(`{
  "type":"object",
  "required":["prompt"],
  "properties":{
    "prompt":{"type":"string","description":"Short question the user must approve or reject."}
  }
}`),
		},
	}, nil
})
