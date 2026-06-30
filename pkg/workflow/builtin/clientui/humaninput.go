package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// HumanInputFactory builds a tool/ui-human-input node. Args: {prompt, placeholder?}.
// Result (shipped by the client widget): {text: string}.
var HumanInputFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-human-input",
		cfg:      cfg,
		spec: node.ToolSpec{
			Name:        cfg.ToolName,
			Description: cfg.ToolDescription,
			InputSchema: json.RawMessage(`{
  "type":"object",
  "required":["prompt"],
  "properties":{
    "prompt":{"type":"string","description":"Question shown above the input."},
    "placeholder":{"type":"string","description":"Optional placeholder text inside the input."}
  }
}`),
		},
	}, nil
})
