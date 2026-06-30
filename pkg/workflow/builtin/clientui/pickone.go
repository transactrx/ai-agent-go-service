package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// PickOneFactory builds a tool/ui-pick-one node. Args: {prompt, options:[{value,label}]}.
// Result (shipped by the client widget): {value: string}.
var PickOneFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-pick-one",
		cfg:      cfg,
		spec: node.ToolSpec{
			Name:        cfg.ToolName,
			Description: cfg.ToolDescription,
			InputSchema: json.RawMessage(`{
  "type":"object",
  "required":["prompt","options"],
  "properties":{
    "prompt":{"type":"string","description":"Short question shown above the choices."},
    "options":{
      "type":"array",
      "minItems":2,
      "items":{
        "type":"object",
        "required":["value","label"],
        "properties":{
          "value":{"type":"string"},
          "label":{"type":"string"}
        }
      }
    }
  }
}`),
		},
	}, nil
})
