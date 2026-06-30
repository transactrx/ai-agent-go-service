package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// PickManyFactory builds a tool/ui-pick-many node. Args:
// {prompt, options:[{value,label}], minPicks?, maxPicks?}.
// Result (shipped by the client widget): {values:[string]}.
var PickManyFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-pick-many",
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
    },
    "minPicks":{"type":"integer","minimum":0,"description":"Minimum selections required. Default 1."},
    "maxPicks":{"type":"integer","minimum":1,"description":"Maximum selections allowed. Default = options.length."}
  }
}`),
		},
	}, nil
})
