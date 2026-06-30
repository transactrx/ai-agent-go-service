package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// AskNumberFactory builds a tool/ui-ask-number node. Args:
// {prompt, mode:"slider"|"input", min?, max?, step?}.
// Result: {value:number}.
var AskNumberFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-ask-number",
		cfg:      cfg,
		spec: node.ToolSpec{
			Name:        cfg.ToolName,
			Description: cfg.ToolDescription,
			InputSchema: json.RawMessage(`{
  "type":"object",
  "required":["prompt","mode"],
  "properties":{
    "prompt":{"type":"string","description":"Short question shown above the control."},
    "mode":{"type":"string","enum":["slider","input"],"description":"slider needs min and max; input is a free numeric field."},
    "min":{"type":"number","description":"Lower bound. Required for mode=slider."},
    "max":{"type":"number","description":"Upper bound. Required for mode=slider."},
    "step":{"type":"number","description":"Increment for slider/input. Default 1."}
  }
}`),
		},
	}, nil
})
