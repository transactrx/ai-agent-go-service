package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// AskDateFactory builds a tool/ui-ask-date node. Args:
// {prompt, mode:"single"|"range", min?, max?} (ISO YYYY-MM-DD bounds).
// Result: {date:"YYYY-MM-DD"} for single, {from,to} for range.
var AskDateFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-ask-date",
		cfg:      cfg,
		spec: node.ToolSpec{
			Name:        cfg.ToolName,
			Description: cfg.ToolDescription,
			InputSchema: json.RawMessage(`{
  "type":"object",
  "required":["prompt","mode"],
  "properties":{
    "prompt":{"type":"string","description":"Short question shown above the calendar."},
    "mode":{"type":"string","enum":["single","range"],"description":"single=one date; range=from/to pair."},
    "min":{"type":"string","description":"Earliest selectable date in YYYY-MM-DD."},
    "max":{"type":"string","description":"Latest selectable date in YYYY-MM-DD."}
  }
}`),
		},
	}, nil
})
