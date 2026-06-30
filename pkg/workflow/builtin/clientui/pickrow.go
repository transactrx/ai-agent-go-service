package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// PickRowFactory builds a tool/ui-pick-row node. Args:
// {prompt, columns:[{id,label,width?}], rows:[{id,<col-values>}]}.
// Result: {row:{...}} — the full row object the user clicked.
var PickRowFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-pick-row",
		cfg:      cfg,
		spec: node.ToolSpec{
			Name:        cfg.ToolName,
			Description: cfg.ToolDescription,
			InputSchema: json.RawMessage(`{
  "type":"object",
  "required":["prompt","columns","rows"],
  "properties":{
    "prompt":{"type":"string","description":"Short question shown above the table."},
    "columns":{
      "type":"array",
      "minItems":1,
      "items":{
        "type":"object",
        "required":["id","label"],
        "properties":{
          "id":{"type":"string","description":"Column key; must match keys in rows[]."},
          "label":{"type":"string","description":"Human-visible column header."},
          "width":{"type":"integer","minimum":40,"description":"Optional column width in pixels."}
        }
      }
    },
    "rows":{
      "type":"array",
      "minItems":1,
      "maxItems":20,
      "items":{
        "type":"object",
        "required":["id"],
        "description":"One row; must contain an 'id' field plus one entry per column id."
      }
    }
  }
}`),
		},
	}, nil
})
