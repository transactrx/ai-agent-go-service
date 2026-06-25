package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// AskFormFactory builds a tool/ui-ask-form node. Args:
// {prompt, fields:[{name,label,type,required?,options?,min?,max?,placeholder?}]}.
// Allowed types: text, select, date, number, checkbox.
// Result: an object keyed by field.name carrying the user-entered value.
var AskFormFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-ask-form",
		cfg:      cfg,
		spec: node.ToolSpec{
			Name:        cfg.ToolName,
			Description: cfg.ToolDescription,
			InputSchema: json.RawMessage(`{
  "type":"object",
  "required":["prompt","fields"],
  "properties":{
    "prompt":{"type":"string","description":"Short question shown above the form."},
    "fields":{
      "type":"array",
      "minItems":1,
      "items":{
        "type":"object",
        "required":["name","label","type"],
        "properties":{
          "name":{"type":"string","description":"Result-key for this field; unique within the form."},
          "label":{"type":"string","description":"Human-visible field label shown in the Webix form."},
          "type":{"type":"string","enum":["text","select","date","number","checkbox"]},
          "required":{"type":"boolean"},
          "options":{"type":"array","items":{"type":"object","required":["value","label"],"properties":{"value":{"type":"string"},"label":{"type":"string"}}},"description":"Only used when type=select."},
          "min":{"type":"number","description":"Only used when type=number."},
          "max":{"type":"number","description":"Only used when type=number."},
          "placeholder":{"type":"string","description":"Hint text shown inside the input when empty; applies to text and number field types."}
        }
      }
    }
  }
}`),
		},
	}, nil
})
