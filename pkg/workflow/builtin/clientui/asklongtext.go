package clientui

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// AskLongTextFactory builds a tool/ui-ask-long-text node. Args:
// {prompt, placeholder?, minLength?, maxLength?}.
// Result: {text:string} (multi-line allowed).
var AskLongTextFactory node.Factory = node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
	cfg, err := parseConfig(raw)
	if err != nil {
		return nil, err
	}
	return &base{
		nodeType: "tool/ui-ask-long-text",
		cfg:      cfg,
		spec: node.ToolSpec{
			Name:        cfg.ToolName,
			Description: cfg.ToolDescription,
			InputSchema: json.RawMessage(`{
  "type":"object",
  "required":["prompt"],
  "properties":{
    "prompt":{"type":"string","description":"Question shown above the textarea."},
    "placeholder":{"type":"string","description":"Optional placeholder text inside the textarea."},
    "minLength":{"type":"integer","minimum":0,"description":"Minimum character count for the answer."},
    "maxLength":{"type":"integer","minimum":1,"description":"Maximum character count for the answer. Must be greater than or equal to minLength when both are set."}
  }
}`),
		},
	}, nil
})
