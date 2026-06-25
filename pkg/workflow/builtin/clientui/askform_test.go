package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestAskFormFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.AskFormFactory.New(json.RawMessage(`{"toolName":"AskForm","toolDescription":"fill form"}`))
	if err != nil {
		t.Fatalf("AskFormFactory: %v", err)
	}
	tool := n.(node.Tool)
	if cu, ok := tool.(node.ClientUITool); !ok || !cu.ClientOnly() {
		t.Fatalf("AskFormFactory must return ClientUITool")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke must error on a client-only tool")
	}
}

func TestAskFormInputSchemaListsAllowedFieldTypes(t *testing.T) {
	n, err := clientui.AskFormFactory.New(json.RawMessage(`{"toolName":"AskForm","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("AskFormFactory: %v", err)
	}
	schema := string(n.(node.Tool).ToolSpec().InputSchema)
	for _, want := range []string{`"prompt"`, `"fields"`, `"text"`, `"select"`, `"date"`, `"number"`, `"checkbox"`, `"minItems":1`} {
		if !strings.Contains(schema, want) {
			t.Fatalf("InputSchema missing %s: %s", want, schema)
		}
	}
	if strings.Contains(schema, `"radio"`) || strings.Contains(schema, `"file"`) {
		t.Fatalf("InputSchema must NOT allow radio/file types: %s", schema)
	}
}

func TestAskFormFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.AskFormFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
