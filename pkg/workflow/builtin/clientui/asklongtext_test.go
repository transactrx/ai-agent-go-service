package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestAskLongTextFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.AskLongTextFactory.New(json.RawMessage(`{"toolName":"AskLongText","toolDescription":"paragraph"}`))
	if err != nil {
		t.Fatalf("AskLongTextFactory: %v", err)
	}
	tool := n.(node.Tool)
	if cu, ok := tool.(node.ClientUITool); !ok || !cu.ClientOnly() {
		t.Fatalf("AskLongTextFactory must return ClientUITool")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke must error on a client-only tool")
	}
}

func TestAskLongTextInputSchemaHasLengthBounds(t *testing.T) {
	n, err := clientui.AskLongTextFactory.New(json.RawMessage(`{"toolName":"AskLongText","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("AskLongTextFactory: %v", err)
	}
	schema := string(n.(node.Tool).ToolSpec().InputSchema)
	for _, want := range []string{`"prompt"`, `"placeholder"`, `"minLength"`, `"maxLength"`} {
		if !strings.Contains(schema, want) {
			t.Fatalf("InputSchema missing %s: %s", want, schema)
		}
	}
}

func TestAskLongTextFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.AskLongTextFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
