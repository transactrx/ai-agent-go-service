package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestPickOneFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.PickOneFactory.New(json.RawMessage(`{"toolName":"PickOne","toolDescription":"choose"}`))
	if err != nil {
		t.Fatalf("PickOneFactory: %v", err)
	}
	tool, ok := n.(node.Tool)
	if !ok {
		t.Fatalf("expected node.Tool, got %T", n)
	}
	if cu, ok := tool.(node.ClientUITool); !ok || !cu.ClientOnly() {
		t.Fatalf("PickOneFactory must return ClientUITool")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke on a client-only tool must error if called")
	}
}

func TestPickOneInputSchemaHasOptions(t *testing.T) {
	n, err := clientui.PickOneFactory.New(json.RawMessage(`{"toolName":"PickOne","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("PickOneFactory: %v", err)
	}
	schema := string(n.(node.Tool).ToolSpec().InputSchema)
	if !strings.Contains(schema, `"options"`) {
		t.Fatalf("InputSchema must declare options: %s", schema)
	}
	if !strings.Contains(schema, `"prompt"`) {
		t.Fatalf("InputSchema must declare prompt: %s", schema)
	}
	if !strings.Contains(schema, `"minItems":2`) {
		t.Fatalf("InputSchema must require at least 2 options: %s", schema)
	}
}

func TestPickOneFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.PickOneFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
