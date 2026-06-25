package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestPickRowFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.PickRowFactory.New(json.RawMessage(`{"toolName":"PickRow","toolDescription":"pick row"}`))
	if err != nil {
		t.Fatalf("PickRowFactory: %v", err)
	}
	tool := n.(node.Tool)
	if cu, ok := tool.(node.ClientUITool); !ok || !cu.ClientOnly() {
		t.Fatalf("PickRowFactory must return ClientUITool")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke must error on a client-only tool")
	}
}

func TestPickRowInputSchemaHasColumnsAndRows(t *testing.T) {
	n, err := clientui.PickRowFactory.New(json.RawMessage(`{"toolName":"PickRow","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("PickRowFactory: %v", err)
	}
	schema := string(n.(node.Tool).ToolSpec().InputSchema)
	for _, want := range []string{`"prompt"`, `"columns"`, `"rows"`, `"minItems":1`, `"maxItems":20`} {
		if !strings.Contains(schema, want) {
			t.Fatalf("InputSchema missing %s: %s", want, schema)
		}
	}
}

func TestPickRowFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.PickRowFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
