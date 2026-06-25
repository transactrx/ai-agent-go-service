package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestPickManyFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.PickManyFactory.New(json.RawMessage(`{"toolName":"PickMany","toolDescription":"choose many"}`))
	if err != nil {
		t.Fatalf("PickManyFactory: %v", err)
	}
	tool, ok := n.(node.Tool)
	if !ok {
		t.Fatalf("expected node.Tool, got %T", n)
	}
	if cu, ok := tool.(node.ClientUITool); !ok || !cu.ClientOnly() {
		t.Fatalf("PickManyFactory must return ClientUITool")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke on a client-only tool must error if called")
	}
}

func TestPickManyInputSchemaDeclaresOptionsAndPicksBounds(t *testing.T) {
	n, err := clientui.PickManyFactory.New(json.RawMessage(`{"toolName":"PickMany","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("PickManyFactory: %v", err)
	}
	schema := string(n.(node.Tool).ToolSpec().InputSchema)
	for _, want := range []string{`"prompt"`, `"options"`, `"minPicks"`, `"maxPicks"`, `"minItems":2`} {
		if !strings.Contains(schema, want) {
			t.Fatalf("InputSchema missing %s: %s", want, schema)
		}
	}
}

func TestPickManyFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.PickManyFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
