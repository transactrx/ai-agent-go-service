package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestAskNumberFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.AskNumberFactory.New(json.RawMessage(`{"toolName":"AskNumber","toolDescription":"pick number"}`))
	if err != nil {
		t.Fatalf("AskNumberFactory: %v", err)
	}
	tool := n.(node.Tool)
	if cu, ok := tool.(node.ClientUITool); !ok || !cu.ClientOnly() {
		t.Fatalf("AskNumberFactory must return ClientUITool")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke must error on a client-only tool")
	}
}

func TestAskNumberInputSchemaHasModeAndBounds(t *testing.T) {
	n, err := clientui.AskNumberFactory.New(json.RawMessage(`{"toolName":"AskNumber","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("AskNumberFactory: %v", err)
	}
	schema := string(n.(node.Tool).ToolSpec().InputSchema)
	for _, want := range []string{`"prompt"`, `"mode"`, `"slider"`, `"input"`, `"min"`, `"max"`, `"step"`} {
		if !strings.Contains(schema, want) {
			t.Fatalf("InputSchema missing %s: %s", want, schema)
		}
	}
}

func TestAskNumberFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.AskNumberFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
