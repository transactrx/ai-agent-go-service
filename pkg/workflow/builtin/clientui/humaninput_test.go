package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestHumanInputFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.HumanInputFactory.New(json.RawMessage(`{"toolName":"AskUser","toolDescription":"ask"}`))
	if err != nil {
		t.Fatalf("HumanInputFactory: %v", err)
	}
	tool, ok := n.(node.Tool)
	if !ok {
		t.Fatalf("expected node.Tool, got %T", n)
	}
	if cu, ok := tool.(node.ClientUITool); !ok || !cu.ClientOnly() {
		t.Fatalf("HumanInputFactory must return ClientUITool")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke on a client-only tool must error if called")
	}
}

func TestHumanInputSchemaDeclaresPromptAndPlaceholder(t *testing.T) {
	n, err := clientui.HumanInputFactory.New(json.RawMessage(`{"toolName":"AskUser","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("HumanInputFactory: %v", err)
	}
	schema := string(n.(node.Tool).ToolSpec().InputSchema)
	if !strings.Contains(schema, `"prompt"`) {
		t.Fatalf("InputSchema must declare prompt: %s", schema)
	}
	if !strings.Contains(schema, `"placeholder"`) {
		t.Fatalf("InputSchema must declare placeholder: %s", schema)
	}
}

func TestHumanInputFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.HumanInputFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
