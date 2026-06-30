package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestConfirmFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.ConfirmFactory.New(json.RawMessage(`{"toolName":"ConfirmAction","toolDescription":"Ask user yes/no"}`))
	if err != nil {
		t.Fatalf("ConfirmFactory.New: %v", err)
	}
	tool, ok := n.(node.Tool)
	if !ok {
		t.Fatalf("ConfirmFactory must return node.Tool, got %T", n)
	}
	cu, ok := tool.(node.ClientUITool)
	if !ok || !cu.ClientOnly() {
		t.Fatalf("ConfirmFactory must return a ClientUITool with ClientOnly()==true")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke on a client-only tool must error if called")
	}
}

func TestConfirmToolSpecExposesPromptInput(t *testing.T) {
	n, err := clientui.ConfirmFactory.New(json.RawMessage(`{"toolName":"ConfirmAction","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("ConfirmFactory.New: %v", err)
	}
	spec := n.(node.Tool).ToolSpec()
	if spec.Name != "ConfirmAction" {
		t.Fatalf("ToolSpec.Name = %q, want ConfirmAction", spec.Name)
	}
	if !strings.Contains(string(spec.InputSchema), `"prompt"`) {
		t.Fatalf("InputSchema missing prompt: %s", spec.InputSchema)
	}
}

func TestConfirmFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.ConfirmFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
