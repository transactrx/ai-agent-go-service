package clientui_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestAskDateFactoryReturnsClientOnlyTool(t *testing.T) {
	n, err := clientui.AskDateFactory.New(json.RawMessage(`{"toolName":"AskDate","toolDescription":"pick date"}`))
	if err != nil {
		t.Fatalf("AskDateFactory: %v", err)
	}
	tool := n.(node.Tool)
	if cu, ok := tool.(node.ClientUITool); !ok || !cu.ClientOnly() {
		t.Fatalf("AskDateFactory must return ClientUITool")
	}
	if _, err := tool.Invoke(context.Background(), nil); err == nil {
		t.Fatalf("Invoke must error on a client-only tool")
	}
}

func TestAskDateInputSchemaDeclaresModeAndBounds(t *testing.T) {
	n, err := clientui.AskDateFactory.New(json.RawMessage(`{"toolName":"AskDate","toolDescription":"d"}`))
	if err != nil {
		t.Fatalf("AskDateFactory: %v", err)
	}
	schema := string(n.(node.Tool).ToolSpec().InputSchema)
	for _, want := range []string{`"prompt"`, `"mode"`, `"single"`, `"range"`, `"min"`, `"max"`} {
		if !strings.Contains(schema, want) {
			t.Fatalf("InputSchema missing %s: %s", want, schema)
		}
	}
}

func TestAskDateFactoryEmptyToolNameFails(t *testing.T) {
	_, err := clientui.AskDateFactory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err == nil {
		t.Fatalf("expected error when toolName missing")
	}
}
