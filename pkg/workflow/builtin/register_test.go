package builtin

import (
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestRegisterDefaults_RegistersGenericTypes(t *testing.T) {
	reg := node.NewRegistry()
	if err := RegisterDefaults(reg); err != nil {
		t.Fatalf("RegisterDefaults: %v", err)
	}
	want := []string{
		"trigger/nats-chat", "ai/agent", "ai/bedrock",
		"memory/postgres", "memory/dynamodb", "tool/opensearch",
		"tool/serpapi", "tool/quickchart",
		"tool/ui-confirm", "tool/ui-pick-one", "tool/ui-human-input",
		"tool/ui-pick-many", "tool/ui-ask-date", "tool/ui-ask-form",
		"tool/ui-pick-row", "tool/ui-ask-number", "tool/ui-ask-long-text",
	}
	have := map[string]bool{}
	for _, k := range reg.Types() {
		have[k] = true
	}
	for _, k := range want {
		if !have[k] {
			t.Errorf("expected default type %q to be registered", k)
		}
	}
}

func TestRegisterDefaults_ExcludesTenantPolicy(t *testing.T) {
	reg := node.NewRegistry()
	if err := RegisterDefaults(reg); err != nil {
		t.Fatalf("RegisterDefaults: %v", err)
	}
	if _, ok := reg.Get("policy/powerline-scope"); ok {
		t.Fatal("policy/powerline-scope must NOT be a library default (tenant-specific)")
	}
}
