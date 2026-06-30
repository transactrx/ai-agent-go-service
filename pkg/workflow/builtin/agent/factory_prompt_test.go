package agent

import (
	"encoding/json"
	"strings"
	"testing"
)

func buildAgent(t *testing.T, cfgJSON string) *agentNode {
	t.Helper()
	n, err := Factory.New(json.RawMessage(cfgJSON))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	return n.(*agentNode)
}

func TestSystemPrompt_MergeOrderFlexThenFixed(t *testing.T) {
	a := buildAgent(t, `{"systemMessageFixed":"FIXED RULES","systemMessageFlexible":"FLEX KNOWLEDGE"}`)
	got := a.systemPrompt()
	if got != "FLEX KNOWLEDGE\n\nFIXED RULES" {
		t.Fatalf("merged = %q", got)
	}
}

func TestSystemPrompt_BackwardCompatNoFlex(t *testing.T) {
	a := buildAgent(t, `{"systemMessageFixed":"ONLY FIXED"}`)
	if got := a.systemPrompt(); got != "ONLY FIXED" {
		t.Fatalf("merged = %q, want fixed only", got)
	}
}

func TestSpec_DeclaresOverridableAndTemplates(t *testing.T) {
	a := buildAgent(t, `{"systemMessageFixed":"x"}`)
	spec := a.Spec()
	if len(spec.OverridableFields) != 1 || spec.OverridableFields[0] != "systemMessageFlexible" {
		t.Fatalf("OverridableFields = %v", spec.OverridableFields)
	}
	joined := strings.Join(spec.AllowedTemplates, ",")
	if !strings.Contains(joined, "systemMessageFixed") || !strings.Contains(joined, "systemMessageFlexible") {
		t.Fatalf("AllowedTemplates = %v", spec.AllowedTemplates)
	}
}

func TestReconfigure_SwapsFlexForNextPrompt(t *testing.T) {
	a := buildAgent(t, `{"systemMessageFixed":"FIXED","systemMessageFlexible":"OLD"}`)
	if err := a.Reconfigure("systemMessageFlexible", "NEW"); err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	if got := a.systemPrompt(); got != "NEW\n\nFIXED" {
		t.Fatalf("merged = %q", got)
	}
}

func TestReconfigure_RejectsUnknownField(t *testing.T) {
	a := buildAgent(t, `{"systemMessage":"FIXED"}`)
	if err := a.Reconfigure("systemMessageFixed", "hack"); err == nil {
		t.Fatal("expected error reconfiguring a locked field")
	}
}

func TestSystemPrompt_LegacySystemMessageAlias(t *testing.T) {
	// Old workflow JSONs use `systemMessage`; it must behave as the fixed part.
	a := buildAgent(t, `{"systemMessage":"LEGACY FIXED","systemMessageFlexible":"FLEX"}`)
	if got := a.systemPrompt(); got != "FLEX\n\nLEGACY FIXED" {
		t.Fatalf("merged = %q", got)
	}
}
