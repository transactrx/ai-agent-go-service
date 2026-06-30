package agent

import (
	"encoding/json"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

type fakeFactory struct{}

func (fakeFactory) New(_ json.RawMessage) (node.Node, error) { return nil, nil }

func TestBuildRegistry_IncludesDefaults(t *testing.T) {
	s := NewService()
	reg, err := s.buildRegistry()
	if err != nil {
		t.Fatalf("buildRegistry: %v", err)
	}
	if _, ok := reg.Get("ai/agent"); !ok {
		t.Error("expected default ai/agent to be registered")
	}
	if _, ok := reg.Get("policy/powerline-scope"); ok {
		t.Error("powerline-scope must not be present by default")
	}
}

func TestBuildRegistry_AppliesExtraFactories(t *testing.T) {
	s := NewService(WithNode("policy/powerline-scope", fakeFactory{}))
	reg, err := s.buildRegistry()
	if err != nil {
		t.Fatalf("buildRegistry: %v", err)
	}
	if _, ok := reg.Get("policy/powerline-scope"); !ok {
		t.Error("expected WithNode factory to be registered")
	}
}

func TestBuildRegistry_OverridesADefault(t *testing.T) {
	// The library promises "everything overridable": a WithNode whose key
	// collides with a default must REPLACE the default, not error. (Registry.Register
	// errors on duplicate keys — see pkg/workflow/node/registry.go — so buildRegistry
	// must use Registry.Replace for extras.)
	override := fakeFactory{}
	s := NewService(WithNode("ai/bedrock", override))
	reg, err := s.buildRegistry()
	if err != nil {
		t.Fatalf("buildRegistry must not error when overriding a default: %v", err)
	}
	got, ok := reg.Get("ai/bedrock")
	if !ok {
		t.Fatal("ai/bedrock missing after override")
	}
	if _, isOverride := got.(fakeFactory); !isOverride {
		t.Error("expected ai/bedrock to be the override factory, got the default")
	}
}
