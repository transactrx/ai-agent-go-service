package loader

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/render"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeOverridableNode declares one overridable field and remembers its config.
type fakeOverridableNode struct{ cfg map[string]string }

func (f *fakeOverridableNode) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/overridable", Role: node.RoleTool,
		OverridableFields: []string{"systemMessageFlexible"}}
}
func (f *fakeOverridableNode) Init(context.Context, node.NodeEnv) error { return nil }
func (f *fakeOverridableNode) Close(context.Context) error             { return nil }

type fakeStore struct {
	content string
	found   bool
	err     error
}

func (s *fakeStore) Latest(context.Context, string, string, string) (string, bool, error) {
	return s.content, s.found, s.err
}

func testLoaderWith(store PromptOverrides) *Loader {
	reg := node.NewRegistry()
	_ = reg.Register("test/overridable", node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
		var cfg map[string]string
		_ = json.Unmarshal(raw, &cfg)
		return &fakeOverridableNode{cfg: cfg}, nil
	}))
	return NewLoader(reg, nil, render.NewRenderer(), log.New(os.Stdout, "", 0),
		func(k string) (string, bool) {
			if k == "TEST_VAR" {
				return "resolved", true
			}
			return "", false
		}, nil, store)
}

func apply(t *testing.T, l *Loader, store PromptOverrides) *fakeOverridableNode {
	t.Helper()
	raw := rawNode{ID: "n1", Type: "test/overridable", Config: json.RawMessage(`{"systemMessageFlexible":"default text"}`)}
	f, _ := l.registry.Get(raw.Type)
	inst, err := f.New(raw.Config)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	out := l.applyOverrides(context.Background(), "wf1", raw, inst)
	return out.(*fakeOverridableNode)
}

func TestApplyOverrides_StoreHit(t *testing.T) {
	store := &fakeStore{content: "from db ${TEST_VAR}", found: true}
	got := apply(t, testLoaderWith(store), store)
	if got.cfg["systemMessageFlexible"] != "from db resolved" {
		t.Fatalf("got %q, want override with env resolved", got.cfg["systemMessageFlexible"])
	}
}

func TestApplyOverrides_NotFoundKeepsDefault(t *testing.T) {
	store := &fakeStore{found: false}
	got := apply(t, testLoaderWith(store), store)
	if got.cfg["systemMessageFlexible"] != "default text" {
		t.Fatalf("got %q, want default", got.cfg["systemMessageFlexible"])
	}
}

func TestApplyOverrides_StoreErrorKeepsDefault(t *testing.T) {
	store := &fakeStore{err: errors.New("dynamo down")}
	got := apply(t, testLoaderWith(store), store)
	if got.cfg["systemMessageFlexible"] != "default text" {
		t.Fatalf("got %q, want default on store error", got.cfg["systemMessageFlexible"])
	}
}

func TestApplyOverrides_BadTemplateKeepsDefault(t *testing.T) {
	store := &fakeStore{content: "bad {{noSuchPlaceholder}}", found: true}
	got := apply(t, testLoaderWith(store), store)
	if got.cfg["systemMessageFlexible"] != "default text" {
		t.Fatalf("got %q, want default on bad template", got.cfg["systemMessageFlexible"])
	}
}

func TestApplyOverrides_NilStoreNoop(t *testing.T) {
	got := apply(t, testLoaderWith(nil), nil)
	if got.cfg["systemMessageFlexible"] != "default text" {
		t.Fatalf("got %q, want default with nil store", got.cfg["systemMessageFlexible"])
	}
}
