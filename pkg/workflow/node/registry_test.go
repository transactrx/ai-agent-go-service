package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

type stubNode struct{}

func (stubNode) Spec() node.NodeSpec                            { return node.NodeSpec{Type: "test/stub"} }
func (stubNode) Init(_ context.Context, _ node.NodeEnv) error   { return nil }
func (stubNode) Close(_ context.Context) error                  { return nil }

func stubFactory() node.Factory {
	return node.FactoryFunc(func(_ json.RawMessage) (node.Node, error) { return stubNode{}, nil })
}

func TestRegisterAndGet(t *testing.T) {
	r := node.NewRegistry()
	if err := r.Register("test/stub", stubFactory()); err != nil {
		t.Fatal(err)
	}
	f, ok := r.Get("test/stub")
	if !ok || f == nil {
		t.Fatal("Get failed")
	}
	n, err := f.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if n.Spec().Type != "test/stub" {
		t.Fatalf("type = %q", n.Spec().Type)
	}
}

func TestDuplicateRegisterFails(t *testing.T) {
	r := node.NewRegistry()
	_ = r.Register("test/stub", stubFactory())
	err := r.Register("test/stub", stubFactory())
	if err == nil {
		t.Fatal("expected duplicate register to error")
	}
}

func TestGetMissingReturnsFalse(t *testing.T) {
	r := node.NewRegistry()
	if _, ok := r.Get("nope"); ok {
		t.Fatal("expected missing")
	}
}

func TestTypesEnumerates(t *testing.T) {
	r := node.NewRegistry()
	_ = r.Register("a", stubFactory())
	_ = r.Register("b", stubFactory())
	got := r.Types()
	if len(got) != 2 {
		t.Fatalf("len = %d", len(got))
	}
}

func TestFactoryErrorPropagates(t *testing.T) {
	r := node.NewRegistry()
	want := errors.New("boom")
	_ = r.Register("bad", node.FactoryFunc(func(_ json.RawMessage) (node.Node, error) { return nil, want }))
	f, _ := r.Get("bad")
	_, err := f.New(nil)
	if !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}
