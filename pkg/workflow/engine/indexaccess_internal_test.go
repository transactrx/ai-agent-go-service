package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

type iaTool struct{ pattern string }

func (iaTool) Spec() node.NodeSpec                      { return node.NodeSpec{Type: "test/ia-tool", Role: node.RoleTool} }
func (iaTool) Init(context.Context, node.NodeEnv) error { return nil }
func (iaTool) Close(context.Context) error              { return nil }
func (t iaTool) AllowedIndexPattern() string            { return t.pattern }

// iaPolicy is a policy node that opted in to index functions.
type iaPolicy struct {
	lookup func(ctx context.Context, pattern string) (string, error)
}

func (iaPolicy) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/ia-policy", Role: node.RolePolicy}
}
func (iaPolicy) Init(context.Context, node.NodeEnv) error { return nil }
func (iaPolicy) Close(context.Context) error              { return nil }
func (iaPolicy) Resolve(context.Context, node.PolicyRequest) (node.PolicyResult, error) {
	return node.PolicyResult{}, nil
}
func (p iaPolicy) FunctionForIndex(ctx context.Context, pattern string) (string, error) {
	return p.lookup(ctx, pattern)
}

// iaPlainPolicy is a policy node that did NOT opt in.
type iaPlainPolicy struct{}

func (iaPlainPolicy) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/ia-plain-policy", Role: node.RolePolicy}
}
func (iaPlainPolicy) Init(context.Context, node.NodeEnv) error { return nil }
func (iaPlainPolicy) Close(context.Context) error              { return nil }
func (iaPlainPolicy) Resolve(context.Context, node.PolicyRequest) (node.PolicyResult, error) {
	return node.PolicyResult{}, nil
}

func fixed(fn string) iaPolicy {
	return iaPolicy{lookup: func(context.Context, string) (string, error) { return fn, nil }}
}

func peer(tool, policy string) map[ConnKey][]ConnRef {
	return map[ConnKey][]ConnRef{
		{DestNode: tool, DestPort: node.PortPolicy}: {{SrcNode: policy, SrcPort: node.PortPolicy}},
	}
}

func TestResolveIndexAccess(t *testing.T) {
	byPattern := iaPolicy{lookup: func(_ context.Context, p string) (string, error) {
		switch p {
		case "a-*":
			return "FN-A", nil
		case "b-*":
			return "FN-B", nil
		}
		return "", errors.New("no row")
	}}
	two := map[ConnKey][]ConnRef{
		{DestNode: "os1", DestPort: node.PortPolicy}: {{SrcNode: "pol", SrcPort: node.PortPolicy}},
		{DestNode: "os2", DestPort: node.PortPolicy}: {{SrcNode: "pol", SrcPort: node.PortPolicy}},
	}
	cases := []struct {
		name      string
		nodes     map[string]node.Node
		conns     map[ConnKey][]ConnRef
		wantKind  node.IndexAccessStatus
		wantIndex string
		wantFn    string
		causeHas  string
	}{
		{"no index-scoped node", map[string]node.Node{"agent": iaPlainPolicy{}}, nil, node.IndexAccessNone, "", "", ""},
		{"bound", map[string]node.Node{"os": iaTool{"dev.cpe-*"}, "pol": fixed("FN-1")}, peer("os", "pol"), node.IndexAccessBound, "dev.cpe-*", "FN-1", ""},
		{"padding around the id is trimmed", map[string]node.Node{"os": iaTool{"dev.cpe-*"}, "pol": fixed("  FN-1 ")}, peer("os", "pol"), node.IndexAccessBound, "dev.cpe-*", "FN-1", ""},
		{"blank function is unbound", map[string]node.Node{"os": iaTool{"dev.cpe-*"}, "pol": fixed("   ")}, peer("os", "pol"), node.IndexAccessUnbound, "dev.cpe-*", "", "empty function id"},
		{"provider error is unbound", map[string]node.Node{"os": iaTool{"x-*"}, "pol": byPattern}, peer("os", "pol"), node.IndexAccessUnbound, "x-*", "", "no row"},
		{"policy peer that did not opt in keeps today's behavior", map[string]node.Node{"os": iaTool{"dev.cpe-*"}, "pol": iaPlainPolicy{}}, peer("os", "pol"), node.IndexAccessNone, "", "", ""},
		{"tool without a policy peer is unbound", map[string]node.Node{"os": iaTool{"dev.cpe-*"}}, nil, node.IndexAccessUnbound, "dev.cpe-*", "", "exactly one policy peer"},
		{"two tools, same function", map[string]node.Node{"os1": iaTool{"a-*"}, "os2": iaTool{"b-*"}, "pol": fixed("FN-1")}, two, node.IndexAccessBound, "a-*,b-*", "FN-1", ""},
		{"two tools, different functions", map[string]node.Node{"os1": iaTool{"a-*"}, "os2": iaTool{"b-*"}, "pol": byPattern}, two, node.IndexAccessUnbound, "a-*,b-*", "", "different functions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveIndexAccess(context.Background(), tc.nodes, tc.conns)
			if got.Kind() != tc.wantKind || got.Index != tc.wantIndex || got.FunctionID != tc.wantFn {
				t.Fatalf("got %+v, want kind=%s index=%q fn=%q", got, tc.wantKind, tc.wantIndex, tc.wantFn)
			}
			if tc.causeHas != "" && !strings.Contains(got.Cause, tc.causeHas) {
				t.Fatalf("cause %q does not contain %q", got.Cause, tc.causeHas)
			}
			if tc.causeHas == "" && got.Cause != "" {
				t.Fatalf("unexpected cause %q", got.Cause)
			}
		})
	}
}

func TestResolveIndexAccessTimesOutAsUnbound(t *testing.T) {
	old := indexAccessTimeout
	indexAccessTimeout = 20 * time.Millisecond
	t.Cleanup(func() { indexAccessTimeout = old })
	blocking := iaPolicy{lookup: func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}}
	got := resolveIndexAccess(context.Background(),
		map[string]node.Node{"os": iaTool{"dev.cpe-*"}, "pol": blocking}, peer("os", "pol"))
	if got.Kind() != node.IndexAccessUnbound || !strings.Contains(got.Cause, "deadline exceeded") {
		t.Fatalf("got %+v, want unbound with a deadline cause", got)
	}
}
