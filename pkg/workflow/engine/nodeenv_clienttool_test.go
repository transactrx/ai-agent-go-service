package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeToolResultHost implements the transport-host capability the NodeEnv
// delegates to. Lives in the engine test because the real host is NATS.
type fakeToolResultHost struct {
	calls   []string
	respond map[string][]byte
	timeout bool
}

func (f *fakeToolResultHost) WaitForToolResult(ctx context.Context, toolCallID string, _ time.Duration) ([]byte, error) {
	f.calls = append(f.calls, toolCallID)
	if f.timeout {
		return nil, errors.New("timeout")
	}
	if b, ok := f.respond[toolCallID]; ok {
		return b, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestNodeEnvAwaitClientToolResultReadsFromContext(t *testing.T) {
	host := &fakeToolResultHost{respond: map[string][]byte{"tc1": []byte(`{"approved":true}`)}}
	env := newNodeEnv(nodeEnvConfig{nodeID: "n", workflowID: "wf"})
	ctx := WithToolResultHost(context.Background(), host)

	out, err := env.AwaitClientToolResult(ctx, "tc1", time.Second)
	if err != nil {
		t.Fatalf("AwaitClientToolResult: %v", err)
	}
	if string(out) != `{"approved":true}` {
		t.Fatalf("payload mismatch: %s", out)
	}
	if len(host.calls) != 1 || host.calls[0] != "tc1" {
		t.Fatalf("expected host called with tc1, got %v", host.calls)
	}
}

func TestNodeEnvAwaitClientToolResultMissingHostFails(t *testing.T) {
	env := newNodeEnv(nodeEnvConfig{nodeID: "n", workflowID: "wf"})
	_, err := env.AwaitClientToolResult(context.Background(), "tc1", time.Second)
	if err == nil {
		t.Fatalf("expected error when no host in context")
	}
}

// Compile-time check: nodeEnv satisfies the interface.
var _ node.NodeEnv = (*nodeEnv)(nil)
