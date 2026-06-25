package agent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	stdlog "log"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// testNodeEnv is a minimal node.NodeEnv stand-in for tests. Render is
// pass-through; other methods return zero values.
type testNodeEnv struct{}

func (testNodeEnv) Logger() *stdlog.Logger                            { return stdlog.New(io.Discard, "", 0) }
func (testNodeEnv) NodeID() string                                    { return "agent1" }
func (testNodeEnv) WorkflowID() string                                { return "wf" }
func (testNodeEnv) Secret(_ string) (secret.String, error)            { return secret.String{}, nil }
func (testNodeEnv) Render(s string, _ node.RenderCtx) (string, error) { return s, nil }
func (testNodeEnv) Host(_ string) (any, bool)                         { return nil, false }
func (testNodeEnv) Peer(_ string) ([]node.Node, error)                { return nil, nil }
func (testNodeEnv) AwaitClientToolResult(_ context.Context, id string, _ time.Duration) ([]byte, error) {
	return nil, fmt.Errorf("testNodeEnv: no ClientToolResultHost (toolCallID=%s)", id)
}
func (testNodeEnv) RetryPolicy() node.RetryPolicy { return nil }

// capturingEnv records logger output so tests can assert chart-trace lines.
type capturingEnv struct {
	testNodeEnv
	buf *bytes.Buffer
}

func (e *capturingEnv) Logger() *stdlog.Logger { return stdlog.New(e.buf, "", 0) }

// Compile-time check.
var _ node.NodeEnv = testNodeEnv{}
var _ node.NodeEnv = &capturingEnv{}
