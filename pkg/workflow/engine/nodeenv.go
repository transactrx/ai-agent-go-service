package engine

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/retry"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/render"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// nodeEnv is the per-node implementation of node.NodeEnv.
type nodeEnv struct {
	logger     *log.Logger
	nodeID     string
	workflowID string
	hosts      map[string]any
	renderer   *render.Renderer
	lookupEnv  func(string) (string, bool)

	policy       *retry.Policy

	// peerResolver is set by the loader after the connection map is built.
	peerMu       sync.RWMutex
	peerResolver func(portName string) ([]node.Node, error)
}

// nodeEnvConfig wires the static dependencies a NodeEnv needs.
type nodeEnvConfig struct {
	logger     *log.Logger
	nodeID     string
	workflowID string
	hosts      map[string]any
	renderer   *render.Renderer
	lookupEnv  func(string) (string, bool)
	policy     *retry.Policy
}

func newNodeEnv(c nodeEnvConfig) *nodeEnv {
	if c.lookupEnv == nil {
		c.lookupEnv = os.LookupEnv
	}
	return &nodeEnv{
		logger:     c.logger,
		nodeID:     c.nodeID,
		workflowID: c.workflowID,
		hosts:      c.hosts,
		renderer:   c.renderer,
		lookupEnv:  c.lookupEnv,
		policy:     c.policy,
	}
}

func (e *nodeEnv) Logger() *log.Logger { return e.logger }
func (e *nodeEnv) NodeID() string      { return e.nodeID }
func (e *nodeEnv) WorkflowID() string  { return e.workflowID }

// RetryPolicy returns the per-node retry policy, or nil if unset.
// Returns nil through the interface when the underlying *retry.Policy is nil
// so callers can use plain nil-check semantics (a typed-nil bug would
// otherwise force them to unwrap).
func (e *nodeEnv) RetryPolicy() node.RetryPolicy {
	if e.policy == nil {
		return nil
	}
	return e.policy
}

// Secret resolves an env-var NAME (from a *Env field) into a redacting wrapper.
func (e *nodeEnv) Secret(envVarName string) (secret.String, error) {
	if envVarName == "" {
		return secret.String{}, fmt.Errorf("secret: empty env var name in node %s", e.nodeID)
	}
	val, ok := e.lookupEnv(envVarName)
	if !ok || val == "" {
		return secret.String{}, fmt.Errorf("secret: env var %q is not set or empty (referenced by node %s)", envVarName, e.nodeID)
	}
	return secret.New(val), nil
}

// Render expands placeholders via the engine's renderer.
func (e *nodeEnv) Render(template string, ctx node.RenderCtx) (string, error) {
	return e.renderer.Render(template, ctx)
}

// Host returns a transport handle by kind.
func (e *nodeEnv) Host(kind string) (any, bool) {
	h, ok := e.hosts[kind]
	return h, ok
}

// Peer returns the connected nodes on a typed input port.
func (e *nodeEnv) Peer(portName string) ([]node.Node, error) {
	e.peerMu.RLock()
	r := e.peerResolver
	e.peerMu.RUnlock()
	if r == nil {
		return nil, fmt.Errorf("peer resolver not yet wired for node %s", e.nodeID)
	}
	return r(portName)
}

// setPeerResolver is called by the loader once the connection map is built.
func (e *nodeEnv) setPeerResolver(r func(string) ([]node.Node, error)) {
	e.peerMu.Lock()
	e.peerResolver = r
	e.peerMu.Unlock()
}

// ClientToolResultHost is the narrow capability AwaitClientToolResult depends
// on. The transport layer (natsstream) implements this on a per-stream
// basis and the trigger installs it on the context.
type ClientToolResultHost interface {
	WaitForToolResult(ctx context.Context, toolCallID string, timeout time.Duration) ([]byte, error)
}

type toolResultHostKey struct{}

// WithToolResultHost returns a context carrying h. The trigger calls this
// before engine.Process so the host is available to every node's NodeEnv
// for the duration of the request.
func WithToolResultHost(ctx context.Context, h ClientToolResultHost) context.Context {
	return context.WithValue(ctx, toolResultHostKey{}, h)
}

// HasToolResultHost reports whether ctx carries a ClientToolResultHost.
// Used by tests and integration smoke tests.
func HasToolResultHost(ctx context.Context) bool {
	_, ok := ctx.Value(toolResultHostKey{}).(ClientToolResultHost)
	return ok
}

// AwaitClientToolResult reads the per-request host from ctx and delegates.
// Returns an error if no host is in ctx (e.g. workflow running outside a
// streaming session — clientOnly tools require streaming).
func (e *nodeEnv) AwaitClientToolResult(ctx context.Context, toolCallID string, timeout time.Duration) ([]byte, error) {
	h, ok := ctx.Value(toolResultHostKey{}).(ClientToolResultHost)
	if !ok {
		return nil, fmt.Errorf("nodeenv: no ClientToolResultHost in context (node %s)", e.nodeID)
	}
	return h.WaitForToolResult(ctx, toolCallID, timeout)
}

// Compile-time check: nodeEnv satisfies the NodeEnv interface.
var _ node.NodeEnv = (*nodeEnv)(nil)
