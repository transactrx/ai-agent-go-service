package inferencegateway

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
)

// fakeEnv is the minimal node.NodeEnv for Init: only Host, Logger, NodeID
// and WorkflowID matter here.
type fakeEnv struct{ hosts map[string]any }

func (f fakeEnv) Logger() *log.Logger                               { return log.New(io.Discard, "", 0) }
func (f fakeEnv) NodeID() string                                    { return "n1" }
func (f fakeEnv) WorkflowID() string                                { return "wf1" }
func (f fakeEnv) Secret(string) (secret.String, error)              { return secret.String{}, nil }
func (f fakeEnv) Render(s string, _ node.RenderCtx) (string, error) { return s, nil }
func (f fakeEnv) Host(kind string) (any, bool)                      { v, ok := f.hosts[kind]; return v, ok }
func (f fakeEnv) Peer(string) ([]node.Node, error)                  { return nil, nil }
func (f fakeEnv) AwaitClientToolResult(context.Context, string, time.Duration) ([]byte, error) {
	return nil, nil
}
func (f fakeEnv) RetryPolicy() node.RetryPolicy { return nil }

func TestInitRequiresNatsHost(t *testing.T) {
	g := newLLM(Config{Alias: "A"})
	err := g.Init(context.Background(), fakeEnv{hosts: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "nats host") {
		t.Fatalf("missing host: err = %v", err)
	}
	err = g.Init(context.Background(), fakeEnv{hosts: map[string]any{"nats": "wrong type"}})
	if err == nil || !strings.Contains(err.Error(), "nats host") {
		t.Fatalf("wrong type: err = %v", err)
	}
}

func TestInitWiresConnectionAndSubject(t *testing.T) {
	srv, _ := runEmbeddedNATS(t)
	ns, err := nats_service.NewLowLevel("trx.test.agent", "q", srv.ClientURL(), "", "", 2048, 300*1024)
	if err != nil {
		t.Fatalf("NewLowLevel: %v", err)
	}
	// Not closing ns: nats-service's ClosedHandler exits the process on any
	// connection close (same caveat as bedrock's TestNatsConn).
	t.Setenv(basePathEnv, "trx.gw")
	g := newLLM(Config{Alias: "A"})
	if err := g.Init(context.Background(), fakeEnv{hosts: map[string]any{"nats": ns}}); err != nil {
		t.Fatal(err)
	}
	if g.nc == nil || g.subject != "trx.gw.invokeStream" || g.wfID != "wf1" || g.nodeID != "n1" || g.logger == nil {
		t.Fatalf("init state = %+v", g)
	}
	g2 := newLLM(Config{Alias: "A", BasePath: "trx.override"})
	_ = g2.Init(context.Background(), fakeEnv{hosts: map[string]any{"nats": ns}})
	if g2.subject != "trx.override.invokeStream" {
		t.Fatalf("basePath override: %q", g2.subject)
	}
}
