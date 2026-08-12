package rabbitmqmanagement

import (
	"context"
	"encoding/json"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestRabbitMQ_Overview(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/overview" {
			http.Error(w, "wrong path", 404)
			return
		}
		_, _ = w.Write([]byte(`{"node":"rabbit@host","queue_totals":{"messages":42}}`))
	}))
	defer srv.Close()

	tool := newToolForTest(Config{Connections: map[string]ConnConfig{
		"trxBatchProcessing": {BaseURL: srv.URL, Username: "u", Password: "p"},
	}})

	args, _ := json.Marshal(map[string]any{"connection": "trxBatchProcessing", "op": "overview"})
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	if resp["node"] != "rabbit@host" {
		t.Errorf("resp = %+v", resp)
	}
}

func TestRabbitMQ_ListQueues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"q1","messages":10},{"name":"q2","messages":0}]`))
	}))
	defer srv.Close()

	tool := newToolForTest(Config{Connections: map[string]ConnConfig{
		"trx": {BaseURL: srv.URL, Username: "u", Password: "p"},
	}})
	args, _ := json.Marshal(map[string]any{"connection": "trx", "op": "list_queues"})
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	var arr []map[string]any
	_ = json.Unmarshal(out, &arr)
	if len(arr) != 2 {
		t.Errorf("got %d queues", len(arr))
	}
}

func TestRabbitMQ_DLQSummary_FiltersByNameSuffix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"orders.dlq","messages":3},{"name":"orders","messages":50},{"name":"audit-dlx","messages":1},{"name":"audit","messages":100}]`))
	}))
	defer srv.Close()

	tool := newToolForTest(Config{Connections: map[string]ConnConfig{
		"trx": {BaseURL: srv.URL, Username: "u", Password: "p"},
	}})
	args, _ := json.Marshal(map[string]any{"connection": "trx", "op": "dlq_summary"})
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	var arr []map[string]any
	_ = json.Unmarshal(out, &arr)
	if len(arr) != 2 {
		t.Errorf("expected 2 DLQs, got %d: %+v", len(arr), arr)
	}
}

func TestRabbitMQ_DisallowedOp_Purge(t *testing.T) {
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"trx": {BaseURL: "http://x", Username: "u", Password: "p"}}})
	args, _ := json.Marshal(map[string]any{"connection": "trx", "op": "purge"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "purge") {
		t.Fatalf("expected purge rejection, got %v", err)
	}
}

func TestRabbitMQ_DisallowedOp_Publish(t *testing.T) {
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"trx": {BaseURL: "http://x"}}})
	args, _ := json.Marshal(map[string]any{"connection": "trx", "op": "publish"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "publish") {
		t.Fatalf("expected publish rejection, got %v", err)
	}
}

func TestRabbitMQ_UnknownConnection(t *testing.T) {
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"trx": {BaseURL: "http://x"}}})
	args, _ := json.Marshal(map[string]any{"connection": "nope", "op": "overview"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("expected unknown-connection, got %v", err)
	}
}

func TestRabbitMQ_DisabledFlag(t *testing.T) {
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"trx": {BaseURL: "http://x"}}, Disabled: true})
	args, _ := json.Marshal(map[string]any{"connection": "trx", "op": "overview"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("expected disabled rejection, got %v", err)
	}
}

func TestRabbitMQ_5xxStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "server fault", 500)
	}))
	defer srv.Close()
	tool := newToolForTest(Config{Connections: map[string]ConnConfig{"trx": {BaseURL: srv.URL}}})
	args, _ := json.Marshal(map[string]any{"connection": "trx", "op": "overview"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected 500 error, got %v", err)
	}
}

// TestInit_ResolvesConnectionFromEnv covers the credential-resolution path
// that replaced the old factory's applyEnvDefaults. Note: unlike the old
// code, Init does NOT fall back to a RABBITMQ_VHOST env var — the workflow
// config now expresses that default via template syntax
// (${RABBITMQ_VHOST:/}) at the render layer, so an empty vhost here always
// defaults to "/".
func TestInit_ResolvesConnectionFromEnv(t *testing.T) {
	t.Setenv("RABBITMQ_MGMT_SCHEME", "http")
	t.Setenv("RABBITMQ_HOST", "mq.internal")
	t.Setenv("RABBITMQ_MGMT_PORT", "15672")
	t.Setenv("RABBITMQ_USER", "guest")
	env := fakeEnv{secrets: map[string]string{"RABBITMQ_PASSWORD": "secretpw"}}

	tool := &Tool{cfg: Config{Connections: map[string]ConnConfig{"prod": {}}}}
	if err := tool.Init(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	cc := tool.cfg.Connections["prod"]
	if cc.BaseURL != "http://mq.internal:15672" {
		t.Errorf("BaseURL = %q", cc.BaseURL)
	}
	if cc.Username != "guest" {
		t.Errorf("Username = %q", cc.Username)
	}
	if cc.Password != "secretpw" {
		t.Errorf("Password = %q", cc.Password)
	}
	if cc.Vhost != "/" {
		t.Errorf("Vhost = %q, want default /", cc.Vhost)
	}
	if tool.client == nil {
		t.Error("Init did not build an http.Client")
	}
}

// TestInit_PreservesTestSeamValues verifies the httptest seam: pre-set
// client / BaseURL / Username / Password / Vhost survive Init untouched,
// so tests can point the tool at an httptest server without env vars.
func TestInit_PreservesTestSeamValues(t *testing.T) {
	tool := &Tool{
		cfg: Config{Connections: map[string]ConnConfig{
			"prod": {BaseURL: "http://example", Username: "u", Password: "p", Vhost: "/custom"},
		}},
		client: http.DefaultClient,
	}
	if err := tool.Init(context.Background(), fakeEnv{}); err != nil {
		t.Fatal(err)
	}
	cc := tool.cfg.Connections["prod"]
	if cc.BaseURL != "http://example" || cc.Username != "u" || cc.Password != "p" || cc.Vhost != "/custom" {
		t.Errorf("Init overwrote pre-set values: %+v", cc)
	}
	if tool.client != http.DefaultClient {
		t.Error("Init replaced pre-set client")
	}
}

func TestInit_DisableRabbitMQEnv(t *testing.T) {
	for _, v := range []string{"true", "1"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("DISABLE_RABBITMQ", v)
			tool := &Tool{cfg: Config{Connections: map[string]ConnConfig{}}}
			if err := tool.Init(context.Background(), fakeEnv{}); err != nil {
				t.Fatal(err)
			}
			if !tool.cfg.Disabled {
				t.Errorf("Disabled = false, want true for DISABLE_RABBITMQ=%s", v)
			}
		})
	}
}

func TestFactoryRequiresToolDescription(t *testing.T) {
	_, err := Factory.New(json.RawMessage(`{"connections":{"prod":{"vhost":"/"}}}`))
	if err == nil {
		t.Fatal("expected error when toolDescription missing")
	}
}

func TestToolSpecUsesConfiguredName(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"toolName":"rabbitmq_inspect","toolDescription":"d","connections":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := n.(node.Tool).ToolSpec().Name; got != "rabbitmq_inspect" {
		t.Fatalf("ToolSpec().Name = %q", got)
	}
}

func TestToolSpecDefaultsName(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"toolDescription":"d","connections":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := n.(node.Tool).ToolSpec().Name; got != "rabbitmq_inspect" {
		t.Fatalf("ToolSpec().Name = %q, want default rabbitmq_inspect", got)
	}
}

// helper: bypass Init, inject the httptest client seam directly.
func newToolForTest(cfg Config) *Tool {
	return &Tool{cfg: cfg, client: http.DefaultClient}
}

// fakeEnv is a minimal node.NodeEnv stand-in for Init tests.
type fakeEnv struct {
	secrets map[string]string
}

func (f fakeEnv) Logger() *stdlog.Logger { return stdlog.New(io.Discard, "", 0) }
func (f fakeEnv) NodeID() string         { return "n1" }
func (f fakeEnv) WorkflowID() string     { return "wf1" }
func (f fakeEnv) Secret(name string) (secret.String, error) {
	if v, ok := f.secrets[name]; ok {
		return secret.New(v), nil
	}
	return secret.String{}, nil
}
func (f fakeEnv) Render(s string, _ node.RenderCtx) (string, error) { return s, nil }
func (f fakeEnv) Host(_ string) (any, bool)                         { return nil, false }
func (f fakeEnv) Peer(_ string) ([]node.Node, error)                { return nil, nil }
func (f fakeEnv) AwaitClientToolResult(_ context.Context, _ string, _ time.Duration) ([]byte, error) {
	return nil, nil
}
func (f fakeEnv) RetryPolicy() node.RetryPolicy { return nil }

var _ node.NodeEnv = fakeEnv{}
