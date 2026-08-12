package webfetch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestWebFetch_HappyPath_HTTPS(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("hello from the test server"))
	}))
	defer srv.Close()

	tool := newToolForTest(Config{DefaultMaxBytes: 1024, TimeoutMs: 1000}, srv.Client())
	args, _ := json.Marshal(map[string]any{"url": srv.URL})
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		t.Fatalf("got err: %v", err)
	}
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	if !strings.Contains(resp["body"].(string), "hello from the test server") {
		t.Errorf("body = %v", resp["body"])
	}
}

func TestWebFetch_NonHTTPS_Rejected(t *testing.T) {
	tool := newToolForTest(Config{DefaultMaxBytes: 1024, TimeoutMs: 1000}, http.DefaultClient)
	args, _ := json.Marshal(map[string]any{"url": "http://example.com"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("expected https-only error, got %v", err)
	}
}

func TestWebFetch_HostDenylist_Rejected(t *testing.T) {
	tool := newToolForTest(Config{DefaultMaxBytes: 1024, TimeoutMs: 1000, HostDenylist: []string{"evil.example.com"}}, http.DefaultClient)
	args, _ := json.Marshal(map[string]any{"url": "https://evil.example.com/path"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("expected denylist rejection, got %v", err)
	}
}

func TestWebFetch_HTMLStrippedToText(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body><p>Hello</p><script>bad()</script></body></html>"))
	}))
	defer srv.Close()

	tool := newToolForTest(Config{DefaultMaxBytes: 1024, TimeoutMs: 1000}, srv.Client())
	args, _ := json.Marshal(map[string]any{"url": srv.URL})
	out, _ := tool.Invoke(context.Background(), args)
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	body := resp["body"].(string)
	if strings.Contains(body, "<script>") || strings.Contains(body, "<p>") {
		t.Errorf("HTML tags not stripped: %q", body)
	}
	if strings.Contains(body, "bad()") {
		t.Errorf("script content leaked: %q", body)
	}
	if !strings.Contains(body, "Hello") {
		t.Errorf("expected 'Hello' in body, got %q", body)
	}
}

func TestWebFetch_MaxBytesCap(t *testing.T) {
	big := strings.Repeat("x", 10_000)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(big))
	}))
	defer srv.Close()

	tool := newToolForTest(Config{DefaultMaxBytes: 100, TimeoutMs: 1000}, srv.Client())
	args, _ := json.Marshal(map[string]any{"url": srv.URL})
	out, _ := tool.Invoke(context.Background(), args)
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	if len(resp["body"].(string)) > 100 {
		t.Errorf("body length %d exceeds cap 100", len(resp["body"].(string)))
	}
}

func TestWebFetch_RespectsContextCancellation(t *testing.T) {
	hold := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-hold
		_, _ = w.Write([]byte("late"))
	}))
	defer srv.Close()
	defer close(hold)

	tool := newToolForTest(Config{DefaultMaxBytes: 1024, TimeoutMs: 5000}, srv.Client())
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	args, _ := json.Marshal(map[string]any{"url": srv.URL})
	_, err := tool.Invoke(ctx, args)
	if err == nil {
		t.Fatal("want error on ctx cancel")
	}
}

func TestWebFetch_5xx_ReturnsRetryableUpstreamToolError(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	tool := newToolForTest(Config{DefaultMaxBytes: 1024, TimeoutMs: 1000}, srv.Client())
	args, _ := json.Marshal(map[string]any{"url": srv.URL})
	_, err := tool.Invoke(context.Background(), args)

	var te *node.ToolError
	if !errors.As(err, &te) || !te.Retryable || te.Code != "upstream" {
		t.Fatalf("want retryable upstream ToolError, got %v", err)
	}
}

func TestFactoryRequiresToolDescription(t *testing.T) {
	_, err := Factory.New(json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected error when toolDescription missing")
	}
}

func TestToolSpecUsesConfiguredName(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"toolName":"web_fetch","toolDescription":"d"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := n.(node.Tool).ToolSpec().Name; got != "web_fetch" {
		t.Fatalf("ToolSpec().Name = %q", got)
	}
}

func TestToolSpecDefaultsToolName(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"toolDescription":"d"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := n.(node.Tool).ToolSpec().Name; got != "web_fetch" {
		t.Fatalf("ToolSpec().Name = %q, want default web_fetch", got)
	}
}

// Helper: construct a Tool with a custom http.Client (used to accept httptest
// TLS certs). httptest servers listen on 127.0.0.1, which the SSRF dial guard
// forbids by default — fetch-semantics tests opt out; the guard itself is
// covered by ssrf_test.go.
func newToolForTest(cfg Config, c *http.Client) *Tool {
	cfg.AllowPrivateHosts = true
	return &Tool{cfg: cfg, client: c}
}
