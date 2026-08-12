package rabbitmqmanagement

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestRabbitMQ_RequestTimeoutEnforced proves requestTimeoutMs bounds the
// management-API call — a slow server must fail fast, not ride the shared
// client's safety-net ceiling.
func TestRabbitMQ_RequestTimeoutEnforced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	tool := &Tool{
		cfg: Config{ToolDescription: "d", Connections: map[string]ConnConfig{
			"prod": {BaseURL: srv.URL, Username: "u", Password: "p", RequestTimeoutMs: 50},
		}},
		client: srv.Client(),
	}

	args, _ := json.Marshal(map[string]any{"connection": "prod", "op": "overview"})
	start := time.Now()
	_, err := tool.Invoke(context.Background(), args)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("expected ctx deadline error, got: %v", err)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("call took %v — requestTimeoutMs=50 not enforced", elapsed)
	}
}
