package webfetch

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// guardTool builds a Tool with the SSRF guard ACTIVE (AllowPrivateHosts false).
func guardTool(cfg Config, c *http.Client) *Tool {
	return &Tool{cfg: cfg, client: c}
}

func TestSSRF_RedirectToHTTP_Rejected(t *testing.T) {
	// TLS server whose page redirects to a plain-http internal-style URL —
	// the classic https-check bypass. The redirect must be refused.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	defer srv.Close()

	tool := guardTool(Config{DefaultMaxBytes: 1024, TimeoutMs: 1000, AllowPrivateHosts: true}, srv.Client())
	// wire the tool's own redirect policy onto the test client (the guarded
	// client built in Init carries it; srv.Client() does not)
	cl := srv.Client()
	cl.CheckRedirect = tool.checkRedirect
	tool.client = cl

	// AllowPrivateHosts=true lets the INITIAL fetch reach 127.0.0.1; scheme
	// re-validation on the hop must still fire.
	args, _ := json.Marshal(map[string]any{"url": srv.URL})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("expected https re-validation error on redirect, got %v", err)
	}
}

func TestSSRF_RedirectToDeniedHost_Rejected(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.example.com/x", http.StatusFound)
	}))
	defer srv.Close()

	tool := guardTool(Config{DefaultMaxBytes: 1024, TimeoutMs: 1000,
		AllowPrivateHosts: true, HostDenylist: []string{"evil.example.com"}}, nil)
	cl := srv.Client()
	cl.CheckRedirect = tool.checkRedirect
	tool.client = cl

	args, _ := json.Marshal(map[string]any{"url": srv.URL})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("expected denylist re-validation error on redirect, got %v", err)
	}
}

func TestSSRF_InitialPrivateAddress_Rejected(t *testing.T) {
	tool := guardTool(Config{DefaultMaxBytes: 1024, TimeoutMs: 1000}, http.DefaultClient)
	for _, u := range []string{
		"https://127.0.0.1/x",
		"https://localhost/x",
		"https://10.1.2.3/x",
		"https://192.168.1.1/x",
		"https://169.254.169.254/latest/meta-data/",
		"https://100.96.0.2/x", // CGNAT
	} {
		args, _ := json.Marshal(map[string]any{"url": u})
		if _, err := tool.Invoke(context.Background(), args); err == nil {
			t.Errorf("%s: expected non-public rejection, got nil", u)
		}
	}
}

func TestSSRF_ValidateURL_PublicHostsPass(t *testing.T) {
	tool := guardTool(Config{}, nil)
	for _, raw := range []string{"https://example.com/a", "https://8.8.8.8/x"} {
		u, _ := url.Parse(raw)
		if err := tool.validateURL(u); err != nil {
			t.Errorf("%s: expected pass, got %v", raw, err)
		}
	}
}

func TestSSRF_IsForbiddenIP_Table(t *testing.T) {
	forbidden := []string{
		"127.0.0.1", "10.0.0.1", "172.16.5.5", "192.168.0.9",
		"169.254.169.254", "100.64.0.1", "100.127.255.255", "0.0.0.0",
		"224.0.0.1", "::1", "fe80::1", "fc00::1", "fd12::34",
	}
	public := []string{"8.8.8.8", "1.1.1.1", "100.63.255.255", "100.128.0.1", "2606:4700::1111"}
	for _, s := range forbidden {
		if !isForbiddenIP(net.ParseIP(s)) {
			t.Errorf("%s: expected forbidden", s)
		}
	}
	for _, s := range public {
		if isForbiddenIP(net.ParseIP(s)) {
			t.Errorf("%s: expected public", s)
		}
	}
}

func TestSSRF_DialGuard_BlocksResolvedPrivateAddress(t *testing.T) {
	// A name that RESOLVES to loopback but passes the URL-level checks would
	// only be caught at dial time. Simulate by calling the guarded client's
	// dialer Control path via a real request to a loopback TLS server, with
	// the URL-level check bypassed (IP literal is caught earlier, so exercise
	// the transport directly).
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("should never arrive"))
	}))
	defer srv.Close()

	tool := guardTool(Config{TimeoutMs: 1000}, nil)
	guarded := tool.buildGuardedClient(0)
	// trust the test server cert so a TLS failure can't mask the dial block
	guarded.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig

	_, err := guarded.Get(srv.URL)
	if err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("expected dial-level block of loopback, got %v", err)
	}
}
