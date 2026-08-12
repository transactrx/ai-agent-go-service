// Package webfetch is the tool/web-fetch node: HTTPS GET with body
// size cap and HTML→text stripping.
//
// SSRF guard: the tool talks to the OPEN INTERNET on the agent's behalf, so
// every hop — the initial URL, every redirect target, and the resolved dial
// address — is validated:
//   - scheme must be https (re-checked per redirect; a public page 302'ing
//     to http://169.254.169.254/... is the classic bypass)
//   - hostname must not be on the config denylist (re-checked per redirect)
//   - the address actually dialed must not be loopback / private / link-local
//     / CGNAT / ULA unless allowPrivateHosts is set — this closes the
//     DNS-name-resolving-to-internal-IP hole that URL-level checks miss
package webfetch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Config is decoded from the workflow-JSON config block.
type Config struct {
	ToolName        string   `json:"toolName,omitempty"` // default "web_fetch"
	ToolDescription string   `json:"toolDescription"`    // required
	DefaultMaxBytes int      `json:"defaultMaxBytes,omitempty"`
	TimeoutMs       int      `json:"timeoutMs,omitempty"`
	HostDenylist    []string `json:"hostDenylist,omitempty"`
	// AllowPrivateHosts disables the private/loopback/link-local IP dial
	// guard. Leave false unless the workflow genuinely needs to fetch
	// internal https endpoints.
	AllowPrivateHosts bool `json:"allowPrivateHosts,omitempty"`
	// FailurePolicy controls what the agent does on a Go error from this tool.
	// Default "surface-to-llm".
	FailurePolicy string `json:"failurePolicy,omitempty"`
}

type fetchInput struct {
	URL      string `json:"url"`
	MaxBytes *int   `json:"maxBytes,omitempty"`
}

type fetchOutput struct {
	URL        string `json:"url"`
	StatusCode int    `json:"statusCode"`
	Body       string `json:"body"`
	Bytes      int    `json:"bytes"`
}

// ClientOverride is a test-only hook: when non-nil, Init uses this
// http.Client instead of building a fresh one. Tests that need to
// trust an httptest.NewTLSServer cert can set this to srv.Client()
// and reset to nil in t.Cleanup.
var ClientOverride *http.Client

// Tool implements the tool/web-fetch node.
type Tool struct {
	node.BaseTool
	cfg    Config
	client *http.Client
}

func (t *Tool) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type:        NodeType,
		Role:        node.RoleTool,
		Description: "HTTPS GET a known URL and return stripped text.",
		OutputPorts: []node.PortSpec{{Name: node.PortAITool, Direction: node.PortOut, Cardinality: node.CardOne}},
	}
}

func (t *Tool) Init(_ context.Context, env node.NodeEnv) error {
	t.InitRetry(env)
	if t.client != nil {
		return nil // already configured (test path)
	}
	if ClientOverride != nil {
		t.client = ClientOverride
		return nil
	}
	timeout := time.Duration(t.cfg.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	t.client = t.buildGuardedClient(timeout)
	return nil
}

// buildGuardedClient constructs the SSRF-guarded http.Client (see package
// comment for the threat model).
func (t *Tool) buildGuardedClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout: timeout,
		// Control runs AFTER DNS resolution with the concrete address being
		// dialed — the only place a name→internal-IP rebind can be caught.
		Control: func(_, address string, _ syscall.RawConn) error {
			if t.cfg.AllowPrivateHosts {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("web_fetch: bad dial address %q: %w", address, err)
			}
			if ip := net.ParseIP(host); ip != nil && isForbiddenIP(ip) {
				return fmt.Errorf("web_fetch: dial to non-public address %s blocked", ip)
			}
			return nil
		},
	}
	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		TLSHandshakeTimeout: timeout,
		Proxy:               http.ProxyFromEnvironment,
	}
	return &http.Client{
		Timeout:       timeout,
		Transport:     transport,
		CheckRedirect: t.checkRedirect,
	}
}

// checkRedirect re-validates every redirect target: scheme + denylist.
// Without this, the https-only and denylist checks on the initial URL are
// decorative — any fetched page can 302 to http://<internal-host>/...
func (t *Tool) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 5 {
		return fmt.Errorf("web_fetch: too many redirects")
	}
	return t.validateURL(req.URL)
}

// validateURL enforces the https-only + denylist rules on one URL. Called for
// the initial request and for every redirect hop.
func (t *Tool) validateURL(u *url.URL) error {
	if u.Scheme != "https" {
		return fmt.Errorf("web_fetch requires https URL (got scheme %q in %q)", u.Scheme, u.Redacted())
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" && !t.cfg.AllowPrivateHosts {
		return fmt.Errorf("web_fetch: host %q is not a public host", host)
	}
	if ip := net.ParseIP(host); ip != nil && !t.cfg.AllowPrivateHosts && isForbiddenIP(ip) {
		return fmt.Errorf("web_fetch: address %s is not public", ip)
	}
	for _, deny := range t.cfg.HostDenylist {
		if strings.ToLower(deny) == host {
			return fmt.Errorf("web_fetch: host %q is denied by configuration", host)
		}
	}
	return nil
}

// isForbiddenIP reports whether ip is anything but a public unicast address:
// loopback, RFC1918 private, link-local (incl. 169.254.169.254 metadata),
// CGNAT 100.64/10, IPv6 ULA, unspecified, and multicast are all forbidden.
func isForbiddenIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	// CGNAT 100.64.0.0/10 (net.IP has no helper for it).
	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1]&0xC0 == 64 {
		return true
	}
	// IPv6 unique-local fc00::/7 (IsPrivate covers this on modern Go, kept
	// explicit for clarity).
	if v6 := ip.To16(); v6 != nil && ip.To4() == nil && v6[0]&0xFE == 0xFC {
		return true
	}
	return false
}

func (t *Tool) Close(_ context.Context) error { return nil }

func (t *Tool) ToolSpec() node.ToolSpec {
	name := t.cfg.ToolName
	if name == "" {
		name = "web_fetch"
	}
	desc := t.cfg.ToolDescription
	if desc == "" {
		desc = "Fetch a known HTTPS URL and return its body stripped to plain text. Use after web_search to read a discovered page."
	}
	return node.ToolSpec{
		Name:        name,
		Description: desc,
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"url":{"type":"string","description":"HTTPS URL to fetch"},
				"maxBytes":{"type":"integer","description":"Override the default max-bytes cap for this call"}
			},
			"required":["url"]
		}`),
	}
}

// FailurePolicy returns the configured policy, defaulting to surface-to-llm.
func (t *Tool) FailurePolicy() node.FailurePolicy {
	if t.cfg.FailurePolicy == "" {
		return node.FailureSurfaceToLLM
	}
	return node.FailurePolicy(t.cfg.FailurePolicy)
}

func (t *Tool) Invoke(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	var in fetchInput
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("parse args: %w", err)
	}
	parsed, err := url.Parse(in.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}
	// Same validation applied to every redirect hop (see CheckRedirect); the
	// resolved dial address is additionally guarded in the transport.
	if err := t.validateURL(parsed); err != nil {
		return nil, err
	}

	maxBytes := t.cfg.DefaultMaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20 // 1 MiB default
	}
	if in.MaxBytes != nil && *in.MaxBytes > 0 && *in.MaxBytes < maxBytes {
		maxBytes = *in.MaxBytes
	}

	req, err := http.NewRequestWithContext(ctx, "GET", in.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", in.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 && resp.StatusCode < 600 {
		return nil, &node.ToolError{Code: "upstream", Retryable: true,
			Cause: fmt.Errorf("web_fetch upstream %d %s", resp.StatusCode, resp.Status)}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxBytes)))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	text := stripHTML(string(body))
	out := fetchOutput{
		URL:        in.URL,
		StatusCode: resp.StatusCode,
		Body:       text,
		Bytes:      len(body),
	}
	return json.Marshal(out)
}

var (
	scriptRE = regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script>`)
	styleRE  = regexp.MustCompile(`(?is)<style\b[^>]*>.*?</style>`)
	tagRE    = regexp.MustCompile(`<[^>]+>`)
	wsRE     = regexp.MustCompile(`\s+`)
)

func stripHTML(s string) string {
	s = scriptRE.ReplaceAllString(s, "")
	s = styleRE.ReplaceAllString(s, "")
	s = tagRE.ReplaceAllString(s, " ")
	s = wsRE.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// Compile-time check that Tool satisfies the node.Tool interface.
var _ node.Tool = (*Tool)(nil)
