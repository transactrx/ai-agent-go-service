package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestBuildFinalQueryNoLLMClausesAddsPolicyMust(t *testing.T) {
	policyMust := []json.RawMessage{json.RawMessage(`{"term":{"origin.keyword":"PioneerRx"}}`)}
	final := buildFinalQuery(nil, policyMust)
	b, _ := json.Marshal(final)
	s := string(b)
	if !strings.Contains(s, "PioneerRx") {
		t.Fatalf("missing policy clause: %s", s)
	}
	if !strings.Contains(s, `"bool"`) {
		t.Fatalf("missing bool wrap: %s", s)
	}
}

func TestBuildFinalQueryAppendsPolicyMust(t *testing.T) {
	llm := map[string]json.RawMessage{
		"must": json.RawMessage(`[{"term":{"status":"R"}}]`),
	}
	policyMust := []json.RawMessage{json.RawMessage(`{"term":{"origin.keyword":"X"}}`)}
	final := buildFinalQuery(llm, policyMust)
	b, _ := json.Marshal(final)
	s := string(b)
	if !strings.Contains(s, `"status":"R"`) {
		t.Fatalf("dropped llm clause: %s", s)
	}
	if !strings.Contains(s, `"origin.keyword":"X"`) {
		t.Fatalf("dropped policy clause: %s", s)
	}
}

func TestInvokeRejectsDisallowedKey(t *testing.T) {
	tool := &opensearchTool{cfg: Config{}, http: &http.Client{}}
	tool.policy = stubPolicy{}
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30","queryBody":{"should":[{}]}}`)
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	_, err := tool.Invoke(ctx, args)
	if err == nil || !strings.Contains(err.Error(), "disallowed key") {
		t.Fatalf("expected disallowed-key error, got %v", err)
	}
}

func TestInvokeRejectsMissingAccount(t *testing.T) {
	tool := newToolWithPattern(t, "prod.cpe-*")
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30","queryBody":{}}`)
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "account id") {
		t.Fatalf("expected missing-account error, got %v", err)
	}
}

func TestInvokeReturnsPolicyDenied(t *testing.T) {
	re, err := compileIndexPattern("prod.cpe-*")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	tool := &opensearchTool{
		cfg:           Config{AllowedIndexPattern: "prod.cpe-*"},
		http:          &http.Client{},
		policy:        stubPolicy{deny: true},
		allowedRegexp: re,
	}
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30","queryBody":{}}`)
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	_, err = tool.Invoke(ctx, args)
	var pde *node.PolicyDeniedError
	if err == nil {
		t.Fatal("expected PolicyDeniedError")
	}
	if !errors.As(err, &pde) {
		t.Fatalf("expected PolicyDeniedError, got %T: %v", err, err)
	}
}

func TestInvokeHTTPRoundTripIncludesBoolWrap(t *testing.T) {
	var seenBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		seenBody = string(buf)
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0}}}`))
	}))
	defer srv.Close()

	re, err := compileIndexPattern("prod.cpe-*")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	tool := &opensearchTool{
		cfg: Config{
			Host:                  srv.URL,
			RequestTimeoutSeconds: 5,
			MaxResultSize:         500,
			AllowedIndexPattern:   "prod.cpe-*",
		},
		http:          srv.Client(),
		policy:        stubPolicy{},
		user:          secret.New("u"),
		pass:          secret.New("p"),
		allowedRegexp: re,
	}
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30","queryBody":{"must":[{"term":{"x":"y"}}]}}`)
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	out, err := tool.Invoke(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "hits") {
		t.Fatalf("response missing hits: %s", out)
	}
	if !strings.Contains(seenBody, `"bool"`) || !strings.Contains(seenBody, `"x":"y"`) {
		t.Fatalf("server received unexpected body: %s", seenBody)
	}
}

func TestInvokeRejectsCommaListBypass(t *testing.T) {
	tool := newToolWithPattern(t, "prod.cpe-*")
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30,*","queryBody":{}}`)
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	_, err := tool.Invoke(ctx, args)
	if err == nil || !strings.Contains(err.Error(), "allowed pattern") {
		t.Fatalf("expected pattern-rejection error, got %v", err)
	}
}

func TestInvokeRejectsCrossFamilyBypass(t *testing.T) {
	tool := newToolWithPattern(t, "prod.cpe-*")
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30,events.eprescribe-2026-04-30","queryBody":{}}`)
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	_, err := tool.Invoke(ctx, args)
	if err == nil || !strings.Contains(err.Error(), "allowed pattern") {
		t.Fatalf("expected pattern-rejection error, got %v", err)
	}
}

func TestInvokeRejectsQueryStringInjection(t *testing.T) {
	tool := newToolWithPattern(t, "prod.cpe-*")
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30?expand_wildcards=hidden","queryBody":{}}`)
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	_, err := tool.Invoke(ctx, args)
	if err == nil || !strings.Contains(err.Error(), "allowed pattern") {
		t.Fatalf("expected pattern-rejection error, got %v", err)
	}
}

func TestInvokeAcceptsLegitimatePath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0}}}`))
	}))
	defer srv.Close()
	tool := newToolWithPattern(t, "prod.cpe-*")
	tool.cfg.Host = srv.URL
	tool.http = srv.Client()
	tool.user = secret.New("u")
	tool.pass = secret.New("p")
	tool.cfg.MaxResultSize = 500
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30,prod.cpe-2026-04-29","queryBody":{}}`)
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	if _, err := tool.Invoke(ctx, args); err != nil {
		t.Fatalf("legit path rejected: %v", err)
	}
}

func TestBuildURLSetsIgnoreUnavailable(t *testing.T) {
	got := buildURL("https://os.example.com", "prod.cpe-2026-07-30,prod.cpe-2026-07-31")
	want := "https://os.example.com/prod.cpe-2026-07-30,prod.cpe-2026-07-31/_search?ignore_unavailable=true"
	if got != want {
		t.Fatalf("buildURL = %q, want %q", got, want)
	}
}

func TestInvokeRequestCarriesIgnoreUnavailable(t *testing.T) {
	var seenURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenURL = r.URL.String()
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0}}}`))
	}))
	defer srv.Close()
	tool := newToolWithPattern(t, "prod.cpe-*")
	tool.cfg.Host = srv.URL
	tool.http = srv.Client()
	tool.user = secret.New("u")
	tool.pass = secret.New("p")
	tool.cfg.MaxResultSize = 500
	args := json.RawMessage(`{"indexPath":"prod.cpe-2026-07-30,prod.cpe-2026-07-31","queryBody":{}}`)
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	if _, err := tool.Invoke(ctx, args); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if seenURL != "/prod.cpe-2026-07-30,prod.cpe-2026-07-31/_search?ignore_unavailable=true" {
		t.Fatalf("server saw URL %q, want ignore_unavailable=true query param", seenURL)
	}
}

// newToolWithPattern is a test helper that builds an opensearchTool with a
// compiled pattern and a permissive stub policy.
func newToolWithPattern(t *testing.T, pattern string) *opensearchTool {
	t.Helper()
	re, err := compileIndexPattern(pattern)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return &opensearchTool{
		cfg:           Config{AllowedIndexPattern: pattern},
		http:          &http.Client{},
		policy:        stubPolicy{},
		allowedRegexp: re,
	}
}

// stubPolicy is a minimal node.Policy for tests.
type stubPolicy struct {
	deny bool
}

func (stubPolicy) Spec() node.NodeSpec                          { return node.NodeSpec{Role: node.RolePolicy} }
func (stubPolicy) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (stubPolicy) Close(_ context.Context) error                { return nil }
func (s stubPolicy) Resolve(_ context.Context, _ node.PolicyRequest) (node.PolicyResult, error) {
	if s.deny {
		return node.PolicyResult{Deny: true, DenyReason: "stub"}, nil
	}
	return node.PolicyResult{}, nil
}
