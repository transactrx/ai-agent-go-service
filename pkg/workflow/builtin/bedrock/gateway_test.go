package bedrock

import (
	"strings"
	"testing"
)

// TestGatewaySubject: env override wins (trimmed); default is the generic
// placeholder base path (repo convention: deployments set the org value).
func TestGatewaySubject(t *testing.T) {
	t.Setenv(gatewayBasePathEnv, "")
	if got := gatewaySubject(); got != "example.inferenceGateway.resolveModel" {
		t.Fatalf("default subject = %q", got)
	}
	t.Setenv(gatewayBasePathEnv, "trx.inferenceGateway")
	if got := gatewaySubject(); got != "trx.inferenceGateway.resolveModel" {
		t.Fatalf("override subject = %q", got)
	}
	t.Setenv(gatewayBasePathEnv, "  trx.gw  ")
	if got := gatewaySubject(); got != "trx.gw.resolveModel" {
		t.Fatalf("trimmed subject = %q", got)
	}
}

// TestDeriveGatewayQuery: lab is always "anthropic" (the only lab
// parseModelID accepts); family is the dash form the gateway tokenizer
// matches ("claude-opus" ≡ catalog "claude opus"). Legacy IDs error.
func TestDeriveGatewayQuery(t *testing.T) {
	cases := []struct {
		id, lab, family string
		wantErr         bool
	}{
		{id: "us.anthropic.claude-opus-4-7", lab: "anthropic", family: "claude-opus"},
		{id: "global.anthropic.claude-opus-5-20270101-v1:0", lab: "anthropic", family: "claude-opus"},
		{id: "anthropic.claude-sonnet-4-5", lab: "anthropic", family: "claude-sonnet"},
		{id: "anthropic.claude-3-5-sonnet-20241022-v2:0", wantErr: true},
		{id: "meta.llama3-1-8b-instruct-v1:0", wantErr: true},
	}
	for _, c := range cases {
		lab, family, err := deriveGatewayQuery(c.id)
		if c.wantErr {
			if err == nil {
				t.Errorf("deriveGatewayQuery(%q): expected error", c.id)
			}
			continue
		}
		if err != nil {
			t.Errorf("deriveGatewayQuery(%q): %v", c.id, err)
			continue
		}
		if lab != c.lab || family != c.family {
			t.Errorf("deriveGatewayQuery(%q) = %q,%q want %q,%q", c.id, lab, family, c.lab, c.family)
		}
	}
}

// TestParseResolveReply: success needs 2xx (or absent) status AND a non-empty
// invokeId; error replies surface status + errorMessage; garbage fails.
func TestParseResolveReply(t *testing.T) {
	ok := `{"modelId":"anthropic.claude-opus-4-8","invokeId":"us.anthropic.claude-opus-4-8","family":"claude opus"}`
	cases := []struct {
		name, status, body, want, wantErrPart string
	}{
		{name: "success", status: "200", body: ok, want: "us.anthropic.claude-opus-4-8"},
		{name: "no status header", status: "", body: ok, want: "us.anthropic.claude-opus-4-8"},
		{name: "error status", status: "400", body: `{"status":400,"errorMessage":"no invocable model matches"}`, wantErrPart: "400"},
		{name: "empty invokeId", status: "200", body: `{"modelId":"anthropic.claude-opus-4-8"}`, wantErrPart: "invokeId"},
		{name: "not json", status: "200", body: `Server Error`, wantErrPart: "JSON"},
	}
	for _, c := range cases {
		got, err := parseResolveReply(c.status, []byte(c.body))
		if c.wantErrPart != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErrPart) {
				t.Errorf("%s: err = %v, want containing %q", c.name, err, c.wantErrPart)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.name, got, err, c.want)
		}
	}
}
