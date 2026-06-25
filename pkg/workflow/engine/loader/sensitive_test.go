package loader_test

import (
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/loader"
)

func TestSensitivePasswordLiteralRejected(t *testing.T) {
	in := map[string]any{"password": "literal"}
	errs := loader.ValidateSensitiveFields(in)
	if len(errs) == 0 {
		t.Fatal("expected error for literal password")
	}
}

func TestSensitivePasswordEnvFormAccepted(t *testing.T) {
	in := map[string]any{"passwordEnv": "OPENSEARCH_PASSWORD"}
	errs := loader.ValidateSensitiveFields(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
}

func TestSensitivePasswordEnvWithVarRefRejected(t *testing.T) {
	in := map[string]any{"passwordEnv": "${OPENSEARCH_PASSWORD}"}
	errs := loader.ValidateSensitiveFields(in)
	if len(errs) == 0 {
		t.Fatal("expected error for ${} ref in *Env field")
	}
}

func TestSensitiveDsnLiteralRejected(t *testing.T) {
	in := map[string]any{"dsn": "postgres://u:p@h/db"}
	errs := loader.ValidateSensitiveFields(in)
	if len(errs) == 0 {
		t.Fatal("expected error for dsn literal")
	}
}

func TestSensitiveDsnEnvAccepted(t *testing.T) {
	in := map[string]any{"dsnEnv": "CHAT_MEMORY_DSN"}
	if errs := loader.ValidateSensitiveFields(in); len(errs) != 0 {
		t.Fatalf("got %v", errs)
	}
}

func TestSensitiveTokenLiteralRejected(t *testing.T) {
	in := map[string]any{"apiToken": "abc"}
	errs := loader.ValidateSensitiveFields(in)
	if len(errs) == 0 || !strings.Contains(errs[0].Error(), "apiToken") {
		t.Fatalf("got %v", errs)
	}
}

func TestSensitiveNestedFieldRejected(t *testing.T) {
	in := map[string]any{"nodes": []any{
		map[string]any{"config": map[string]any{"apiKey": "leak"}},
	}}
	errs := loader.ValidateSensitiveFields(in)
	if len(errs) == 0 {
		t.Fatal("expected error for nested apiKey")
	}
}

func TestSensitiveMaxTokensNotFlagged(t *testing.T) {
	// "maxTokens" contains "Token" as substring but does not END with "token";
	// the validator must NOT treat it as sensitive.
	in := map[string]any{"maxTokens": 4096}
	if errs := loader.ValidateSensitiveFields(in); len(errs) != 0 {
		t.Fatalf("maxTokens flagged as sensitive: %v", errs)
	}
}

func TestSensitivePluralTokensNotFlagged(t *testing.T) {
	// "tokens" (plural) is not the sensitive suffix "token".
	in := map[string]any{"tokens": []any{}}
	if errs := loader.ValidateSensitiveFields(in); len(errs) != 0 {
		t.Fatalf("tokens flagged as sensitive: %v", errs)
	}
}
