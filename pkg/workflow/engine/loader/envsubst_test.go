package loader_test

import (
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/loader"
)

func lookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestResolveLiteral(t *testing.T) {
	out, errs := loader.Resolve("plain", lookup(nil))
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if s, _ := out.(string); s != "plain" {
		t.Fatalf("got %v", out)
	}
}

func TestResolveSimple(t *testing.T) {
	out, errs := loader.Resolve("${X}", lookup(map[string]string{"X": "ok"}))
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if s, _ := out.(string); s != "ok" {
		t.Fatalf("got %v", out)
	}
}

func TestResolveDefault(t *testing.T) {
	out, errs := loader.Resolve("${MISSING:fallback}", lookup(nil))
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if s, _ := out.(string); s != "fallback" {
		t.Fatalf("got %v", out)
	}
}

func TestResolveMissingNoDefault(t *testing.T) {
	_, errs := loader.Resolve(map[string]any{"a": "${MISSING}"}, lookup(nil))
	if len(errs) != 1 {
		t.Fatalf("errs: %v", errs)
	}
	if !strings.Contains(errs[0].Path, "a") {
		t.Fatalf("path: %s", errs[0].Path)
	}
	if errs[0].Var != "MISSING" {
		t.Fatalf("var: %s", errs[0].Var)
	}
}

func TestResolveNested(t *testing.T) {
	in := map[string]any{
		"region":   "${AWS_REGION:us-east-1}",
		"settings": map[string]any{"host": "${HOST}"},
		"list":     []any{"${X}", "literal"},
	}
	out, errs := loader.Resolve(in, lookup(map[string]string{"HOST": "h", "X": "v"}))
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	m := out.(map[string]any)
	if m["region"] != "us-east-1" {
		t.Fatalf("region: %v", m["region"])
	}
	if m["settings"].(map[string]any)["host"] != "h" {
		t.Fatalf("host: %v", m["settings"])
	}
	arr := m["list"].([]any)
	if arr[0] != "v" || arr[1] != "literal" {
		t.Fatalf("list: %v", arr)
	}
}

func TestResolveEmptyValueTreatedAsMissing(t *testing.T) {
	_, errs := loader.Resolve("${X}", lookup(map[string]string{"X": ""}))
	if len(errs) != 1 {
		t.Fatalf("errs: %v", errs)
	}
}
