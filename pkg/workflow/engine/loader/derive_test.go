package loader

import (
	"reflect"
	"testing"
)

func TestExtendsTarget(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{"present", `{"id":"d","extends":"base"}`, "base", true},
		{"absent", `{"id":"plain"}`, "", false},
		{"empty string", `{"id":"d","extends":""}`, "", false},
		{"malformed json", `{"id":`, "", false},
		{"wrong type", `{"id":"d","extends":42}`, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := ExtendsTarget([]byte(c.raw))
			if got != c.want || ok != c.ok {
				t.Fatalf("ExtendsTarget(%s) = (%q,%v), want (%q,%v)", c.raw, got, ok, c.want, c.ok)
			}
		})
	}
}

func TestDeepMerge(t *testing.T) {
	base := map[string]any{
		"a": "base", "keep": 1.0,
		"obj": map[string]any{"x": "bx", "y": "by"},
	}
	overlay := map[string]any{
		"a": "over",
		"obj": map[string]any{"x": "ox", "z": "oz"},
		"new": true,
	}
	got := deepMerge(base, overlay)
	want := map[string]any{
		"a": "over", "keep": 1.0,
		"obj": map[string]any{"x": "ox", "y": "by", "z": "oz"},
		"new": true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deepMerge = %#v, want %#v", got, want)
	}
	// inputs not mutated
	if base["a"] != "base" || base["obj"].(map[string]any)["x"] != "bx" {
		t.Fatal("deepMerge mutated base")
	}
}

func TestDeepMergeNullDeletes(t *testing.T) {
	base := map[string]any{"cfg": map[string]any{"chartUrlPrefix": "https://x", "width": 800.0}}
	overlay := map[string]any{"cfg": map[string]any{"chartUrlPrefix": nil}}
	got := deepMerge(base, overlay)
	cfg := got["cfg"].(map[string]any)
	if _, exists := cfg["chartUrlPrefix"]; exists {
		t.Fatal("null overlay value must delete the key")
	}
	if cfg["width"] != 800.0 {
		t.Fatal("sibling keys must survive")
	}
}

func TestDeepMergeScalarOverArray(t *testing.T) {
	// non-map overlay values replace wholesale (arrays, scalars)
	base := map[string]any{"list": []any{"a", "b"}}
	overlay := map[string]any{"list": []any{"c"}}
	got := deepMerge(base, overlay)
	if !reflect.DeepEqual(got["list"], []any{"c"}) {
		t.Fatal("arrays must replace, not concatenate")
	}
}
