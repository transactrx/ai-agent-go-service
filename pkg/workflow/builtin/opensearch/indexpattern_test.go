// pkg/workflow/builtin/opensearch/indexpattern_test.go
package opensearch

import "testing"

func TestCompileIndexPatternProdCpe(t *testing.T) {
	r, err := compileIndexPattern("prod.cpe-*")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cases := []struct {
		in   string
		want bool
	}{
		{"prod.cpe-2026-04-30", true},
		{"prod.cpe-2026-04", true},
		{"prod.cpe-2026-*", true},
		{"prod.cpe-*", true},
		{"prod.cpe-2026.04.30", true},
		{"prod.cpe-", false},                 // empty variable portion
		{"prod.cpe", false},                  // missing trailing dash
		{"events.eprescribe-2026-04-30", false},
		{"*", false},
		{"prod.cpe-2026/_search", false},     // path char
		{"prod.cpe-2026?expand=hidden", false}, // query-string injection
		{"prod.cpe-2026,2026", false},        // embedded comma
		{"prod.cpe- 2026", false},            // whitespace
	}
	for _, c := range cases {
		if got := r.MatchString(c.in); got != c.want {
			t.Errorf("MatchString(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestCompileIndexPatternDevCpe(t *testing.T) {
	r, err := compileIndexPattern("dev.cpe-*")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !r.MatchString("dev.cpe-2026-04-30") {
		t.Fatal("dev.cpe-2026-04-30 should match")
	}
	if r.MatchString("prod.cpe-2026-04-30") {
		t.Fatal("prod.cpe-2026-04-30 should NOT match dev.cpe-* pattern")
	}
}

func TestCompileIndexPatternLiteralNoWildcard(t *testing.T) {
	r, err := compileIndexPattern("events.eprescribe-current")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !r.MatchString("events.eprescribe-current") {
		t.Fatal("literal pattern must match itself")
	}
	if r.MatchString("events.eprescribe-current-extra") {
		t.Fatal("literal pattern must reject longer strings")
	}
}

func TestCompileIndexPatternRejectsMultipleWildcards(t *testing.T) {
	if _, err := compileIndexPattern("prod.*.cpe-*"); err == nil {
		t.Fatal("expected error for multi-wildcard pattern")
	}
}

func TestCompileIndexPatternRejectsEmpty(t *testing.T) {
	if _, err := compileIndexPattern(""); err == nil {
		t.Fatal("expected error for empty pattern")
	}
}

func TestValidateIndexPathBypassAttempts(t *testing.T) {
	r, _ := compileIndexPattern("prod.cpe-*")
	if err := validateIndexPath("prod.cpe-2026-04-30,*", r); err == nil {
		t.Fatal("comma-bareword-* bypass must be rejected")
	}
	if err := validateIndexPath("prod.cpe-*,events.eprescribe-*", r); err == nil {
		t.Fatal("cross-family bypass must be rejected")
	}
	if err := validateIndexPath("*", r); err == nil {
		t.Fatal("bare * must be rejected")
	}
	if err := validateIndexPath("", r); err == nil {
		t.Fatal("empty indexPath must be rejected")
	}
	if err := validateIndexPath("prod.cpe-2026-04-30,prod.cpe-2026-04-29", r); err != nil {
		t.Fatalf("legitimate multi-day list must pass: %v", err)
	}
	if err := validateIndexPath("prod.cpe-*", r); err != nil {
		t.Fatalf("workflow's own wildcard must pass: %v", err)
	}
}
