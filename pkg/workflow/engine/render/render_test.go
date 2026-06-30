package render_test

import (
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/render"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func ctxAt(yyyy int, mo time.Month, dd, hh, mm int) node.RenderCtx {
	return node.RenderCtx{
		Now:       time.Date(yyyy, mo, dd, hh, mm, 0, 0, time.UTC),
		SessionID: "sess-1",
		UserID:    "user-9f",
		RequestID: "req-abc",
	}
}

func TestNowUtcDefault(t *testing.T) {
	r := render.NewRenderer()
	out, err := r.Render("at {{nowUtc}}", ctxAt(2026, 5, 6, 14, 32))
	if err != nil {
		t.Fatal(err)
	}
	if out != "at 2026-05-06 14:32" {
		t.Fatalf("got %q", out)
	}
}

func TestNowUtcCustomFormat(t *testing.T) {
	r := render.NewRenderer()
	out, err := r.Render("{{nowUtc:2006-01-02}}", ctxAt(2026, 5, 6, 14, 32))
	if err != nil {
		t.Fatal(err)
	}
	if out != "2026-05-06" {
		t.Fatalf("got %q", out)
	}
}

func TestNowUtcDateAndWeekday(t *testing.T) {
	r := render.NewRenderer()
	out, err := r.Render("{{nowUtcDate}} {{nowUtcWeekday}}", ctxAt(2026, 5, 6, 14, 32))
	if err != nil {
		t.Fatal(err)
	}
	if out != "2026-05-06 Wednesday" {
		t.Fatalf("got %q", out)
	}
}

func TestSessionUserRequest(t *testing.T) {
	r := render.NewRenderer()
	out, err := r.Render("{{sessionId}}/{{userId}}/{{requestId}}", ctxAt(2026, 5, 6, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if out != "sess-1/user-9f/req-abc" {
		t.Fatalf("got %q", out)
	}
}

func TestTriggerJSONPath(t *testing.T) {
	r := render.NewRenderer()
	c := ctxAt(2026, 5, 6, 0, 0)
	c.TriggerEvent = map[string]any{"message": "hello"}
	out, err := r.Render("Q: {{trigger.message}}", c)
	if err != nil {
		t.Fatal(err)
	}
	if out != "Q: hello" {
		t.Fatalf("got %q", out)
	}
}

func TestNodesJSONPath(t *testing.T) {
	r := render.NewRenderer()
	c := ctxAt(2026, 5, 6, 0, 0)
	c.NodeOutputs = map[string]any{"n1": map[string]any{"x": 42}}
	out, err := r.Render("v={{nodes.n1.x}}", c)
	if err != nil {
		t.Fatal(err)
	}
	if out != "v=42" {
		t.Fatalf("got %q", out)
	}
}

func TestUnknownPlaceholderError(t *testing.T) {
	r := render.NewRenderer()
	_, err := r.Render("{{whatIsThis}}", ctxAt(2026, 1, 1, 0, 0))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "whatIsThis") {
		t.Fatalf("err missing name: %v", err)
	}
}

func TestMissingTriggerKeyError(t *testing.T) {
	r := render.NewRenderer()
	c := ctxAt(2026, 1, 1, 0, 0)
	c.TriggerEvent = map[string]any{"message": "hi"}
	_, err := r.Render("{{trigger.missing}}", c)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateCatchesUnknownPlaceholder(t *testing.T) {
	r := render.NewRenderer()
	if err := r.Validate("{{whatever}}"); err == nil {
		t.Fatal("expected unknown-placeholder error from Validate")
	}
	if err := r.Validate("{{nowUtc}}"); err != nil {
		t.Fatalf("known placeholder rejected: %v", err)
	}
}

func TestNewPlaceholders(t *testing.T) {
	r := render.NewRenderer()
	fixed := time.Date(2026, 6, 18, 2, 30, 0, 0, time.UTC) // 22:30 ET on the 17th
	ctx := node.RenderCtx{Now: fixed, UserName: "Ada Lovelace", TimeZone: "America/New_York", IndexMapping: "{\"x\":1}"}

	cases := map[string]string{
		"{{userName}}":            "Ada Lovelace",
		"{{userTimeZone}}":        "America/New_York",
		"{{nowLocal}}":            "2026-06-17 22:30",
		"{{nowLocalWeekday}}":     "Wednesday",
		"{{indexMapping}}":        "{\"x\":1}",
		"{{nowLocal:2006-01-02}}": "2026-06-17",
	}
	for tmpl, want := range cases {
		got, err := r.Render(tmpl, ctx)
		if err != nil || got != want {
			t.Errorf("%s => %q (err %v), want %q", tmpl, got, err, want)
		}
	}

	empty := node.RenderCtx{Now: fixed} // no name, no tz, no mapping
	for tmpl, want := range map[string]string{
		"{{userName}}":     "the user",
		"{{userTimeZone}}": "UTC",
		"{{nowLocal}}":     "2026-06-18 02:30", // bad/empty tz -> UTC
		"{{indexMapping}}": "(index mapping temporarily unavailable)",
	} {
		got, _ := r.Render(tmpl, empty)
		if got != want {
			t.Errorf("fallback %s => %q, want %q", tmpl, got, want)
		}
	}

	if err := r.Validate("{{userName}} {{userTimeZone}} {{nowLocal}} {{nowLocalWeekday}} {{indexMapping}}"); err != nil {
		t.Errorf("Validate rejected new placeholders: %v", err)
	}

	// Invalid tz: nowLocal AND userTimeZone both fall back to UTC.
	bad := node.RenderCtx{Now: fixed, TimeZone: "Not/AZone"}
	for tmpl, want := range map[string]string{
		"{{userTimeZone}}": "UTC",
		"{{nowLocal}}":     "2026-06-18 02:30",
	} {
		got, _ := r.Render(tmpl, bad)
		if got != want {
			t.Errorf("invalid tz %s => %q, want %q", tmpl, got, want)
		}
	}

	// Winter date in EST (UTC-5, not EDT): verifies tz offset + weekday roll-back.
	winter := node.RenderCtx{Now: time.Date(2026, 1, 15, 2, 30, 0, 0, time.UTC), TimeZone: "America/New_York"}
	for tmpl, want := range map[string]string{
		"{{nowLocal}}":        "2026-01-14 21:30", // UTC-5
		"{{nowLocalWeekday}}": "Wednesday",        // local rolled back to the 14th
	} {
		got, err := r.Render(tmpl, winter)
		if err != nil || got != want {
			t.Errorf("winter %s => %q (err %v), want %q", tmpl, got, err, want)
		}
	}
}
