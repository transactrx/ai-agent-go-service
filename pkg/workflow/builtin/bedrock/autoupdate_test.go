package bedrock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

// TestParseModelID covers Bedrock inference-profile and foundation-model ID
// shapes: optional geo prefix, optional -YYYYMMDD date suffix, optional -vN:M
// suffix, single- and dual-number versions.
func TestParseModelID(t *testing.T) {
	cases := []struct {
		id      string
		want    parsedModel
		wantErr bool
	}{
		{id: "us.anthropic.claude-opus-4-7", want: parsedModel{prefix: "us.", family: "claude-opus", major: 4, minor: 7}},
		{id: "anthropic.claude-opus-4-1-20250805-v1:0", want: parsedModel{prefix: "", family: "claude-opus", major: 4, minor: 1}},
		{id: "anthropic.claude-opus-4-20250514-v1:0", want: parsedModel{prefix: "", family: "claude-opus", major: 4, minor: 0}},
		{id: "us.anthropic.claude-sonnet-4-5", want: parsedModel{prefix: "us.", family: "claude-sonnet", major: 4, minor: 5}},
		{id: "eu.anthropic.claude-opus-4-10", want: parsedModel{prefix: "eu.", family: "claude-opus", major: 4, minor: 10}},
		{id: "global.anthropic.claude-opus-5-20270101-v1:0", want: parsedModel{prefix: "global.", family: "claude-opus", major: 5, minor: 0}},
		{id: "anthropic.claude-3-5-sonnet-20241022-v2:0", wantErr: true}, // legacy naming: not auto-updatable
		{id: "meta.llama3-1-8b-instruct-v1:0", wantErr: true},
		{id: "", wantErr: true},
		{id: "us.anthropic.claude-opus", wantErr: true}, // no version
	}
	for _, c := range cases {
		got, err := parseModelID(c.id)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseModelID(%q): expected error, got %+v", c.id, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseModelID(%q): unexpected error: %v", c.id, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseModelID(%q) = %+v, want %+v", c.id, got, c.want)
		}
	}
}

// TestNewerThan verifies numeric (major, minor) comparison — in particular
// that 4-10 > 4-9 (string comparison would get this wrong).
func TestNewerThan(t *testing.T) {
	cases := []struct {
		a, b parsedModel
		want bool
	}{
		{parsedModel{major: 4, minor: 8}, parsedModel{major: 4, minor: 7}, true},
		{parsedModel{major: 4, minor: 7}, parsedModel{major: 4, minor: 7}, false},
		{parsedModel{major: 4, minor: 7}, parsedModel{major: 4, minor: 8}, false},
		{parsedModel{major: 4, minor: 10}, parsedModel{major: 4, minor: 9}, true},
		{parsedModel{major: 5, minor: 0}, parsedModel{major: 4, minor: 99}, true},
	}
	for _, c := range cases {
		if got := c.a.newerThan(c.b); got != c.want {
			t.Errorf("(%d-%d).newerThan(%d-%d) = %v, want %v", c.a.major, c.a.minor, c.b.major, c.b.minor, got, c.want)
		}
	}
}

// TestLatestCandidate verifies family/prefix filtering, strict-upgrade-only,
// numeric ordering across many candidates, and tolerance of garbage IDs.
func TestLatestCandidate(t *testing.T) {
	current := parsedModel{prefix: "us.", family: "claude-opus", major: 4, minor: 7}
	ids := []string{
		"us.anthropic.claude-opus-4-5",   // older
		"us.anthropic.claude-opus-4-8",   // newer
		"us.anthropic.claude-opus-4-10",  // newest in family+prefix
		"us.anthropic.claude-opus-4-9",   // newer but not max (4-10 > 4-9)
		"eu.anthropic.claude-opus-4-11",  // wrong prefix
		"us.anthropic.claude-sonnet-4-9", // wrong family
		"anthropic.claude-opus-4-12",     // wrong prefix (none)
		"meta.llama3-1-8b-instruct-v1:0", // garbage: ignored
	}
	got, ok := latestCandidate(ids, current)
	if !ok || got != "us.anthropic.claude-opus-4-10" {
		t.Fatalf("latestCandidate = %q, %v; want us.anthropic.claude-opus-4-10, true", got, ok)
	}

	// No strictly newer model → no candidate.
	if got, ok := latestCandidate([]string{"us.anthropic.claude-opus-4-7", "us.anthropic.claude-opus-4-6"}, current); ok {
		t.Fatalf("expected no candidate, got %q", got)
	}

	// Empty list → no candidate.
	if _, ok := latestCandidate(nil, current); ok {
		t.Fatal("expected no candidate for empty list")
	}
}

// TestAutoUpdateEnabled: absent → true (spec default); explicit false → off;
// explicit true → on. Exercises real JSON unmarshal like the loader does.
func TestAutoUpdateEnabled(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{`{"model":"us.anthropic.claude-opus-4-7"}`, true},
		{`{"model":"us.anthropic.claude-opus-4-7","autoUpdate":true}`, true},
		{`{"model":"us.anthropic.claude-opus-4-7","autoUpdate":false}`, false},
	}
	for _, c := range cases {
		var cfg Config
		if err := json.Unmarshal([]byte(c.raw), &cfg); err != nil {
			t.Fatalf("unmarshal %s: %v", c.raw, err)
		}
		if got := cfg.autoUpdateEnabled(); got != c.want {
			t.Errorf("autoUpdateEnabled for %s = %v, want %v", c.raw, got, c.want)
		}
	}
}

// TestModelSwapConcurrent exercises currentModel/setModel under the race
// detector (run with -race). In-flight requests read the model once at start;
// the updater is the only writer.
func TestModelSwapConcurrent(t *testing.T) {
	b := &bedrockLLM{model: "us.anthropic.claude-opus-4-7"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			b.setModel("us.anthropic.claude-opus-4-8")
		}
	}()
	for i := 0; i < 1000; i++ {
		if m := b.currentModel(); m == "" {
			t.Fatal("currentModel returned empty")
		}
	}
	<-done
	if got := b.currentModel(); got != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("final model = %q", got)
	}
}

// TestNextRunAt verifies the next-02:00-local computation around the
// boundary. time.Date normalizes DST transitions, so date math is safe.
func TestNextRunAt(t *testing.T) {
	loc := time.FixedZone("test", -5*3600)
	cases := []struct {
		now  time.Time
		want time.Time
	}{
		// Before 02:00 → same day 02:00.
		{time.Date(2026, 6, 5, 1, 0, 0, 0, loc), time.Date(2026, 6, 5, 2, 0, 0, 0, loc)},
		// Exactly 02:00 → next day (strictly after now).
		{time.Date(2026, 6, 5, 2, 0, 0, 0, loc), time.Date(2026, 6, 6, 2, 0, 0, 0, loc)},
		// Afternoon → next day 02:00.
		{time.Date(2026, 6, 5, 14, 30, 0, 0, loc), time.Date(2026, 6, 6, 2, 0, 0, 0, loc)},
		// Month boundary.
		{time.Date(2026, 6, 30, 23, 59, 0, 0, loc), time.Date(2026, 7, 1, 2, 0, 0, 0, loc)},
	}
	for _, c := range cases {
		if got := nextRunAt(c.now); !got.Equal(c.want) {
			t.Errorf("nextRunAt(%v) = %v, want %v", c.now, got, c.want)
		}
	}
}

// newTestUpdater returns an updater wired to fakes plus pointers to observe
// state: current model, published events, validate-call and sleep counters.
func newTestUpdater() (*autoUpdater, *string, *[]noteEvent, *int, *int) {
	model := "us.anthropic.claude-opus-4-7"
	var events []noteEvent
	validateCalls, sleeps := 0, 0
	u := &autoUpdater{
		wfID:    "powerlineSearch",
		nodeID:  "bedrock1",
		current: func() string { return model },
		swap:    func(m string) { model = m },
		resolve: nil, // nil = legacy path (gateway disabled)
		list: func(context.Context) ([]string, error) {
			return []string{"us.anthropic.claude-opus-4-8"}, nil
		},
		validate: func(context.Context, string) error { validateCalls++; return nil },
		publish: func(_ string, data []byte) error {
			var ev noteEvent
			if err := json.Unmarshal(data, &ev); err != nil {
				return err
			}
			events = append(events, ev)
			return nil
		},
		subject: "trx.test.modelAutoUpdate",
		now:     time.Now,
		sleep:   func(context.Context, time.Duration) { sleeps++ },
	}
	return u, &model, &events, &validateCalls, &sleeps
}

// TestRunOnceUpgrade: candidate validates on attempt 2 → swap + "upgraded"
// event with attempts=2; one backoff sleep.
func TestRunOnceUpgrade(t *testing.T) {
	u, model, events, validateCalls, sleeps := newTestUpdater()
	u.validate = func(context.Context, string) error {
		*validateCalls++
		if *validateCalls == 1 {
			return errors.New("throttled")
		}
		return nil
	}
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("model = %q, want upgraded", *model)
	}
	if len(*events) != 1 {
		t.Fatalf("events = %d, want 1", len(*events))
	}
	ev := (*events)[0]
	if ev.Event != "upgraded" || ev.From != "us.anthropic.claude-opus-4-7" ||
		ev.To != "us.anthropic.claude-opus-4-8" || ev.Attempts != 2 ||
		ev.WorkflowID != "powerlineSearch" || ev.NodeID != "bedrock1" || ev.Timestamp == "" ||
		ev.Resolver != "fallback" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if *sleeps != 1 {
		t.Fatalf("sleeps = %d, want 1", *sleeps)
	}
}

// TestRunOnceDecline: validation fails 3× → no swap, "declined" event with
// attempts=3 + error text; two backoff sleeps (none after the last attempt).
func TestRunOnceDecline(t *testing.T) {
	u, model, events, validateCalls, sleeps := newTestUpdater()
	u.validate = func(context.Context, string) error {
		*validateCalls++
		return errors.New("AccessDeniedException")
	}
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	if *validateCalls != 3 || *sleeps != 2 {
		t.Fatalf("validateCalls=%d sleeps=%d, want 3 and 2", *validateCalls, *sleeps)
	}
	if len(*events) != 1 {
		t.Fatalf("events = %d, want 1", len(*events))
	}
	ev := (*events)[0]
	if ev.Event != "declined" || ev.Attempts != 3 || ev.Error == "" || ev.To != "us.anthropic.claude-opus-4-8" ||
		ev.Resolver != "fallback" {
		t.Fatalf("unexpected event: %+v", ev)
	}
}

// TestRunOnceGatewayUpgrade: gateway answer differs → validated and swapped
// without consulting the catalog scan; event tagged resolver=gateway.
func TestRunOnceGatewayUpgrade(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	listCalls := 0
	u.list = func(context.Context) ([]string, error) { listCalls++; return nil, nil }
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-9", nil }
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-9" {
		t.Fatalf("model = %q, want gateway answer", *model)
	}
	if listCalls != 0 {
		t.Fatalf("listCalls = %d, want 0 (gateway short-circuits the scan)", listCalls)
	}
	if *validateCalls != 1 {
		t.Fatalf("validateCalls = %d, want 1", *validateCalls)
	}
	if len(*events) != 1 || (*events)[0].Event != "upgraded" || (*events)[0].Resolver != "gateway" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// TestRunOnceGatewayFollowsRollback: the gateway is followed even to an OLDER
// release (org rollback) — the strictly-newer rule applies only to the scan.
func TestRunOnceGatewayFollowsRollback(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-5", nil }
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-5" {
		t.Fatalf("model = %q, want rollback followed", *model)
	}
	if len(*events) != 1 || (*events)[0].Resolver != "gateway" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// TestRunOnceGatewayAlreadyLatest: gateway answer == current → full no-op,
// scan not consulted.
func TestRunOnceGatewayAlreadyLatest(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	listCalls := 0
	u.list = func(context.Context) ([]string, error) { listCalls++; return nil, nil }
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-7", nil }
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-7" || *validateCalls != 0 || len(*events) != 0 || listCalls != 0 {
		t.Fatalf("model=%q validateCalls=%d events=%d listCalls=%d — expected full no-op",
			*model, *validateCalls, len(*events), listCalls)
	}
}

// TestRunOnceGatewayErrorFallsBack: gateway failure (timeout, no responder,
// error reply) degrades to the catalog scan; event tagged resolver=fallback.
func TestRunOnceGatewayErrorFallsBack(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	u.resolve = func(context.Context) (string, error) {
		return "", errors.New("nats: no responders available for request")
	}
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("model = %q, want fallback scan result", *model)
	}
	if len(*events) != 1 || (*events)[0].Resolver != "fallback" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// TestRunOnceGatewayDecline: gateway candidate that fails validation 3× is
// declined; current model kept; declined event tagged resolver=gateway.
func TestRunOnceGatewayDecline(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-9", nil }
	u.validate = func(context.Context, string) error {
		*validateCalls++
		return errors.New("ValidationException")
	}
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-7" || *validateCalls != 3 {
		t.Fatalf("model=%q validateCalls=%d, want unchanged and 3", *model, *validateCalls)
	}
	if len(*events) != 1 || (*events)[0].Event != "declined" || (*events)[0].Resolver != "gateway" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// TestRunOnceAlreadyLatest: no newer candidate → no validation, no event.
func TestRunOnceAlreadyLatest(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.list = func(context.Context) ([]string, error) {
		return []string{"us.anthropic.claude-opus-4-7", "us.anthropic.claude-opus-4-5"}, nil
	}
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-7" || *validateCalls != 0 || len(*events) != 0 {
		t.Fatalf("model=%q validateCalls=%d events=%d — expected full no-op", *model, *validateCalls, len(*events))
	}
}

// TestRunOnceListError: control-plane failure → warn-and-wait, no event.
func TestRunOnceListError(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.list = func(context.Context) ([]string, error) { return nil, errors.New("throttled") }
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-7" || *validateCalls != 0 || len(*events) != 0 {
		t.Fatalf("expected no-op on list error")
	}
}

// TestRunOnceUnparseableCurrent: configured model outside the modern naming →
// auto-update silently skips (logged), nothing breaks.
func TestRunOnceUnparseableCurrent(t *testing.T) {
	u, _, events, validateCalls, _ := newTestUpdater()
	cur := "anthropic.claude-3-5-sonnet-20241022-v2:0"
	u.current = func() string { return cur }
	u.runOnce(context.Background())
	if *validateCalls != 0 || len(*events) != 0 {
		t.Fatal("expected no-op for unparseable current model")
	}
}

// TestRunOnceNilPublish: log-only mode (no NATS) must not panic and must
// still swap.
func TestRunOnceNilPublish(t *testing.T) {
	u, model, _, _, _ := newTestUpdater()
	u.publish = nil
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("model = %q, want upgraded in log-only mode", *model)
	}
}

// TestRunStopsOnCancel: run() must exit promptly when its context is
// cancelled (Close path), after the immediate startup check.
func TestRunStopsOnCancel(t *testing.T) {
	u, _, _, validateCalls, _ := newTestUpdater()
	firstCheck := make(chan struct{})
	u.validate = func(context.Context, string) error {
		*validateCalls++
		select {
		case <-firstCheck:
		default:
			close(firstCheck)
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { u.run(ctx); close(done) }()

	// Wait for the startup check to begin, then cancel.
	select {
	case <-firstCheck:
	case <-time.After(2 * time.Second):
		t.Fatal("startup check did not run")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop on cancel")
	}
	if *validateCalls == 0 {
		t.Fatal("startup check did not run")
	}
}

// TestRunOnceAlwaysLogsSummary: EVERY run — upgrade, keep, or any error —
// must end with a summary log line carrying the outcome and the model in
// use (user requirement: model in use always traceable from logs).
func TestRunOnceAlwaysLogsSummary(t *testing.T) {
	scenarios := []struct {
		name        string
		mutate      func(u *autoUpdater)
		wantOutcome string
		wantModel   string
	}{
		{
			name:        "upgraded",
			mutate:      func(u *autoUpdater) {},
			wantOutcome: "outcome=upgraded",
			wantModel:   "modelInUse=us.anthropic.claude-opus-4-8",
		},
		{
			name: "declined",
			mutate: func(u *autoUpdater) {
				u.validate = func(context.Context, string) error { return errors.New("denied") }
			},
			wantOutcome: "outcome=declined",
			wantModel:   "modelInUse=us.anthropic.claude-opus-4-7",
		},
		{
			name: "already latest",
			mutate: func(u *autoUpdater) {
				u.list = func(context.Context) ([]string, error) {
					return []string{"us.anthropic.claude-opus-4-7"}, nil
				}
			},
			wantOutcome: "outcome=already-latest",
			wantModel:   "modelInUse=us.anthropic.claude-opus-4-7",
		},
		{
			name: "list failed",
			mutate: func(u *autoUpdater) {
				u.list = func(context.Context) ([]string, error) { return nil, errors.New("throttled") }
			},
			wantOutcome: "outcome=list-failed",
			wantModel:   "modelInUse=us.anthropic.claude-opus-4-7",
		},
		{
			name: "unparseable current",
			mutate: func(u *autoUpdater) {
				u.current = func() string { return "anthropic.claude-3-5-sonnet-20241022-v2:0" }
			},
			wantOutcome: "outcome=skipped-unparseable-model",
			wantModel:   "modelInUse=anthropic.claude-3-5-sonnet-20241022-v2:0",
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			u, _, _, _, _ := newTestUpdater()
			var buf bytes.Buffer
			u.logger = log.New(&buf, "", 0)
			sc.mutate(u)
			u.runOnce(context.Background())
			out := buf.String()
			if !strings.Contains(out, "autoupdate: run finished "+sc.wantOutcome) ||
				!strings.Contains(out, sc.wantModel) {
				t.Fatalf("summary log missing %q + %q in:\n%s", sc.wantOutcome, sc.wantModel, out)
			}
		})
	}
}
