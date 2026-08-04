package bedrock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
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

// TestSameModel covers parsed comparison so the bare and dated forms of one
// release match; unparseable IDs fall back to string equality.
func TestSameModel(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"us.anthropic.claude-opus-4-8", "us.anthropic.claude-opus-4-8", true},
		// bare vs dated form of the same release
		{"us.anthropic.claude-opus-4-8", "us.anthropic.claude-opus-4-8-20260615-v1:0", true},
		{"us.anthropic.claude-opus-4-7", "us.anthropic.claude-opus-4-8", false},
		// different prefix = different model
		{"us.anthropic.claude-opus-4-8", "global.anthropic.claude-opus-4-8", false},
		// unparseable falls back to string equality
		{"arn:aws:bedrock:custom", "arn:aws:bedrock:custom", true},
		{"arn:aws:bedrock:custom", "us.anthropic.claude-opus-4-8", false},
	}
	for _, c := range cases {
		if got := sameModel(c.a, c.b); got != c.want {
			t.Errorf("sameModel(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
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

// TestApplyEnvPin: empty env must not pin; set env pins with trimming,
// disables updater.
func TestApplyEnvPin(t *testing.T) {
	b := &bedrockLLM{cfg: Config{Model: "us.anthropic.claude-opus-4-7"}, model: "us.anthropic.claude-opus-4-7"}

	t.Setenv(pinEnv, "")
	if b.applyEnvPin() {
		t.Fatal("empty env must not pin")
	}
	if b.currentModel() != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model changed on empty pin: %q", b.currentModel())
	}

	t.Setenv(pinEnv, "  us.anthropic.claude-sonnet-5  ")
	if !b.applyEnvPin() {
		t.Fatal("set env must pin")
	}
	if b.currentModel() != "us.anthropic.claude-sonnet-5" {
		t.Fatalf("model = %q, want trimmed pinned value", b.currentModel())
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

func TestSwapModelKeepsLastKnownGood(t *testing.T) {
	b := &bedrockLLM{model: "us.anthropic.claude-opus-4-7"}
	b.swapModel("us.anthropic.claude-opus-4-8")
	if b.currentModel() != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("model = %q", b.currentModel())
	}
	if b.lastKnownGoodModel() != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("lastKnownGood = %q, want displaced model", b.lastKnownGoodModel())
	}
	// Same-value swap must not clobber the fallback with a duplicate.
	b.swapModel("us.anthropic.claude-opus-4-8")
	if b.lastKnownGoodModel() != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("lastKnownGood = %q after no-op swap", b.lastKnownGoodModel())
	}
}

func TestRecoverModelNeverRecordsBrokenModel(t *testing.T) {
	b := &bedrockLLM{model: "us.anthropic.claude-opus-4-7"}
	b.swapModel("us.anthropic.claude-opus-4-8") // lkg = 4-7
	// 4-8 fails its health check; recovery promotes 4-9. The broken 4-8 must
	// NOT become the fallback; 4-7 stays.
	b.recoverModel("us.anthropic.claude-opus-4-9")
	if b.currentModel() != "us.anthropic.claude-opus-4-9" {
		t.Fatalf("model = %q", b.currentModel())
	}
	if b.lastKnownGoodModel() != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("lastKnownGood = %q, want 4-7 preserved", b.lastKnownGoodModel())
	}
	// Recovery back onto the stored fallback clears it (it is now current).
	b2 := &bedrockLLM{model: "us.anthropic.claude-opus-4-8", lastKnownGood: "us.anthropic.claude-opus-4-7"}
	b2.recoverModel("us.anthropic.claude-opus-4-7")
	if b2.lastKnownGoodModel() != "" {
		t.Fatalf("lastKnownGood = %q, want cleared", b2.lastKnownGoodModel())
	}
}

func TestNextRunAt(t *testing.T) {
	// Anchor is 07:00 UTC regardless of the input's zone.
	est := time.FixedZone("est", -5*3600)
	cases := []struct {
		now  time.Time
		want time.Time
	}{
		// Before 07:00 UTC → same day 07:00 UTC.
		{time.Date(2026, 6, 5, 6, 0, 0, 0, time.UTC), time.Date(2026, 6, 5, 7, 0, 0, 0, time.UTC)},
		// Exactly 07:00 UTC → next day (strictly after now).
		{time.Date(2026, 6, 5, 7, 0, 0, 0, time.UTC), time.Date(2026, 6, 6, 7, 0, 0, 0, time.UTC)},
		// Afternoon UTC → next day.
		{time.Date(2026, 6, 5, 14, 30, 0, 0, time.UTC), time.Date(2026, 6, 6, 7, 0, 0, 0, time.UTC)},
		// Local zone input: 03:00 EST = 08:00 UTC → next day 07:00 UTC.
		{time.Date(2026, 6, 5, 3, 0, 0, 0, est), time.Date(2026, 6, 6, 7, 0, 0, 0, time.UTC)},
		// Month boundary.
		{time.Date(2026, 6, 30, 23, 59, 0, 0, time.UTC), time.Date(2026, 7, 1, 7, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		if got := nextRunAt(c.now); !got.Equal(c.want) {
			t.Errorf("nextRunAt(%v) = %v, want %v", c.now, got, c.want)
		}
	}
}

func TestProductionJitterRange(t *testing.T) {
	for i := 0; i < 200; i++ {
		j := productionJitter()
		if j < 0 || j >= maxJitter {
			t.Fatalf("jitter %v out of [0, %v)", j, maxJitter)
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
		wfID:        "powerlineSearch",
		nodeID:      "bedrock1",
		current:     func() string { return model },
		swap:        func(m string) { model = m },
		recoverSwap: func(string) {}, // default no-op; tests override for recovery assertions
		resolve:     nil,             // nil = legacy path (gateway disabled)
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
// The always-on health check then also fails, but the fake's error is a plain
// error — INCONCLUSIVE (a throttle/timeout looks like this), so recovery is
// never attempted and no health-check-failed event is published: totals are
// validateCalls=3+3=6, sleeps=2+2=4, one event (declined), outcome
// health-check-inconclusive.
func TestRunOnceDecline(t *testing.T) {
	u, model, events, validateCalls, sleeps := newTestUpdater()
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called on an inconclusive health check") }
	u.validate = func(context.Context, string) error {
		*validateCalls++
		return errors.New("AccessDeniedException")
	}
	var buf bytes.Buffer
	u.logger = log.New(&buf, "", 0)
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	// Was 9/6 before error classification: the 3 recovery probes are gone.
	if *validateCalls != 6 || *sleeps != 4 {
		t.Fatalf("validateCalls=%d sleeps=%d, want 6 and 4", *validateCalls, *sleeps)
	}
	if len(*events) != 1 {
		t.Fatalf("events = %d, want 1 (declined only; inconclusive health check never alerts): %+v", len(*events), *events)
	}
	ev := (*events)[0]
	if ev.Event != "declined" || ev.Attempts != 3 || ev.Error == "" || ev.To != "us.anthropic.claude-opus-4-8" ||
		ev.Resolver != "fallback" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if !strings.Contains(buf.String(), "outcome=health-check-inconclusive") {
		t.Fatalf("summary log missing outcome=health-check-inconclusive:\n%s", buf.String())
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

	// +1 validate: the health check still probes the current model even
	// though the gateway confirmed no upgrade is available.
	if *model != "us.anthropic.claude-opus-4-7" || *validateCalls != 1 || len(*events) != 0 || listCalls != 0 {
		t.Fatalf("model=%q validateCalls=%d events=%d listCalls=%d — expected full no-op + 1 health-check validate",
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

// TestRunOnceGatewayDeclineFallsBackToScan: a declined gateway candidate must
// not end the cycle — the catalog scan still runs and, if it finds a
// (different) strictly-newer candidate, that candidate is validated and
// swapped in the SAME cycle. Two notify events: declined(gateway) then
// upgraded(fallback).
func TestRunOnceGatewayDeclineFallsBackToScan(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-9", nil }
	u.validate = func(_ context.Context, id string) error {
		*validateCalls++
		if id == "us.anthropic.claude-opus-4-9" {
			return errors.New("ValidationException")
		}
		return nil
	}
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("model = %q, want fallback candidate swapped in", *model)
	}
	if *validateCalls != 4 {
		t.Fatalf("validateCalls = %d, want 4 (3 failed for 4-9 + 1 ok for 4-8)", *validateCalls)
	}
	if len(*events) != 2 {
		t.Fatalf("events = %d, want 2: %+v", len(*events), *events)
	}
	if ev := (*events)[0]; ev.Event != "declined" || ev.Resolver != "gateway" || ev.To != "us.anthropic.claude-opus-4-9" {
		t.Fatalf("events[0] = %+v, want declined/gateway/4-9", ev)
	}
	if ev := (*events)[1]; ev.Event != "upgraded" || ev.Resolver != "fallback" || ev.To != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("events[1] = %+v, want upgraded/fallback/4-8", ev)
	}
}

// TestRunOnceGatewayDeclineNothingElse: gateway candidate declined and the
// scan finds nothing new → model unchanged, one declined event
// (resolver=gateway). The always-on health check then also fails (same
// always-failing validate fake) and, since a gateway candidate was already
// declined this cycle, recovery is skipped (re-resolving would just return
// the same declined answer): +3 validateCalls (health-check probes), one
// extra "health-check-failed" event, and the summary outcome becomes
// "health-check-failed" (overriding "declined"). The fake returns a real
// smithy ValidationException so the health-check failure is CONCLUSIVE — a
// plain error would now be classified inconclusive and skip the alert.
func TestRunOnceGatewayDeclineNothingElse(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.list = func(context.Context) ([]string, error) {
		return []string{"us.anthropic.claude-opus-4-7"}, nil // only current: no scan candidate
	}
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-9", nil }
	u.validate = func(context.Context, string) error {
		*validateCalls++
		return validationErr()
	}
	var buf bytes.Buffer
	u.logger = log.New(&buf, "", 0)
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-7" || *validateCalls != 6 {
		t.Fatalf("model=%q validateCalls=%d, want unchanged and 6", *model, *validateCalls)
	}
	if len(*events) != 2 || (*events)[0].Event != "declined" || (*events)[0].Resolver != "gateway" ||
		(*events)[1].Event != "health-check-failed" {
		t.Fatalf("unexpected events: %+v", *events)
	}
	if !strings.Contains(buf.String(), "outcome=health-check-failed") {
		t.Fatalf("summary log missing outcome=health-check-failed:\n%s", buf.String())
	}
}

// TestRunOnceGatewayDeclineScanAgrees: the scan's candidate is the same ID the
// gateway already had declined — it must NOT be re-validated: only 3 validate
// calls for the upgrade stage (the gateway attempt), 1 declined event. The
// always-on health check then also fails (same always-failing validate fake)
// and, since the gateway candidate was declined this cycle, recovery is
// skipped: +3 validateCalls (health-check probes of the current model), one
// extra "health-check-failed" event. Conclusive (smithy) error so the health
// check still alerts.
func TestRunOnceGatewayDeclineScanAgrees(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-8", nil } // == scan's candidate
	u.validate = func(context.Context, string) error {
		*validateCalls++
		return validationErr()
	}
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	if *validateCalls != 6 {
		t.Fatalf("validateCalls = %d, want 6 (3 for the declined upgrade, scan not re-validating + 3 for the failed health check)", *validateCalls)
	}
	if len(*events) != 2 || (*events)[0].Event != "declined" || (*events)[0].Resolver != "gateway" ||
		(*events)[1].Event != "health-check-failed" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// TestRunOnceGatewayCandidateUnparseableFallsBack: a gateway answer outside
// the modern parseable naming is rejected before validation — falls straight
// to the scan without ever validating the unparseable ID.
func TestRunOnceGatewayCandidateUnparseableFallsBack(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	validated := []string{}
	u.resolve = func(context.Context) (string, error) { return "anthropic.claude-3-5-sonnet-20241022-v2:0", nil }
	u.validate = func(_ context.Context, id string) error {
		*validateCalls++
		validated = append(validated, id)
		return nil
	}
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("model = %q, want fallback scan candidate swapped in", *model)
	}
	for _, id := range validated {
		if id == "anthropic.claude-3-5-sonnet-20241022-v2:0" {
			t.Fatalf("unparseable gateway candidate was validated: %v", validated)
		}
	}
	if len(*events) != 1 || (*events)[0].Event != "upgraded" || (*events)[0].Resolver != "fallback" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// TestRunOnceGatewayCandidateCrossFamilyFallsBack: a gateway answer in a
// different model family is rejected before validation — falls straight to
// the scan without ever validating the cross-family ID.
func TestRunOnceGatewayCandidateCrossFamilyFallsBack(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	validated := []string{}
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-haiku-4-5", nil }
	u.validate = func(_ context.Context, id string) error {
		*validateCalls++
		validated = append(validated, id)
		return nil
	}
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("model = %q, want fallback scan candidate swapped in", *model)
	}
	for _, id := range validated {
		if id == "us.anthropic.claude-haiku-4-5" {
			t.Fatalf("cross-family gateway candidate was validated: %v", validated)
		}
	}
	if len(*events) != 1 || (*events)[0].Event != "upgraded" || (*events)[0].Resolver != "fallback" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// TestRunOnceAlreadyLatest: no newer candidate → no upgrade-stage validation,
// no event. +1 validate: the always-on health check still probes the current
// model (default validate fake passes, so no event either).
func TestRunOnceAlreadyLatest(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.list = func(context.Context) ([]string, error) {
		return []string{"us.anthropic.claude-opus-4-7", "us.anthropic.claude-opus-4-5"}, nil
	}
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-7" || *validateCalls != 1 || len(*events) != 0 {
		t.Fatalf("model=%q validateCalls=%d events=%d — expected full no-op + 1 health-check validate", *model, *validateCalls, len(*events))
	}
}

// TestRunOnceListError: control-plane failure → warn-and-wait, no event.
// +1 validate: the health check still runs after a list-failed upgrade stage
// (default validate fake passes, so the model stays healthy and unchanged).
func TestRunOnceListError(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.list = func(context.Context) ([]string, error) { return nil, errors.New("throttled") }
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-7" || *validateCalls != 1 || len(*events) != 0 {
		t.Fatalf("expected no-op + 1 health-check validate on list error")
	}
}

// TestRunOnceUnparseableCurrent: configured model outside the modern naming →
// auto-update silently skips the upgrade stage (logged), nothing breaks.
// +1 validate: the health check still probes the (unparseable) current model
// — the default fake passes, so it stays healthy; recovery is impossible for
// unparseable models but is never reached here since validate succeeds.
func TestRunOnceUnparseableCurrent(t *testing.T) {
	u, _, events, validateCalls, _ := newTestUpdater()
	cur := "anthropic.claude-3-5-sonnet-20241022-v2:0"
	u.current = func() string { return cur }
	u.runOnce(context.Background())
	if *validateCalls != 1 || len(*events) != 0 {
		t.Fatal("expected no-op + 1 health-check validate for unparseable current model")
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
			// The always-failing validate fake also fails the always-on
			// health check, but with a plain error — inconclusive, so the
			// final outcome is "health-check-inconclusive" (overriding the
			// upgrade stage's "declined") rather than "health-check-failed":
			// same summary-log guarantee, updated outcome.
			name: "declined",
			mutate: func(u *autoUpdater) {
				u.validate = func(context.Context, string) error { return errors.New("denied") }
			},
			wantOutcome: "outcome=health-check-inconclusive",
			wantModel:   "modelInUse=us.anthropic.claude-opus-4-7",
		},
		{
			// Same scenario with a conclusive (smithy) error: recovery is
			// attempted, finds nothing (scan candidate already declined), and
			// the cycle ends health-check-failed.
			name: "health check failed",
			mutate: func(u *autoUpdater) {
				u.validate = func(context.Context, string) error { return validationErr() }
			},
			wantOutcome: "outcome=health-check-failed",
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

// TestVerifyToolProbe covers the probe's pass/fail criteria over decoded
// LLMEvent sequences: a completed tool_use block named probeToolName with
// valid non-empty JSON input is the only passing shape.
func TestVerifyToolProbe(t *testing.T) {
	cases := []struct {
		name    string
		events  []node.LLMEvent
		wantErr string // substring; "" means nil error
	}{
		{
			name: "valid echo tool_use stop",
			events: []node.LLMEvent{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{Name: "echo", InputJSON: json.RawMessage(`{"value":"pong"}`)}},
			},
			wantErr: "",
		},
		{
			name: "wrong tool name",
			events: []node.LLMEvent{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{Name: "notecho", InputJSON: json.RawMessage(`{"value":"pong"}`)}},
			},
			wantErr: "notecho",
		},
		{
			name: "empty input JSON",
			events: []node.LLMEvent{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{Name: "echo", InputJSON: json.RawMessage(``)}},
			},
			wantErr: "not valid JSON",
		},
		{
			name: "invalid input JSON",
			events: []node.LLMEvent{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{Name: "echo", InputJSON: json.RawMessage(`{"value":`)}},
			},
			wantErr: "not valid JSON",
		},
		{
			name: "text only, no tool_use",
			events: []node.LLMEvent{
				{Kind: node.LLMTextDelta, Delta: "hi"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
			wantErr: "tool_use",
		},
		{
			name: "start/delta but no stop — incomplete block never verified",
			events: []node.LLMEvent{
				{Kind: node.LLMToolUseStart, ToolUse: &node.LLMToolUse{Name: "echo"}},
				{Kind: node.LLMToolUseDelta, ToolUse: &node.LLMToolUse{Name: "echo", InputJSON: json.RawMessage(`{"value":"pong"}`)}},
			},
			wantErr: "tool_use",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := verifyToolProbe(c.events)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("verifyToolProbe: unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("verifyToolProbe: expected error containing %q, got nil", c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("verifyToolProbe error = %q, want substring %q", err.Error(), c.wantErr)
			}
		})
	}
}

// TestProbeDecodeRoundTrip proves the probe's pass criterion is satisfiable by
// the exact production decoder: a realistic synthetic Anthropic chunk
// sequence (tool_use content_block_start → two input_json_delta chunks →
// content_block_stop → message_delta stop_reason=tool_use) fed through
// handleAnthropicChunk — the same function production streaming uses — then
// verified by verifyToolProbe.
func TestProbeDecodeRoundTrip(t *testing.T) {
	chunks := []string{
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"echo","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"value\":"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"pong\"}"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use"}}`,
	}

	out := make(chan node.LLMEvent, 64)
	var events []node.LLMEvent
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range out {
			events = append(events, ev)
		}
	}()

	accum := map[int]*node.LLMToolUse{}
	ctx := context.Background()
	for _, c := range chunks {
		if err := handleAnthropicChunk(ctx, []byte(c), accum, out); err != nil {
			t.Fatalf("handleAnthropicChunk: %v", err)
		}
	}
	close(out)
	<-done

	if err := verifyToolProbe(events); err != nil {
		t.Fatalf("verifyToolProbe: %v", err)
	}
}

// Health check passes → nothing new: outcome stays already-latest, exactly
// one extra validate call (the current-model probe), no events.
func TestRunOnceHealthCheckHealthy(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called") }
	u.list = func(context.Context) ([]string, error) { return nil, nil } // no candidates
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	if *validateCalls != 1 { // health check only
		t.Fatalf("validateCalls = %d, want 1", *validateCalls)
	}
	if len(*events) != 0 {
		t.Fatalf("events = %d, want 0", len(*events))
	}
}

// Current model fails its health check CONCLUSIVELY (smithy
// ValidationException: the model is gone/unentitled); gateway re-resolve
// returns the same broken model (rejected via sameModel); catalog scan
// provides 4-8, which validates → recoverSwap, outcome recovered, "recovered"
// event. Only conclusive failures may swap live traffic.
func TestRunOnceHealthCheckRecoversOnConclusiveError(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	recovered := ""
	u.recoverSwap = func(m string) { recovered = m; *model = m }
	u.resolve = func(context.Context) (string, error) { return *model, nil } // "confirms" current
	u.validate = func(_ context.Context, id string) error {
		if id == "us.anthropic.claude-opus-4-7" {
			return validationErr()
		}
		return nil
	}
	u.runOnce(context.Background())
	if recovered != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("recovered = %q, want 4-8", recovered)
	}
	if len(*events) != 1 || (*events)[0].Event != "recovered" ||
		(*events)[0].From != "us.anthropic.claude-opus-4-7" ||
		(*events)[0].To != "us.anthropic.claude-opus-4-8" ||
		(*events)[0].Resolver != "recovery" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// A gateway candidate already declined this cycle → recovery is skipped
// (re-resolving would return the same declined answer): outcome
// health-check-failed, two events (declined, health-check-failed). The fake
// returns a conclusive (smithy) error so the scenario still reaches
// failHealthCheck — a plain "everything fails" error is now classified
// inconclusive and would stop before the alert.
func TestRunOnceHealthCheckFailedAfterDecline(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called") }
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-8", nil }
	u.validate = func(_ context.Context, _ string) error { return validationErr() }
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	if len(*events) != 2 || (*events)[0].Event != "declined" || (*events)[1].Event != "health-check-failed" {
		t.Fatalf("unexpected events: %+v", *events)
	}
	if (*events)[1].From != "us.anthropic.claude-opus-4-7" || (*events)[1].To != "" || (*events)[1].Error == "" {
		t.Fatalf("bad health-check-failed payload: %+v", (*events)[1])
	}
}

// No recovery candidate anywhere (gateway nil, catalog empty) → outcome
// health-check-failed, single event. Conclusive (smithy) error so
// failHealthCheck is still exercised.
func TestRunOnceHealthCheckFailedNoCandidate(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called") }
	u.list = func(context.Context) ([]string, error) { return nil, nil }
	u.validate = func(_ context.Context, _ string) error { return validationErr() }
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	if len(*events) != 1 || (*events)[0].Event != "health-check-failed" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// After a validated upgrade the health check is skipped entirely (the model
// in use was probed seconds ago): validate calls = candidate probes only.
func TestRunOnceHealthCheckSkippedAfterUpgrade(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called") }
	u.runOnce(context.Background())
	if *model != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("model = %q, want upgraded", *model)
	}
	if *validateCalls != 1 { // one candidate probe, zero health-check probes
		t.Fatalf("validateCalls = %d, want 1", *validateCalls)
	}
	if len(*events) != 1 || (*events)[0].Event != "upgraded" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// TestHealthCheckConclusive: only a missing/unentitled model (smithy
// ValidationException / ResourceNotFoundException) or a tool-contract
// violation indicts the model. Throttles, timeouts and plain transport errors
// are inconclusive — they must never trigger recovery or an alert.
func TestHealthCheckConclusive(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"model gone", validationErr(), true},
		{"model not found", &smithy.GenericAPIError{Code: "ResourceNotFoundException"}, true},
		{"tool contract violation", fmt.Errorf("%w: no tool_use block", errProbeContract), true},
		{"throttled", &smithy.GenericAPIError{Code: "ThrottlingException"}, false},
		{"timeout", context.DeadlineExceeded, false},
		{"plain transport error", errors.New("connection reset by peer"), false},
	}
	for _, c := range cases {
		if got := healthCheckConclusive(c.err); got != c.want {
			t.Errorf("healthCheckConclusive(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// A transient health-check failure (throttle) must NOT be treated as "model
// broken": no recovery probes, no events, model untouched, outcome
// health-check-inconclusive. Gateway confirms the current model, so the only
// validate calls are the 3 health-check probes.
func TestRunOnceHealthCheckInconclusiveOnTransientError(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called on a transient error") }
	u.swap = func(string) { t.Fatal("swap must not be called on a transient error") }
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-7", nil }
	u.validate = func(context.Context, string) error {
		*validateCalls++
		return errors.New("ThrottlingException: rate exceeded")
	}
	var buf bytes.Buffer
	u.logger = log.New(&buf, "", 0)
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	if *validateCalls != 3 {
		t.Fatalf("validateCalls = %d, want 3 (health-check probes only; no recovery probes)", *validateCalls)
	}
	if len(*events) != 0 {
		t.Fatalf("events = %d, want 0 (a throttle must not alert): %+v", len(*events), *events)
	}
	out := buf.String()
	if !strings.Contains(out, "health check inconclusive for current model") ||
		!strings.Contains(out, "outcome=health-check-inconclusive") {
		t.Fatalf("missing inconclusive log lines:\n%s", out)
	}
}

// B2: a model that conclusively failed its health check must never be
// recorded as last-known-good, even by a LATER cycle's upgrade. Cycle 1:
// gateway confirms current, current fails conclusively, no recovery candidate
// → health-check-failed (unhealthiness latched). Cycle 2 on the SAME updater:
// gateway offers 4-8, it validates → promoted via recoverSwap, not swap.
func TestUpgradeAfterConclusiveFailureUsesRecoverSwap(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	swapped, recovered := "", ""
	u.swap = func(m string) { swapped = m; *model = m }
	u.recoverSwap = func(m string) { recovered = m; *model = m }
	u.list = func(context.Context) ([]string, error) { return nil, nil } // no catalog recovery candidate
	gatewayAnswer := "us.anthropic.claude-opus-4-7"                      // cycle 1: confirms current
	u.resolve = func(context.Context) (string, error) { return gatewayAnswer, nil }
	broken := true
	u.validate = func(context.Context, string) error {
		if broken {
			return validationErr() // conclusive
		}
		return nil
	}

	u.runOnce(context.Background()) // cycle 1
	if len(*events) != 1 || (*events)[0].Event != "health-check-failed" {
		t.Fatalf("cycle 1 events = %+v, want one health-check-failed", *events)
	}
	if !u.curUnhealthy {
		t.Fatal("cycle 1 did not latch curUnhealthy")
	}

	broken = false
	gatewayAnswer = "us.anthropic.claude-opus-4-8" // cycle 2: an upgrade appears
	u.runOnce(context.Background())                // cycle 2

	if recovered != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("recovered = %q, want the upgrade promoted via recoverSwap", recovered)
	}
	if swapped != "" {
		t.Fatalf("swap was used (%q): the broken model would have become last-known-good", swapped)
	}
	if u.curUnhealthy {
		t.Fatal("curUnhealthy not cleared after the upgrade")
	}
	if len(*events) != 2 || (*events)[1].Event != "upgraded" {
		t.Fatalf("cycle 2 events = %+v, want an upgraded event", *events)
	}
}

// B3: context cancelled during the LAST validation attempt is a shutdown, not
// a verdict — outcome cancelled, no declined event, no health check.
func TestRunOnceCancelledDuringFinalAttempt(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.validate = func(context.Context, string) error {
		*validateCalls++
		if *validateCalls == validationAttempts {
			cancel() // shutdown lands while the final probe is in flight
		}
		return errors.New("connection reset by peer")
	}
	var buf bytes.Buffer
	u.logger = log.New(&buf, "", 0)
	u.runOnce(ctx)

	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	if *validateCalls != validationAttempts {
		t.Fatalf("validateCalls = %d, want %d (health check must not run)", *validateCalls, validationAttempts)
	}
	if len(*events) != 0 {
		t.Fatalf("events = %d, want 0 (cancellation must not publish declined/health-check-failed): %+v", len(*events), *events)
	}
	if !strings.Contains(buf.String(), "outcome=cancelled") {
		t.Fatalf("summary log missing outcome=cancelled:\n%s", buf.String())
	}
}

// Recovery when the GATEWAY ITSELF errors (not "confirms current"): the
// catalog scan must still supply the replacement. The first scan (upgrade
// stage) is throttled so no upgrade happens and the health check runs; the
// recovery re-scan succeeds and offers 4-8.
func TestRecoveryGatewayErrorFallsToCatalog(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	recovered := ""
	u.recoverSwap = func(m string) { recovered = m; *model = m }
	u.swap = func(string) { t.Fatal("swap must not be called: recovery uses recoverSwap") }
	u.resolve = func(context.Context) (string, error) { return "", errors.New("gateway down") }
	listCalls := 0
	u.list = func(context.Context) ([]string, error) {
		listCalls++
		if listCalls == 1 {
			return nil, errors.New("throttled")
		}
		return []string{"us.anthropic.claude-opus-4-8"}, nil
	}
	u.validate = func(_ context.Context, id string) error {
		if id == "us.anthropic.claude-opus-4-7" {
			return validationErr() // conclusive: current model is gone
		}
		return nil
	}
	u.runOnce(context.Background())

	if recovered != "us.anthropic.claude-opus-4-8" || *model != "us.anthropic.claude-opus-4-8" {
		t.Fatalf("recovered = %q model = %q, want 4-8 via the catalog scan", recovered, *model)
	}
	if len(*events) != 1 || (*events)[0].Event != "recovered" || (*events)[0].Resolver != "recovery" {
		t.Fatalf("unexpected events: %+v", *events)
	}
}

// B4: the gateway answering with the DATED form of the current release is the
// current model, not a candidate — no probe of it, no swap, no event; just the
// health check's single probe.
func TestSameModelUsedForAlreadyLatest(t *testing.T) {
	u, model, events, validateCalls, _ := newTestUpdater()
	u.swap = func(string) { t.Fatal("swap must not be called: the gateway named the current release") }
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called") }
	listCalls := 0
	u.list = func(context.Context) ([]string, error) { listCalls++; return nil, nil }
	probed := []string{}
	u.validate = func(_ context.Context, id string) error {
		*validateCalls++
		probed = append(probed, id)
		return nil
	}
	u.resolve = func(context.Context) (string, error) {
		return "us.anthropic.claude-opus-4-7-20260101-v1:0", nil // dated form of current
	}
	var buf bytes.Buffer
	u.logger = log.New(&buf, "", 0)
	u.runOnce(context.Background())

	if *model != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("model = %q, want unchanged", *model)
	}
	if listCalls != 0 {
		t.Fatalf("listCalls = %d, want 0 (gateway confirmed current, scan not consulted)", listCalls)
	}
	if len(probed) != 1 || probed[0] != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("probed = %v, want only the health check of the current model", probed)
	}
	if len(*events) != 0 {
		t.Fatalf("events = %d, want 0: %+v", len(*events), *events)
	}
	if !strings.Contains(buf.String(), "outcome=already-latest") {
		t.Fatalf("summary log missing outcome=already-latest:\n%s", buf.String())
	}
}

// fakeHostLookup is a minimal hostLookup for TestNatsConn — no need for the
// full node.NodeEnv surface.
type fakeHostLookup map[string]any

func (f fakeHostLookup) Host(kind string) (any, bool) {
	v, ok := f[kind]
	return v, ok
}

// TestNatsConn covers the three natsConn outcomes: missing host, host of the
// wrong type, and a real *nats_service.NatService yielding a live conn.
// hostLookup narrows the parameter so this doesn't need a full node.NodeEnv.
func TestNatsConn(t *testing.T) {
	t.Run("missing nats host", func(t *testing.T) {
		if got := natsConn(fakeHostLookup{}); got != nil {
			t.Fatalf("natsConn = %v, want nil", got)
		}
	})

	t.Run("host of wrong type", func(t *testing.T) {
		if got := natsConn(fakeHostLookup{"nats": "not-a-nat-service"}); got != nil {
			t.Fatalf("natsConn = %v, want nil", got)
		}
	})

	t.Run("real NatService", func(t *testing.T) {
		srv, _ := runEmbeddedNATS(t)
		ns, err := nats_service.NewLowLevel("trx.test.agent", "q", srv.ClientURL(), "", "", 2048, 300*1024)
		if err != nil {
			t.Fatalf("NewLowLevel: %v", err)
		}
		// Deliberately not closing ns's connection: nats-service's
		// ClosedHandler calls os.Exit(-1) on any connection close (including
		// a shutting-down embedded server), which would kill the whole test
		// binary. The embedded server's own t.Cleanup (registered by
		// runEmbeddedNATS) tearing down the process is enough for this test.

		got := natsConn(fakeHostLookup{"nats": ns})
		if got == nil {
			t.Fatal("natsConn = nil, want a live conn")
		}
	})
}

// TestNewGatewayResolveEndToEnd exercises the resolver newGatewayResolve
// builds: it derives lab/family from current() on every call, then hits the
// gateway over a real NATS request/reply.
func TestNewGatewayResolveEndToEnd(t *testing.T) {
	t.Run("round trip", func(t *testing.T) {
		_, nc := runEmbeddedNATS(t)
		const subject = "trx.test.gatewayresolve"

		var sawFamily string
		sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
			var req map[string]string
			if err := json.Unmarshal(m.Data, &req); err != nil {
				t.Errorf("responder: bad request body: %v", err)
				return
			}
			sawFamily = req["family"]
			reply := &nats.Msg{
				Subject: m.Reply,
				Header:  nats.Header{nats_service_common.STATUS: []string{"200"}},
				Data:    []byte(`{"modelId":"anthropic.claude-opus-4-8","invokeId":"us.anthropic.claude-opus-4-8"}`),
			}
			if err := m.RespondMsg(reply); err != nil {
				t.Errorf("responder: RespondMsg: %v", err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		resolve := newGatewayResolve(nc, subject, func() string { return "us.anthropic.claude-opus-4-7" })
		got, err := resolve(context.Background())
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if got != "us.anthropic.claude-opus-4-8" {
			t.Fatalf("resolve = %q, want %q", got, "us.anthropic.claude-opus-4-8")
		}
		if sawFamily != "claude-opus" {
			t.Fatalf("responder saw family %q, want %q", sawFamily, "claude-opus")
		}
	})

	t.Run("unparseable current skips the request", func(t *testing.T) {
		_, nc := runEmbeddedNATS(t)
		const subject = "trx.test.gatewayresolve.unparseable"

		hit := false
		sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
			hit = true
			_ = m.Respond(nil)
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		resolve := newGatewayResolve(nc, subject, func() string { return "anthropic.claude-3-5-sonnet-20241022-v2:0" })
		_, err = resolve(context.Background())
		if err == nil {
			t.Fatal("resolve: expected error for unparseable current model")
		}
		if hit {
			t.Fatal("responder was hit despite unparseable current model")
		}
	})
}
