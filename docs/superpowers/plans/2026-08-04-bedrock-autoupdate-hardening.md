# Bedrock Auto-Update Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port powerlineAIApi's four operational-hardening behaviors (UTC+jitter schedule, current-model health check with recovery, last-known-good request-time fallback, env-pin break-glass) into the existing per-node auto-updater in `ai-agent-go-service/pkg/workflow/builtin/bedrock/`.

**Architecture:** All logic changes live in the bedrock builtin package, additive, keeping the injected-effects `autoUpdater` style so every new behavior is unit-testable without AWS/NATS. The consumer repo (`opensearchAiChatApi`) only gets docs, one terraform env var, and a dependency bump.

**Tech Stack:** Go 1.25, aws-sdk-go-v2 (bedrock/bedrockruntime, smithy-go errors), nats.go, stdlib testing.

**Spec:** `docs/superpowers/specs/2026-08-04-bedrock-autoupdate-hardening-design.md` (approved 2026-08-04).

## Global Constraints

- Library repo: `/Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service`, branch off `Development`. Consumer repo: `/Users/yceleiro/Documents/TransactRx/GitHub/opensearchAiChatApi`, branch `Development`.
- Commit per task is fine; **NEVER push or tag-push without explicit user approval** (pushes trigger deploys).
- Target library version: **v1.3.0** (tagging/pushing is an approval-gated step, Task 8).
- No changes to `Config` JSON shape, node contracts, engine, or existing NATS payload fields — only two new `noteEvent.Event` values (`recovered`, `health-check-failed`) and one new env var (`AI_BEDROCK_MODEL_ID`).
- Prime rule preserved: every updater failure keeps the current model; `runOnce` never returns an error.
- Schedule constants are code constants, not config: `anchorHourUTC = 7`, `maxJitter = 30 * time.Minute`, `validationAttempts = 3`, backoffs `5s/15s`.
- Run tests from the library repo root: `go test ./pkg/workflow/builtin/bedrock/ -race`.

---

### Task 0: Branch setup (library)

**Files:** none (git only)

- [ ] **Step 1: Create the feature branch**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service
git checkout Development && git pull --ff-only
git checkout -b feature/bedrock-autoupdate-hardening
```

- [ ] **Step 2: Verify baseline is green**

Run: `go test ./pkg/workflow/builtin/bedrock/ -race`
Expected: PASS (all existing tests).

---

### Task 1: Schedule — 07:00 UTC anchor + jitter

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/autoupdate.go` (lines 122-133 `checkHour`/`nextRunAt`, line 159-172 `autoUpdater` struct, lines 302-317 `run`, lines 343-358 `startAutoUpdate`)
- Test: `pkg/workflow/builtin/bedrock/autoupdate_test.go` (replace `TestNextRunAt` at lines 155-175; add `TestProductionJitterRange`, `TestRunUsesJitter` not needed — jitter is exercised via `nextRunAt` + injected field)

**Interfaces:**
- Consumes: existing `autoUpdater` struct, `run(ctx)`.
- Produces: `nextRunAt(now time.Time) time.Time` (now UTC-anchored), `productionJitter() time.Duration`, new struct field `jitter func() time.Duration` (nil-safe: nil = zero jitter). Task 4 and Task 8 rely on these names.

- [ ] **Step 1: Rewrite `TestNextRunAt` to expect the UTC anchor (failing test)**

Replace the body of `TestNextRunAt` (autoupdate_test.go:155-175) with:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/workflow/builtin/bedrock/ -run 'TestNextRunAt|TestProductionJitterRange' -v`
Expected: FAIL (`nextRunAt` still local-02:00; `productionJitter`/`maxJitter` undefined).

- [ ] **Step 3: Implement UTC anchor + jitter**

In `autoupdate.go`, replace lines 122-133 (`checkHour` + `nextRunAt`) with:

```go
// anchorHourUTC is the daily check time: 07:00 UTC — anchored to UTC (not
// server-local) so every region and container agrees on the cycle time
// regardless of host timezone (ported from powerlineAIApi).
const anchorHourUTC = 7

// maxJitter spreads wake-ups across containers/nodes so the gateway and
// Bedrock are not hit by a thundering herd at the anchor.
const maxJitter = 30 * time.Minute

// productionJitter draws a fresh random delay in [0, maxJitter) per cycle.
func productionJitter() time.Duration {
	return time.Duration(rand.Int63n(int64(maxJitter)))
}

// nextRunAt returns the next strictly-future occurrence of 07:00 UTC.
func nextRunAt(now time.Time) time.Time {
	utc := now.UTC()
	next := time.Date(utc.Year(), utc.Month(), utc.Day(), anchorHourUTC, 0, 0, 0, time.UTC)
	if !next.After(utc) {
		next = next.Add(24 * time.Hour)
	}
	return next
}
```

Add `"math/rand"` to the import block.

Add the jitter field to `autoUpdater` (after `sleep`, line ~171):

```go
	jitter   func() time.Duration // per-cycle random delay after the anchor; nil → none
```

In `run` (lines 304-317), compute the wake time with jitter:

```go
func (u *autoUpdater) run(ctx context.Context) {
	u.runOnce(ctx)
	for {
		delay := time.Duration(0)
		if u.jitter != nil {
			delay = u.jitter()
		}
		next := nextRunAt(u.now()).Add(delay)
		u.logf("autoupdate: next run at %s", next.Format(time.RFC3339))
		timer := time.NewTimer(next.Sub(u.now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			u.runOnce(ctx)
		}
	}
}
```

In `startAutoUpdate` (struct literal at lines 343-358), add:

```go
		jitter:   productionJitter,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/workflow/builtin/bedrock/ -race`
Expected: PASS (whole package — `TestRunStopsOnCancel` still passes because it drives `now`/`sleep`; if it asserts on the old log format or timer math, update its expectations to the new `next run at` line).

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/bedrock/autoupdate.go pkg/workflow/builtin/bedrock/autoupdate_test.go
git commit -m "feat(bedrock): anchor auto-update at 07:00 UTC with 0-30min jitter"
```

---

### Task 2: `sameModel` comparison helper

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/autoupdate.go` (next to `parseModelID`, after line 104)
- Test: `pkg/workflow/builtin/bedrock/autoupdate_test.go`

**Interfaces:**
- Produces: `sameModel(a, b string) bool` — parsed comparison (prefix/family/major/minor) so the bare and dated forms of one release match; unparseable IDs fall back to string equality. Task 4 uses it to reject recovery candidates equal to the broken model.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/workflow/builtin/bedrock/ -run TestSameModel -v`
Expected: FAIL — `sameModel` undefined.

- [ ] **Step 3: Implement**

Add after `latestCandidate` (autoupdate.go:104):

```go
// sameModel reports whether two IDs refer to the same model release: parsed
// comparison so the bare and dated forms of one release are not mistaken for
// different models. Unparseable IDs fall back to string equality.
func sameModel(a, b string) bool {
	pa, ea := parseModelID(a)
	pb, eb := parseModelID(b)
	if ea != nil || eb != nil {
		return a == b
	}
	return pa == pb
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/workflow/builtin/bedrock/ -run TestSameModel -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/bedrock/autoupdate.go pkg/workflow/builtin/bedrock/autoupdate_test.go
git commit -m "feat(bedrock): sameModel release comparison for recovery gating"
```

---

### Task 3: Last-known-good model state on `bedrockLLM`

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/factory.go` (struct at lines 63-77, setters at lines 87-92)
- Test: `pkg/workflow/builtin/bedrock/autoupdate_test.go`

**Interfaces:**
- Consumes: existing `modelMu sync.RWMutex`, `model string`, `currentModel()`, `setModel(m)` (kept unchanged — used by the env pin in Task 6 and existing tests).
- Produces (Task 4 and Task 5 rely on these exact names):
  - `swapModel(m string)` — validated-upgrade swap; demotes the displaced model into `lastKnownGood`.
  - `recoverModel(m string)` — health-check-recovery swap; never records the displaced (broken) model; clears a stale `lastKnownGood` equal to `m`.
  - `lastKnownGoodModel() string` — RLock accessor.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/workflow/builtin/bedrock/ -run 'TestSwapModel|TestRecoverModel' -v`
Expected: FAIL — `swapModel`/`recoverModel`/`lastKnownGoodModel`/`lastKnownGood` undefined.

- [ ] **Step 3: Implement**

In `factory.go`, extend the struct (add below `model string`, line 74):

```go
	// lastKnownGood is the model displaced by the last validated upgrade —
	// offered as a request-time fallback in Stream if the fresh model breaks
	// mid-day. In-memory only; reset on restart (startup runOnce re-resolves).
	lastKnownGood string
```

Add below `setModel` (line 92):

```go
// swapModel promotes m after a VALIDATED upgrade, keeping the displaced
// model as last-known-good for the request-time fallback.
func (b *bedrockLLM) swapModel(m string) {
	b.modelMu.Lock()
	if b.model != m {
		b.lastKnownGood = b.model
	}
	b.model = m
	b.modelMu.Unlock()
}

// recoverModel promotes m after the current model FAILED its health check.
// The broken model is never recorded as fallback; a stale fallback equal to
// m is cleared (it is current again, not a fallback).
func (b *bedrockLLM) recoverModel(m string) {
	b.modelMu.Lock()
	if b.lastKnownGood == m {
		b.lastKnownGood = ""
	}
	b.model = m
	b.modelMu.Unlock()
}

// lastKnownGoodModel returns the request-time fallback model ("" when none).
func (b *bedrockLLM) lastKnownGoodModel() string {
	b.modelMu.RLock()
	defer b.modelMu.RUnlock()
	return b.lastKnownGood
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/workflow/builtin/bedrock/ -race -run 'TestSwapModel|TestRecoverModel|TestModelSwapConcurrent' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/bedrock/factory.go pkg/workflow/builtin/bedrock/autoupdate_test.go
git commit -m "feat(bedrock): last-known-good model state with swap/recover semantics"
```

---

### Task 4: Health check of the current model + recovery in `runOnce`

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/autoupdate.go` (`autoUpdater` struct ~line 159, `runOnce` lines 181-239, `noteEvent` doc line 143, `startAutoUpdate` struct literal lines 343-358)
- Test: `pkg/workflow/builtin/bedrock/autoupdate_test.go` (new tests + expectation updates on existing `TestRunOnce*` tests)

**Interfaces:**
- Consumes: `sameModel` (Task 2), `swapModel`/`recoverModel` (Task 3), existing `validate`, `resolve`, `list`, `notify`, `tryUpgrade`, `validationAttempts`, `validationBackoffs`.
- Produces: new `autoUpdater` field `recoverSwap func(string)`; new outcomes `recovered`, `health-check-failed` in the summary line; new `noteEvent.Event` values `"recovered"` (Resolver `"recovery"`) and `"health-check-failed"` (`To` empty). Design refinement (documented in code comment): the health check is **skipped when this cycle just validated and swapped** (`upgraded`) — re-probing a model validated seconds ago is 3 redundant Bedrock calls.

- [ ] **Step 1: Write the failing tests**

Add to `autoupdate_test.go` (uses the existing `newTestUpdater` helper at line 179; set `u.recoverSwap` explicitly in each test):

```go
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

// Current model fails its health check; gateway re-resolve returns the same
// broken model (rejected via sameModel); catalog scan provides 4-8, which
// validates → recoverSwap, outcome recovered, "recovered" event.
func TestRunOnceHealthCheckRecovers(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	recovered := ""
	u.recoverSwap = func(m string) { recovered = m; *model = m }
	u.resolve = func(context.Context) (string, error) { return *model, nil } // "confirms" current
	u.validate = func(_ context.Context, id string) error {
		if id == "us.anthropic.claude-opus-4-7" {
			return errors.New("model deprecated by AWS")
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
// health-check-failed, two events (declined, health-check-failed).
func TestRunOnceHealthCheckFailedAfterDecline(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called") }
	u.resolve = func(context.Context) (string, error) { return "us.anthropic.claude-opus-4-8", nil }
	u.validate = func(_ context.Context, _ string) error { return errors.New("everything fails") }
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
// health-check-failed, single event.
func TestRunOnceHealthCheckFailedNoCandidate(t *testing.T) {
	u, model, events, _, _ := newTestUpdater()
	u.recoverSwap = func(string) { t.Fatal("recoverSwap must not be called") }
	u.list = func(context.Context) ([]string, error) { return nil, nil }
	u.validate = func(_ context.Context, _ string) error { return errors.New("model broken") }
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/workflow/builtin/bedrock/ -run 'TestRunOnceHealthCheck' -v`
Expected: FAIL — `recoverSwap` field undefined.

- [ ] **Step 3: Implement**

3a. `autoUpdater` struct: add below `swap` (line ~164):

```go
	recoverSwap func(string) // health-check-recovery setter (never records the broken model)
```

3b. Update the `noteEvent.Event` comment (line 143):

```go
	Event      string `json:"event"` // "upgraded" | "declined" | "recovered" | "health-check-failed"
```

3c. Restructure `runOnce` (lines 181-239). Rename the existing stage-1/stage-2 body into `resolveAndUpgrade` and append the health-check stage:

```go
// runOnce executes one full cycle: resolve → validate → swap (upgrade
// stages), then a health check of the model now in use with a one-shot
// recovery. Never returns an error: every failure mode is "stay on the
// current model and try again next cycle".
func (u *autoUpdater) runOnce(ctx context.Context) {
	outcome := "already-latest"
	defer func() {
		u.logf("autoupdate: run finished outcome=%s modelInUse=%s", outcome, u.current())
	}()

	declined := u.resolveAndUpgrade(ctx, &outcome)
	// Skip the health check when this cycle just validated the model in use
	// (upgraded): re-probing a model probed seconds ago is pure cost.
	if outcome == "upgraded" || outcome == "cancelled" || ctx.Err() != nil {
		return
	}
	u.healthCheck(ctx, declined, &outcome)
}

// resolveAndUpgrade is the pre-existing two-stage upgrade path (gateway
// first, catalog scan fallback). Returns the gateway candidate declined this
// cycle ("" when none) so recovery can avoid a pointless re-resolve.
func (u *autoUpdater) resolveAndUpgrade(ctx context.Context, outcome *string) (declined string) {
	cur := u.current()
	parsed, err := parseModelID(cur)
	if err != nil {
		*outcome = "skipped-unparseable-model"
		u.logf("autoupdate: skipped: %v", err)
		return ""
	}

	// Stage 1 — org inferenceGateway (source of truth; followed within the
	// family in either direction, gated by validation).
	if u.resolve != nil {
		switch id, rerr := u.resolve(ctx); {
		case rerr != nil:
			u.logf("autoupdate: gateway resolve failed (using catalog-scan fallback): %v", rerr)
		case id == cur:
			u.logf("autoupdate: gateway confirms current model %s is latest", cur)
			return ""
		default:
			if gerr := gatewayCandidateOK(id, parsed); gerr != nil {
				u.logf("autoupdate: gateway candidate rejected (using catalog-scan fallback): %v", gerr)
			} else if u.tryUpgrade(ctx, cur, id, "gateway", outcome) {
				return ""
			} else if *outcome == "cancelled" {
				return ""
			} else {
				declined = id
			}
		}
	}
	if ctx.Err() != nil {
		*outcome = "cancelled"
		return declined
	}

	// Stage 2 — Bedrock catalog scan, strictly newer within prefix+family.
	ids, lerr := u.list(ctx)
	if lerr != nil {
		*outcome = "list-failed"
		u.logf("autoupdate: list inference profiles failed (retry next cycle): %v", lerr)
		return declined
	}
	cand, ok := latestCandidate(ids, parsed)
	if !ok || cand == declined {
		return declined
	}
	u.tryUpgrade(ctx, cur, cand, "fallback", outcome)
	return declined
}

// healthCheck probes the model currently in use (the daily Bedrock-
// deprecation detector). On failure it attempts a one-shot recovery so a
// model AWS broke under us is replaced without operator action.
func (u *autoUpdater) healthCheck(ctx context.Context, declined string, outcome *string) {
	cur := u.current()
	var lastErr error
	for attempt := 1; attempt <= validationAttempts; attempt++ {
		if ctx.Err() != nil {
			*outcome = "cancelled"
			return
		}
		if lastErr = u.validate(ctx, cur); lastErr == nil {
			return // healthy — outcome untouched
		}
		u.logf("autoupdate: health check %d/%d of current model %s failed: %v", attempt, validationAttempts, cur, lastErr)
		if attempt < validationAttempts {
			u.sleep(ctx, validationBackoffs[attempt-1])
		}
	}
	if declined != "" {
		// Re-resolving would return the candidate already declined this
		// cycle — give up until tomorrow.
		u.failHealthCheck(cur, lastErr, outcome)
		return
	}
	u.recoverFromFailedHealthCheck(ctx, cur, lastErr, outcome)
}

// recoverFromFailedHealthCheck re-resolves (gateway first, catalog second),
// gates the candidate, probes it, and promotes via recoverSwap so the broken
// model is never recorded as fallback.
func (u *autoUpdater) recoverFromFailedHealthCheck(ctx context.Context, broken string, healthErr error, outcome *string) {
	u.logf("autoupdate: current model %s failed its health check, recovering", broken)
	cand := u.recoveryCandidate(ctx, broken)
	if cand == "" {
		u.failHealthCheck(broken, healthErr, outcome)
		return
	}
	var lastErr error
	for attempt := 1; attempt <= validationAttempts; attempt++ {
		if ctx.Err() != nil {
			*outcome = "cancelled"
			return
		}
		if lastErr = u.validate(ctx, cand); lastErr == nil {
			u.recoverSwap(cand)
			*outcome = "recovered"
			u.logf("autoupdate: model updated from %s to %s (resolver=recovery)", broken, cand)
			u.notify(noteEvent{Event: "recovered", From: broken, To: cand, Attempts: attempt, Resolver: "recovery"})
			return
		}
		u.logf("autoupdate: recovery validation %d/%d of %s failed: %v", attempt, validationAttempts, cand, lastErr)
		if attempt < validationAttempts {
			u.sleep(ctx, validationBackoffs[attempt-1])
		}
	}
	u.failHealthCheck(broken, lastErr, outcome)
}

// recoveryCandidate returns a replacement for a broken current model, or "".
// Gateway first (rejecting cross-family, unparseable, or same-release
// answers), catalog scan second. An unparseable broken model yields "" —
// nothing to gate against.
func (u *autoUpdater) recoveryCandidate(ctx context.Context, broken string) string {
	parsed, err := parseModelID(broken)
	if err != nil {
		return ""
	}
	if u.resolve != nil {
		if id, rerr := u.resolve(ctx); rerr != nil {
			u.logf("autoupdate: recovery gateway resolve failed: %v", rerr)
		} else if gerr := gatewayCandidateOK(id, parsed); gerr != nil {
			u.logf("autoupdate: recovery gateway candidate rejected: %v", gerr)
		} else if sameModel(id, broken) {
			u.logf("autoupdate: recovery gateway returned the broken model %s, ignoring", id)
		} else {
			return id
		}
	}
	ids, lerr := u.list(ctx)
	if lerr != nil {
		u.logf("autoupdate: recovery list inference profiles failed: %v", lerr)
		return ""
	}
	if cand, ok := latestCandidate(ids, parsed); ok && !sameModel(cand, broken) {
		return cand
	}
	return ""
}

// failHealthCheck records the terminal unhealthy state for this cycle.
func (u *autoUpdater) failHealthCheck(cur string, err error, outcome *string) {
	*outcome = "health-check-failed"
	u.logf("autoupdate: health check FAILED for current model %s: %v", cur, err)
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	u.notify(noteEvent{Event: "health-check-failed", From: cur, Attempts: validationAttempts, Error: msg})
}
```

3d. Wire in `startAutoUpdate` (struct literal): change `swap: b.setModel,` to `swap: b.swapModel,` and add `recoverSwap: b.recoverModel,`.

- [ ] **Step 4: Update existing `TestRunOnce*` expectations**

The always-on health check changes validate-call counts and event counts in existing tests whose outcome is not `upgraded`. Run:

`go test ./pkg/workflow/builtin/bedrock/ -run TestRunOnce -v`

and fix failures using these rules (the behavior change is intended):
- Outcome `upgraded` (e.g. `TestRunOnceUpgrade`, `TestRunOnceGatewayUpgrade`, `TestRunOnceGatewayFollowsRollback`): unchanged — health check skipped.
- Outcome `already-latest`/`list-failed`/`skipped-unparseable-model` with a **passing** validate fake (e.g. `TestRunOnceGatewayAlreadyLatest`, `TestRunOnceAlreadyLatest`, `TestRunOnceGatewayDeclineScanAgrees` variants where validate succeeds): `validateCalls` += 1 (the health-check probe); events unchanged. `TestRunOnceListError`: health check still runs after `list-failed` — validateCalls += 1. `TestRunOnceUnparseableCurrent`: health check runs and (fake validate passes) adds 1 call; recovery is impossible for unparseable models so nothing else changes.
- Outcome `declined` with an **always-failing** validate fake (e.g. `TestRunOnceDecline`, `TestRunOnceGatewayDeclineNothingElse`): health check also fails → one extra `health-check-failed` event, `validateCalls` += 3, `sleeps` += 2. Where the test's gateway was nil and nothing was declined via gateway (`TestRunOnceDecline` declines the catalog candidate — `declined` stays "" because it tracks only the **gateway** candidate), recovery runs: gateway nil → catalog scan returns the same candidate 4-8, but `sameModel(cand, broken)` is false (4-8 vs 4-7) so recovery probes 4-8 again and fails 3 more times → total `validateCalls` = 3 (upgrade) + 3 (health) + 3 (recovery) = 9, `sleeps` = 6, events = `declined` + `health-check-failed`.
- Add `recoverSwap: func(string) {}` to `newTestUpdater` so older tests never nil-panic; individual tests override with `t.Fatal` guards as shown in Step 1.

Also give each updated test a one-line comment stating the new expectation, e.g. `// +1 validate: daily health check of the current model.`

- [ ] **Step 5: Run full package to verify green**

Run: `go test ./pkg/workflow/builtin/bedrock/ -race`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/workflow/builtin/bedrock/autoupdate.go pkg/workflow/builtin/bedrock/factory.go pkg/workflow/builtin/bedrock/autoupdate_test.go
git commit -m "feat(bedrock): daily health check of current model with one-shot recovery"
```

---

### Task 5: Request-time last-known-good fallback in `Stream`

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/factory.go` (client field type, line 65)
- Modify: `pkg/workflow/builtin/bedrock/stream.go` (invoke block, lines 27-39)
- Test: `pkg/workflow/builtin/bedrock/stream_fallback_test.go` (new file)

**Interfaces:**
- Consumes: `lastKnownGoodModel()` (Task 3).
- Produces:
  - `type invokeAPI interface { InvokeModelWithResponseStream(ctx context.Context, params *bedrockruntime.InvokeModelWithResponseStreamInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelWithResponseStreamOutput, error) }` — `bedrockLLM.client` becomes this type (`*bedrockruntime.Client` satisfies it; `Init` unchanged).
  - `isModelUnavailable(err error) bool` — true for smithy `APIError` codes `ValidationException` / `ResourceNotFoundException`.

- [ ] **Step 1: Write the failing tests (new file `stream_fallback_test.go`)**

```go
package bedrock

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/smithy-go"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeInvoker records the ModelId of each call and returns scripted errors.
type fakeInvoker struct {
	models []string
	errs   []error
}

func (f *fakeInvoker) InvokeModelWithResponseStream(_ context.Context, in *bedrockruntime.InvokeModelWithResponseStreamInput, _ ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelWithResponseStreamOutput, error) {
	f.models = append(f.models, aws.ToString(in.ModelId))
	return nil, f.errs[len(f.models)-1]
}

func validationErr() error {
	return &smithy.GenericAPIError{Code: "ValidationException", Message: "model no longer supported"}
}

func probeReq() node.LLMRequest {
	return node.LLMRequest{
		MaxTokens: 16,
		Messages: []node.Message{{
			Role:    node.UserMsg,
			Content: []node.ContentBlock{{Type: node.BlockText, Text: "hi"}},
		}},
	}
}

func TestIsModelUnavailable(t *testing.T) {
	if !isModelUnavailable(validationErr()) {
		t.Fatal("ValidationException must be model-unavailable")
	}
	if !isModelUnavailable(&smithy.GenericAPIError{Code: "ResourceNotFoundException"}) {
		t.Fatal("ResourceNotFoundException must be model-unavailable")
	}
	if isModelUnavailable(&smithy.GenericAPIError{Code: "ThrottlingException"}) {
		t.Fatal("ThrottlingException must NOT trigger the fallback")
	}
	if isModelUnavailable(errors.New("plain")) {
		t.Fatal("non-APIError must NOT trigger the fallback")
	}
}

// Model-unavailable error + lkg set → exactly one retry with the lkg model.
func TestStreamRetriesWithLastKnownGood(t *testing.T) {
	fake := &fakeInvoker{errs: []error{validationErr(), errors.New("still down")}}
	b := &bedrockLLM{
		cfg:           Config{Model: "new", MaxTokens: 16, AnthropicVersion: defaultAnthropicVersion},
		model:         "us.anthropic.claude-opus-4-8",
		lastKnownGood: "us.anthropic.claude-opus-4-7",
		client:        fake,
	}
	out := make(chan node.LLMEvent, 8)
	err := b.Stream(context.Background(), probeReq(), out)
	if err == nil {
		t.Fatal("expected error (second call also fails)")
	}
	if len(fake.models) != 2 || fake.models[0] != "us.anthropic.claude-opus-4-8" || fake.models[1] != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("calls = %v, want [4-8, 4-7]", fake.models)
	}
}

// Non-model errors and missing lkg must NOT retry.
func TestStreamNoRetryCases(t *testing.T) {
	cases := []struct {
		name string
		lkg  string
		err  error
	}{
		{"throttling error", "us.anthropic.claude-opus-4-7", &smithy.GenericAPIError{Code: "ThrottlingException"}},
		{"no lkg", "", validationErr()},
		{"lkg equals current", "us.anthropic.claude-opus-4-8", validationErr()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeInvoker{errs: []error{c.err, errors.New("must not be reached")}}
			b := &bedrockLLM{
				cfg:           Config{Model: "x", MaxTokens: 16, AnthropicVersion: defaultAnthropicVersion},
				model:         "us.anthropic.claude-opus-4-8",
				lastKnownGood: c.lkg,
				client:        fake,
			}
			out := make(chan node.LLMEvent, 8)
			if err := b.Stream(context.Background(), probeReq(), out); err == nil {
				t.Fatal("expected error")
			}
			if len(fake.models) != 1 {
				t.Fatalf("calls = %d, want 1 (no retry)", len(fake.models))
			}
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/workflow/builtin/bedrock/ -run 'TestIsModelUnavailable|TestStreamRetries|TestStreamNoRetry' -v`
Expected: FAIL — compile error: `isModelUnavailable` undefined and `fakeInvoker` not assignable to `*bedrockruntime.Client`.

- [ ] **Step 3: Implement**

3a. `factory.go`: change the client field (line 65) and add the interface above the struct:

```go
// invokeAPI is the one Bedrock Runtime call the node makes — an interface so
// tests can fake invoke failures without AWS. *bedrockruntime.Client
// satisfies it.
type invokeAPI interface {
	InvokeModelWithResponseStream(ctx context.Context, params *bedrockruntime.InvokeModelWithResponseStreamInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelWithResponseStreamOutput, error)
}
```

```go
	client invokeAPI
```

(`Init` at line 112 keeps `b.client = bedrockruntime.NewFromConfig(awsCfg)` unchanged.)

3b. `stream.go`: replace the invoke block (lines 27-39) with:

```go
	input := func(m string) *bedrockruntime.InvokeModelWithResponseStreamInput {
		return &bedrockruntime.InvokeModelWithResponseStreamInput{
			ModelId:     aws.String(m),
			ContentType: aws.String("application/json"),
			Accept:      aws.String("application/json"),
			Body:        payload,
		}
	}
	resp, err := b.client.InvokeModelWithResponseStream(ctx, input(model))
	if err != nil && isModelUnavailable(err) {
		// Request-time fallback: the fresh model broke mid-day (deprecated or
		// removed by AWS after the last check) — retry ONCE with the model
		// displaced by the last upgrade. Only before any event was emitted;
		// mid-stream failures are never retried.
		if lkg := b.lastKnownGoodModel(); lkg != "" && lkg != model {
			if b.logger != nil {
				b.logger.Printf("ai/bedrock wf=%s node=%s model %s failed (%v), retrying with last-known-good %s", b.wfID, b.nodeID, model, err, lkg)
			}
			model = lkg
			resp, err = b.client.InvokeModelWithResponseStream(ctx, input(model))
		}
	}
	if err != nil {
		if b.logger != nil {
			b.logger.Printf("ai/bedrock invoke failed: wf=%s node=%s model=%s region=%s err=%v", b.wfID, b.nodeID, model, b.cfg.Region, err)
		}
		emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
		return err
	}
```

3c. Add `isModelUnavailable` at the bottom of `stream.go`:

```go
// isModelUnavailable reports invoke errors that indicate the model ID itself
// is bad (deprecated, removed, or unentitled) rather than a transient fault —
// only these justify the last-known-good retry.
func isModelUnavailable(err error) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.ErrorCode() {
	case "ValidationException", "ResourceNotFoundException":
		return true
	}
	return false
}
```

Add `"errors"` and `"github.com/aws/smithy-go"` to `stream.go` imports (smithy-go is already an indirect dependency of aws-sdk-go-v2; if `go build` complains, run `go mod tidy`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/workflow/builtin/bedrock/ -race`
Expected: PASS (whole package — the interface change must not break `validateModel`, which calls the same method).

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/bedrock/factory.go pkg/workflow/builtin/bedrock/stream.go pkg/workflow/builtin/bedrock/stream_fallback_test.go
git commit -m "feat(bedrock): retry once with last-known-good model when invoke says model unavailable"
```

---

### Task 6: Break-glass env pin `AI_BEDROCK_MODEL_ID`

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/factory.go` (`Init`, lines 107-120; new const + method)
- Test: `pkg/workflow/builtin/bedrock/autoupdate_test.go`

**Interfaces:**
- Consumes: `setModel` (existing), `currentModel()`.
- Produces: `const pinEnv = "AI_BEDROCK_MODEL_ID"`, `applyEnvPin() bool`. When pinned, `startAutoUpdate` is never called.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/workflow/builtin/bedrock/ -run TestApplyEnvPin -v`
Expected: FAIL — `pinEnv`/`applyEnvPin` undefined.

- [ ] **Step 3: Implement**

In `factory.go`, add near the top (after the `defaultAnthropicVersion` const block):

```go
// pinEnv pins the model PROCESS-WIDE (every ai/bedrock node) and disables
// auto-update — the break-glass operator override: no JSON edit, no
// redeploy. Ported from powerlineAIApi's AI_BEDROCK_MODEL_ID.
const pinEnv = "AI_BEDROCK_MODEL_ID"

// applyEnvPin returns true when pinEnv is set; the pinned value overrides the
// workflow JSON model verbatim (trimmed, no format validation — operator-
// controlled) and the caller must not start the updater.
func (b *bedrockLLM) applyEnvPin() bool {
	pin := strings.TrimSpace(os.Getenv(pinEnv))
	if pin == "" {
		return false
	}
	b.setModel(pin)
	return true
}
```

Add `"os"` and `"strings"` to `factory.go` imports.

In `Init` (lines 116-118), replace:

```go
	if b.cfg.autoUpdateEnabled() {
		b.startAutoUpdate(env, awsCfg)
	}
```

with:

```go
	if b.applyEnvPin() {
		b.logger.Printf("ai/bedrock wf=%s node=%s model pinned to %s via %s — auto-update disabled", b.wfID, b.nodeID, b.currentModel(), pinEnv)
	} else if b.cfg.autoUpdateEnabled() {
		b.startAutoUpdate(env, awsCfg)
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/workflow/builtin/bedrock/ -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/bedrock/factory.go pkg/workflow/builtin/bedrock/autoupdate_test.go
git commit -m "feat(bedrock): AI_BEDROCK_MODEL_ID break-glass pin disables auto-update"
```

---

### Task 7: Library docs + full verification

**Files:**
- Modify: `DEPLOYMENT.md` (env-var table — find it with `grep -n "INFERENCE_GATEWAY_BASE_PATH\|MODEL_AUTOUPDATE" DEPLOYMENT.md`)
- Modify: `pkg/workflow/builtin/bedrock/autoupdate.go` (header comment, lines 1-7)

- [ ] **Step 1: Update the autoupdate.go header comment**

Replace lines 1-7 with:

```go
// Auto-update: resolves the latest Claude model of the same family — primary
// source is the org inferenceGateway (NATS resolveModel, see gateway.go),
// fallback is a Bedrock control-plane scan — validates candidates with a real
// test invocation (production payload shape, 3 attempts), and hot-swaps the
// node's model in memory. Runs at startup and daily at 07:00 UTC (+0-30min
// jitter); each cycle also health-checks the model in use and recovers via
// re-resolve when it fails. AI_BEDROCK_MODEL_ID pins the model and disables
// all of this. Specs:
// docs/superpowers/specs/2026-06-05-bedrock-model-autoupdate-design.md
// docs/superpowers/specs/2026-07-30-bedrock-gateway-model-resolution-design.md
// docs/superpowers/specs/2026-08-04-bedrock-autoupdate-hardening-design.md
```

- [ ] **Step 2: Update DEPLOYMENT.md**

In the env-var section that already documents `INFERENCE_GATEWAY_BASE_PATH` and `MODEL_AUTOUPDATE_NOTIFY_SUBJECT`, add one row/entry:

```markdown
| `AI_BEDROCK_MODEL_ID` | `""` | Break-glass: when set, every ai/bedrock node uses this model verbatim and auto-update is fully disabled. Empty = auto mode. |
```

And in the notification-event documentation (same file, `modelAutoUpdate` section), extend the event list: `upgraded | declined | recovered | health-check-failed` — `recovered` carries `resolver:"recovery"`; `health-check-failed` has empty `to` and a populated `error`. Note the schedule change: daily check now anchors at **07:00 UTC + 0–30 min jitter** (was 02:00 server-local).

- [ ] **Step 3: Full library verification**

```bash
gofmt -l pkg/ && go vet ./... && go test ./... -race
```

Expected: gofmt prints nothing; vet clean; all packages PASS.

- [ ] **Step 4: Commit**

```bash
git add DEPLOYMENT.md pkg/workflow/builtin/bedrock/autoupdate.go
git commit -m "docs: bedrock auto-update hardening (UTC schedule, health check, env pin)"
```

---

### Task 8: Release v1.3.0 — APPROVAL GATED

**Files:** none (git only). **Do not execute without explicit user approval — pushing triggers CI.**

- [ ] **Step 1: Ask the user to approve pushing the branch + opening the PR to `Development`**
- [ ] **Step 2 (after merge approval): tag and push `v1.3.0`**

```bash
git tag v1.3.0 && git push origin v1.3.0
```

---

### Task 9: Consumer repo — docs + terraform (opensearchAiChatApi)

**Files:**
- Modify: `terraform/variables.tf` (append after the `INFERENCE_GATEWAY_BASE_PATH` variable, ~line 64)
- Modify: `terraform/ecs.tf` (env list, next to the `INFERENCE_GATEWAY_BASE_PATH` entry at line 64)
- Modify: `.github/workflows/DeploymentEast.yml` and `DeploymentWest.yml` (next to the existing export at line 101)
- Modify: `.envTemplate`, `docs/configuration.md`

**Interfaces:**
- Consumes: library env contract from Task 6/7: `AI_BEDROCK_MODEL_ID`, `INFERENCE_GATEWAY_BASE_PATH`.

- [ ] **Step 1: terraform/variables.tf**

```hcl
variable "AI_BEDROCK_MODEL_ID" {
  type        = string
  default     = ""
  description = "Break-glass model pin (ai-agent-go-service >= v1.3.0): when set, every ai/bedrock node uses this model verbatim and auto-update is disabled. Empty = auto mode."
}
```

- [ ] **Step 2: terraform/ecs.tf** — add to the container env list (same list as `INFERENCE_GATEWAY_BASE_PATH`, line 64):

```hcl
    { name = "AI_BEDROCK_MODEL_ID", value = var.AI_BEDROCK_MODEL_ID },
```

- [ ] **Step 3: Both deployment workflows** — add next to line 101:

```yaml
          export TF_VAR_AI_BEDROCK_MODEL_ID=${{ vars.AI_BEDROCK_MODEL_ID }}
```

(An unset GitHub var exports empty → terraform default `""` → auto mode; same pattern as the existing gateway var.)

- [ ] **Step 4: `.envTemplate`** — append (matching the file's existing comment style):

```
# Model auto-update (ai-agent-go-service >= v1.1.0). Org gateway base path;
# library placeholder default means "gateway off, catalog scan only".
INFERENCE_GATEWAY_BASE_PATH=trx.inferenceGateway
# Break-glass: pin the Bedrock model verbatim and disable auto-update
# (lib >= v1.3.0). Empty = auto mode.
AI_BEDROCK_MODEL_ID=
```

- [ ] **Step 5: `docs/configuration.md`** — add both vars to the env table with the same descriptions, plus a line in the model-auto-update section: daily check anchors at 07:00 UTC + 0–30 min jitter; notify events now include `recovered` and `health-check-failed`.

- [ ] **Step 6: Validate + commit**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/opensearchAiChatApi
terraform -chdir=terraform fmt -check && terraform -chdir=terraform validate || true  # validate needs init; fmt check is the hard gate
git add terraform/variables.tf terraform/ecs.tf .github/workflows/DeploymentEast.yml .github/workflows/DeploymentWest.yml .envTemplate docs/configuration.md
git commit -m "config: AI_BEDROCK_MODEL_ID break-glass pin + document model auto-update env vars"
```

---

### Task 10: Consumer repo — dependency bump (blocked on Task 8)

**Files:**
- Modify: `go.mod`, `go.sum`, `vendor/`

- [ ] **Step 1: Bump + vendor (only after the v1.3.0 tag exists on origin)**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/opensearchAiChatApi
GOPRIVATE=github.com/transactrx go get github.com/transactrx/ai-agent-go-service@v1.3.0
go mod vendor
```

- [ ] **Step 2: Build + test**

```bash
go build ./... && go test ./...
```

Expected: PASS (this repo's own tests are in `pkg/workflow/builtin/powerlinescope`).

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum vendor/
git commit -m "chore(deps): bump ai-agent-go-service to v1.3.0 (auto-update hardening)"
```

---

### Task 11: Operator checklist (manual, no code)

- [ ] Verify GitHub environment variable `INFERENCE_GATEWAY_BASE_PATH` = `trx.inferenceGateway` in both `Development` and `Production` environments of `transactrx/opensearchAiChatApi` (`gh variable list --env Development --repo transactrx/opensearchAiChatApi`). Not visible from the repo; without it, gateway resolution silently stays off (catalog scan only).
- [ ] Do NOT set `AI_BEDROCK_MODEL_ID` anywhere — it is break-glass only.
- [ ] After deploy, check logs for `autoupdate: enabled (model=… gateway=trx.inferenceGateway.resolveModel …)` and the first `run finished outcome=…` line.
- [ ] Release-notes callout for other lib consumers: schedule moved from 02:00 server-local to 07:00 UTC + jitter.
