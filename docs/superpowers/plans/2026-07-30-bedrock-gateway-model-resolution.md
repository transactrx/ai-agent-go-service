# Bedrock Gateway Model Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The `ai/bedrock` node's daily auto-update asks the org inferenceGateway (NATS request/reply) for the latest release of its model family, keeping the existing Bedrock catalog scan as fallback.

**Architecture:** New `gateway.go` holds a thin NATS client plus pure helpers (subject/env resolution, query derivation, reply parsing). `autoUpdater` gains one injected `resolve` func; `runOnce` tries gateway first, falls back to the unchanged `list`+`latestCandidate` scan, then the existing validate→swap→notify pipeline runs. No node-config changes; env-only wiring.

**Tech Stack:** Go 1.25 (vendored deps, `go test -mod=vendor`), `github.com/nats-io/nats.go` (`RequestWithContext`), `github.com/transactrx/nats-service` (STATUS header const, `NatsServiceError` reply shape).

**Spec:** `docs/superpowers/specs/2026-07-30-bedrock-gateway-model-resolution-design.md`

## Global Constraints

- **Do NOT run `git commit`** — user commits explicitly (user CLAUDE.md rule). Leave changes in the working tree; end each task with tests green.
- Env var: `INFERENCE_GATEWAY_BASE_PATH`, default `example.inferenceGateway` (generic placeholder, repo convention per commit e38389b); request subject = `<basePath>.resolveModel`; request timeout 10 s.
- Gateway request body: `{"lab":"anthropic","family":"<family>"}` where family comes from `parseModelID` (e.g. `claude-opus`).
- Follow gateway always: any `invokeId` ≠ current model becomes the candidate (upgrade, rollback, or prefix change) — still gated by the existing ping validation. Fallback scan keeps strictly-newer-same-prefix rule.
- `runOnce` never returns an error; every failure = keep current model, retry next cycle.
- nats-service reply protocol: header `nats_service_common.STATUS` (key `"status"`) holds `"200"` on success; error replies carry `NatsServiceError` JSON (`{"status":…,"errorMessage":…}`).
- All tests: `go test -mod=vendor ./pkg/workflow/builtin/bedrock/` (add `-race` on final runs).

---

### Task 1: gateway.go — subject/env, query derivation, reply parsing, NATS client

**Files:**
- Create: `pkg/workflow/builtin/bedrock/gateway.go`
- Create: `pkg/workflow/builtin/bedrock/gateway_test.go`

**Interfaces:**
- Consumes: `parseModelID` (existing, `autoupdate.go`).
- Produces (used by Tasks 2–3):
  - `gatewaySubject() string`
  - `deriveGatewayQuery(modelID string) (lab, family string, err error)`
  - `parseResolveReply(status string, body []byte) (string, error)`
  - `resolveViaGateway(ctx context.Context, nc *nats.Conn, subject, lab, family string) (string, error)`
  - consts `gatewayBasePathEnv = "INFERENCE_GATEWAY_BASE_PATH"`, `defaultGatewayBasePath = "example.inferenceGateway"`

- [ ] **Step 1: Write the failing tests**

Create `pkg/workflow/builtin/bedrock/gateway_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -mod=vendor ./pkg/workflow/builtin/bedrock/ -run 'TestGatewaySubject|TestDeriveGatewayQuery|TestParseResolveReply' -v`
Expected: compile FAIL — `undefined: gatewaySubject` etc.

- [ ] **Step 3: Write the implementation**

Create `pkg/workflow/builtin/bedrock/gateway.go`:

```go
// Gateway model resolution: asks the org inferenceGateway NATS service
// (resolveModel endpoint) for the latest release of the node's model family.
// Primary source for the auto-update check in autoupdate.go; the Bedrock
// catalog scan there remains the fallback. Spec:
// docs/superpowers/specs/2026-07-30-bedrock-gateway-model-resolution-design.md
package bedrock

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
)

// INFERENCE_GATEWAY_BASE_PATH points at the deployment's gateway (org value
// "trx.inferenceGateway"). The default is an obvious placeholder (repo
// convention, cf. pkg/idt): with no responder there, resolution fails fast
// and the auto-updater falls back to the catalog scan.
const (
	gatewayBasePathEnv     = "INFERENCE_GATEWAY_BASE_PATH"
	defaultGatewayBasePath = "example.inferenceGateway"
	resolveModelSuffix     = ".resolveModel"
	gatewayRequestTimeout  = 10 * time.Second
	anthropicLab           = "anthropic"
)

// gatewaySubject returns the resolveModel request subject.
func gatewaySubject() string {
	base := strings.TrimSpace(os.Getenv(gatewayBasePathEnv))
	if base == "" {
		base = defaultGatewayBasePath
	}
	return base + resolveModelSuffix
}

// deriveGatewayQuery maps a concrete model/profile ID to the gateway request
// pair. parseModelID only accepts anthropic IDs, so the lab is fixed; the
// dashed family ("claude-opus") matches the gateway's "claude opus" — its
// tokenizer splits dashes.
func deriveGatewayQuery(modelID string) (lab, family string, err error) {
	p, err := parseModelID(modelID)
	if err != nil {
		return "", "", err
	}
	return anthropicLab, p.family, nil
}

// resolveReply is the subset of the gateway's ModelInfo (success) and
// NatsServiceError (failure) bodies this client consumes.
type resolveReply struct {
	InvokeID     string `json:"invokeId"`
	ErrorMessage string `json:"errorMessage"`
}

// parseResolveReply extracts the invokeId from a resolveModel reply. status
// is the nats-service STATUS header value; empty is treated as success.
func parseResolveReply(status string, body []byte) (string, error) {
	var r resolveReply
	jsonErr := json.Unmarshal(body, &r)
	if status != "" && !strings.HasPrefix(status, "2") {
		return "", fmt.Errorf("gateway status %s: %s", status, errText(r.ErrorMessage, body))
	}
	if jsonErr != nil {
		return "", fmt.Errorf("gateway reply not JSON: %w", jsonErr)
	}
	if r.InvokeID == "" {
		return "", fmt.Errorf("gateway reply has no invokeId: %s", errText("", body))
	}
	return r.InvokeID, nil
}

// errText prefers the structured errorMessage, else the (truncated) raw body.
func errText(msg string, body []byte) string {
	if msg != "" {
		return msg
	}
	const max = 200
	if len(body) > max {
		return string(body[:max]) + "..."
	}
	return string(body)
}

// resolveViaGateway performs one request/reply against the gateway. Kept
// thin (like listActiveProfileIDs): decisions live in the pure helpers above.
func resolveViaGateway(ctx context.Context, nc *nats.Conn, subject, lab, family string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, gatewayRequestTimeout)
	defer cancel()
	body, err := json.Marshal(map[string]string{"lab": lab, "family": family})
	if err != nil {
		return "", err
	}
	msg, err := nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		return "", fmt.Errorf("gateway request %s: %w", subject, err)
	}
	return parseResolveReply(msg.Header.Get(nats_service_common.STATUS), msg.Data)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -mod=vendor ./pkg/workflow/builtin/bedrock/ -run 'TestGatewaySubject|TestDeriveGatewayQuery|TestParseResolveReply' -v`
Expected: PASS. Also `go build -mod=vendor ./...` — clean (verifies the `nats-service-common` vendored import resolves; it is already in `vendor/`).

---

### Task 2: runOnce two-stage resolution + resolver-tagged events

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/autoupdate.go:120-201` (`noteEvent`, `autoUpdater`, `runOnce`)
- Modify: `pkg/workflow/builtin/bedrock/autoupdate_test.go` (extend harness assertions, add gateway scenarios)

**Interfaces:**
- Consumes: nothing from Task 1 at runtime (the injected func is opaque here).
- Produces: `autoUpdater.resolve func(ctx context.Context) (string, error)` field (nil = gateway disabled); `noteEvent.Resolver string` (`"gateway"`/`"fallback"`, JSON key `resolver`). Task 3 wires `resolve`.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/workflow/builtin/bedrock/autoupdate_test.go`:

```go
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
	u.resolve = func(context.Context) (string, error) { return "", errors.New("nats: no responders available for request") }
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
```

Also strengthen two existing tests (nil `resolve` = legacy path) — in `TestRunOnceUpgrade` extend the event assertion:

```go
	if ev.Event != "upgraded" || ev.From != "us.anthropic.claude-opus-4-7" ||
		ev.To != "us.anthropic.claude-opus-4-8" || ev.Attempts != 2 ||
		ev.WorkflowID != "powerlineSearch" || ev.NodeID != "bedrock1" || ev.Timestamp == "" ||
		ev.Resolver != "fallback" {
		t.Fatalf("unexpected event: %+v", ev)
	}
```

and in `TestRunOnceDecline`:

```go
	if ev.Event != "declined" || ev.Attempts != 3 || ev.Error == "" || ev.To != "us.anthropic.claude-opus-4-8" ||
		ev.Resolver != "fallback" {
		t.Fatalf("unexpected event: %+v", ev)
	}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -mod=vendor ./pkg/workflow/builtin/bedrock/ -run 'TestRunOnce' -v`
Expected: compile FAIL — `unknown field resolve` / `unknown field Resolver`.

- [ ] **Step 3: Implement**

In `pkg/workflow/builtin/bedrock/autoupdate.go`:

(a) `noteEvent` — add field after `Error`:

```go
	Error      string `json:"error,omitempty"`
	// Resolver says which source produced the candidate: "gateway" (org
	// inferenceGateway) or "fallback" (Bedrock catalog scan).
	Resolver  string `json:"resolver,omitempty"`
	Timestamp string `json:"timestamp"`
```

(b) `autoUpdater` — add injected field after `current`/`swap`:

```go
	current  func() string                                   // effective model getter
	swap     func(string)                                    // effective model setter
	resolve  func(ctx context.Context) (string, error)       // gateway resolveModel; nil → catalog scan only
	list     func(ctx context.Context) ([]string, error)     // ACTIVE system inference-profile IDs (fallback)
```

(c) Replace the body of `runOnce` between the `parseModelID` guard and the validation loop (the guard, the deferred summary log, the validation loop, and `notify` stay as they are — only the candidate selection changes, and the two `notify` calls gain `Resolver: resolver`):

```go
	// Stage 1 — org inferenceGateway, the source of truth. Followed wherever
	// it points (upgrade, org rollback, or geo-prefix change); every swap is
	// still gated by validation below. Any failure falls through to stage 2.
	cand, resolver := "", ""
	if u.resolve != nil {
		switch id, rerr := u.resolve(ctx); {
		case rerr != nil:
			u.logf("autoupdate: gateway resolve failed (using catalog-scan fallback): %v", rerr)
		case id == cur:
			return // outcome stays "already-latest"
		default:
			cand, resolver = id, "gateway"
		}
	}

	// Stage 2 — Bedrock catalog scan, strictly newer within the same
	// prefix+family (pre-gateway behavior, unchanged).
	if cand == "" {
		ids, lerr := u.list(ctx)
		if lerr != nil {
			outcome = "list-failed"
			u.logf("autoupdate: list inference profiles failed (retry next cycle): %v", lerr)
			return
		}
		c, ok := latestCandidate(ids, parsed)
		if !ok {
			return // outcome stays "already-latest"
		}
		cand, resolver = c, "fallback"
	}
	u.logf("autoupdate: found candidate %s (current %s, resolver %s), validating", cand, cur, resolver)

	var lastErr error
	for attempt := 1; attempt <= validationAttempts; attempt++ {
		if ctx.Err() != nil {
			outcome = "cancelled"
			return
		}
		if lastErr = u.validate(ctx, cand); lastErr == nil {
			u.swap(cand)
			outcome = "upgraded"
			u.logf("autoupdate: upgraded %s -> %s (attempt %d/%d, resolver %s)", cur, cand, attempt, validationAttempts, resolver)
			u.notify(noteEvent{Event: "upgraded", From: cur, To: cand, Attempts: attempt, Resolver: resolver})
			return
		}
		u.logf("autoupdate: validation %d/%d of %s failed: %v", attempt, validationAttempts, cand, lastErr)
		if attempt < validationAttempts {
			u.sleep(ctx, validationBackoffs[attempt-1])
		}
	}
	outcome = "declined"
	u.notify(noteEvent{Event: "declined", From: cur, To: cand, Attempts: validationAttempts, Error: lastErr.Error(), Resolver: resolver})
```

Also update the file's package comment (line 1-4) first sentence to:

```go
// Auto-update: resolves the latest Claude model of the same family — primary
// source is the org inferenceGateway (NATS resolveModel, see gateway.go),
// fallback is a Bedrock control-plane scan — validates candidates with a real
// test invocation (production payload shape, 3 attempts), and hot-swaps the
// node's model in memory. Specs:
// docs/superpowers/specs/2026-06-05-bedrock-model-autoupdate-design.md
// docs/superpowers/specs/2026-07-30-bedrock-gateway-model-resolution-design.md
```

- [ ] **Step 4: Run the full package tests**

Run: `go test -mod=vendor -race ./pkg/workflow/builtin/bedrock/ -v`
Expected: ALL PASS — new gateway scenarios plus every pre-existing test (nil `resolve` keeps legacy behavior; `TestRunOnceAlwaysLogsSummary` unchanged and green).

---

### Task 3: wire the gateway resolver in startAutoUpdate

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/autoupdate.go:254-311` (`startAutoUpdate`, new `natsConn` helper next to `resolveNotify`)
- Modify: `pkg/workflow/builtin/bedrock/factory.go:19-28` (`Config.Model` doc comment)

**Interfaces:**
- Consumes: `gatewaySubject`, `deriveGatewayQuery`, `resolveViaGateway` (Task 1); `autoUpdater.resolve` (Task 2); existing `env.Host("nats")` / `nats_service.NatService.GetNatsService()`.
- Produces: `natsConn(env node.NodeEnv) *nats.Conn` helper.

- [ ] **Step 1: Add the `natsConn` helper** (below `resolveNotify` in `autoupdate.go`; import `"github.com/nats-io/nats.go"`):

```go
// natsConn returns the shared NATS connection from the hosts map, or nil
// when the host is missing, mistyped, or not connected.
func natsConn(env node.NodeEnv) *nats.Conn {
	host, ok := env.Host("nats")
	if !ok {
		return nil
	}
	ns, ok := host.(*nats_service.NatService)
	if !ok {
		return nil
	}
	return ns.GetNatsService()
}
```

- [ ] **Step 2: Wire `resolve` in `startAutoUpdate`** — insert between the `resolveNotify` block and `ctx, cancel := ...`, and pass the field:

```go
	// Gateway resolution: primary source for the daily check. Missing NATS
	// conn → nil resolve → catalog scan only (pre-gateway behavior). The
	// lab/family query is derived from the CURRENT model on every call, so it
	// stays correct across gateway-issued prefix changes.
	var resolve func(ctx context.Context) (string, error)
	gwSubject := gatewaySubject()
	if nc := natsConn(env); nc == nil {
		b.logger.Printf("ai/bedrock wf=%s node=%s autoupdate: gateway resolution off (no NATS connection) — catalog scan only", b.wfID, b.nodeID)
	} else {
		resolve = func(ctx context.Context) (string, error) {
			lab, family, derr := deriveGatewayQuery(b.currentModel())
			if derr != nil {
				return "", derr
			}
			return resolveViaGateway(ctx, nc, gwSubject, lab, family)
		}
	}
```

In the `autoUpdater` literal add `resolve: resolve,` after `swap:`, and extend the final startup log line to include the gateway subject:

```go
	b.logger.Printf("ai/bedrock wf=%s node=%s autoupdate: enabled (model=%s gateway=%s subject=%s)", b.wfID, b.nodeID, b.currentModel(), gwSubject, subject)
```

- [ ] **Step 3: Update `Config.Model` semantics comment** in `factory.go` — replace the `model` field comment on the `bedrockLLM` struct (factory.go:70-71):

```go
	// model is the current effective model ID. cfg.Model is the configured
	// starting model; the auto-updater (gateway-first, catalog-scan fallback)
	// may swap model to a different validated release.
```

- [ ] **Step 4: Build + full test suite**

Run: `go build -mod=vendor ./... && go test -mod=vendor -race ./pkg/workflow/builtin/bedrock/`
Expected: clean build, ALL PASS.

- [ ] **Step 5: Whole-repo regression**

Run: `go test -mod=vendor ./...`
Expected: ALL PASS (matches `docker-compose` test job invocation; NATS-dependent tests skip/pass as they do today).

---

### Task 4: deployment docs

**Files:**
- Modify: `docs/DEPLOYMENT.md:31-41` (env table)
- Modify: `docs/superpowers/specs/2026-07-30-bedrock-gateway-model-resolution-design.md` (deployment section correction)

**Interfaces:** none (docs only).

- [ ] **Step 1: Add env rows to the agent-half table** in `docs/DEPLOYMENT.md` after the `AWS_REGION_DYNAMODB` row:

```markdown
| `INFERENCE_GATEWAY_BASE_PATH` | — | `example.inferenceGateway` | Org inferenceGateway NATS base path; `ai/bedrock` auto-update asks `<base>.resolveModel` for the family's latest release (org value: `trx.inferenceGateway`). Unset/unreachable → Bedrock catalog-scan fallback. Set it in the consuming service's deployment env (Terraform task definition or GitHub environment vars). |
| `MODEL_AUTOUPDATE_NOTIFY_SUBJECT` | — | `<NATS_BASE_PATH>.modelAutoUpdate` | Subject for `ai/bedrock` auto-update upgraded/declined notifications |
```

- [ ] **Step 2: Correct the spec's deployment section** — in `docs/superpowers/specs/2026-07-30-bedrock-gateway-model-resolution-design.md`, replace the `docker-compose.yml` bullet with:

```markdown
- `docker-compose.yml`: no change — the compose file only runs NATS + the test
  suite (no agent service); the placeholder default already makes local runs
  fall back to the catalog scan.
```

- [ ] **Step 3: Verify docs render** — `grep -n 'INFERENCE_GATEWAY_BASE_PATH' docs/DEPLOYMENT.md` shows the row inside the table; table pipes balanced (7 rows → 9 rows, same column count).

---

## Self-review (done at plan time)

- **Spec coverage:** decisions 1–5 → Tasks 1–3; wire contract → Task 1; error handling → Task 2 (fall-through + never-error preserved); env/deployment → Tasks 1 (env const) and 4 (docs); testing list → Tasks 1–2 (gateway differs/equals/error/nil, decline ×3, parse tables, derive tables). Compose deviation documented in Task 4.
- **Type consistency:** `resolve func(ctx context.Context) (string, error)` identical in Tasks 2 (field) and 3 (wiring); `Resolver`/`resolver` JSON key consistent; `gatewaySubject`/`deriveGatewayQuery`/`resolveViaGateway` signatures match between Task 1 (def) and Task 3 (use).
- **Placeholder scan:** none; the one intentional NOTE in Task 2 Step 1 flags a corrected snippet inline.
