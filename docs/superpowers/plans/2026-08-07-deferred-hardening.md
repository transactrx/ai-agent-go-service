# Deferred Hardening v1.5.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve all nine deferred hardening items from the v1.4.0/v1.5.0 reviews as patch release v1.5.1.

**Architecture:** Nine independent, small fixes in the ai-agent-go-service library: two agent-loop/trigger behavior fixes (empty tool output padding, timeout status mapping), one doc-string correction, five derivation-loader hardenings, and two missing trigger tests. Each task is TDD, self-contained, and individually committable.

**Tech Stack:** Go 1.25, stdlib testing only (repo convention — no testify).

**Spec:** `docs/superpowers/specs/2026-08-07-deferred-hardening-design.md`

## Global Constraints

- Branch: `feature/deferred-hardening` (already created from Development @ 2bbb57a).
- After EVERY task: `go build ./... && go test ./...` green; final task also runs `-race`.
- No new config fields, no API surface changes, no new dependencies.
- Log-line shapes must stay `workflow %s: register failed: ...` for loader failures (isolation pattern).
- Commit per task, conventional-commit style, `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer.

---

### Task 1: A1 — pad empty tool results

**Files:**
- Modify: `pkg/workflow/builtin/agent/loop.go` (success-path payload assignment, directly after `isErr := err != nil; payload := out` — around line 480)
- Test: `pkg/workflow/builtin/agent/emptytoolresult_test.go` (create)

**Interfaces:**
- Consumes: existing `node.ContentBlock{Type: node.BlockToolResult, ToolResult: payload}` assembly.
- Produces: guarantee used by nothing else in-code; behavioral only — `ToolResult` payload is never empty/blank JSON.

- [ ] **Step 1: Write the failing test.** The agent package has existing tests with a fake LLM + fake tool harness — locate the existing test helper (grep `func TestAgent` and the fake tool registration in `pkg/workflow/builtin/agent/*_test.go`) and follow its pattern. The test: register a tool whose `Invoke` returns `json.RawMessage("")` and no error; drive one turn where the LLM first requests that tool, then answers. Assert that the second LLM request's tool_result content block is exactly `"(tool returned no output)"` (JSON string), NOT empty. Also add a table case for `json.RawMessage(`""`)` (empty JSON string) and `json.RawMessage("null")` — all three pad.

```go
// core assertion, adapt to the harness's captured-requests shape:
var got string
_ = json.Unmarshal(capturedSecondRequest.ToolResultBlock.ToolResult, &got)
if got != "(tool returned no output)" {
    t.Fatalf("empty tool output not padded: %q", got)
}
```

- [ ] **Step 2: Run it, verify it fails** — `go test ./pkg/workflow/builtin/agent/ -run EmptyToolResult -v` → FAIL (empty payload passes through today).

- [ ] **Step 3: Implement.** In `loop.go`, right after `isErr := err != nil` / `payload := out` (before the PolicyDeniedError branch is fine, but ONLY pad the success path):

```go
// Bedrock rejects user messages with empty content; a tool that legitimately
// returns nothing must still produce a non-empty tool_result block.
if !isErr && isBlankToolPayload(payload) {
    payload = mustJSON("(tool returned no output)")
}
```

and at file bottom:

```go
// isBlankToolPayload reports whether a successful tool payload would render
// as empty content: no bytes, whitespace, an empty JSON string, or JSON null.
func isBlankToolPayload(p json.RawMessage) bool {
    s := strings.TrimSpace(string(p))
    return s == "" || s == `""` || s == "null"
}
```

- [ ] **Step 4: Run** `go test ./pkg/workflow/builtin/agent/ -v` → all PASS.
- [ ] **Step 5: Commit** `fix(agent): pad empty tool results — Bedrock rejects empty content blocks`

---

### Task 2: A2a — agent loop classifies deadline/cancel before LLMError

**Files:**
- Modify: `pkg/workflow/builtin/agent/loop.go:295-296` (the `if llmErr != nil { return a.failStream(sink, errcode.LLMError, llmErr) }` site)
- Test: `pkg/workflow/builtin/agent/timeouterr_test.go` (create)

**Interfaces:**
- Consumes: `errcode.Timeout`, `errcode.Cancelled`, `errcode.LLMError` from `pkg/workflow/errcode`.
- Produces: stream error frames now carry `code:"timeout"` when the workflow deadline expired mid-LLM-call. Task 3 depends on this code value.

- [ ] **Step 1: Write the failing test.** Using the same fake-LLM harness: fake LLM returns error `fmt.Errorf("bedrock: %w", context.DeadlineExceeded)`. Run a turn; capture the sink's Close event. Assert the error frame's `code` is `"timeout"`, not `"llm-error"`. Second case: error wraps `context.Canceled` → code `"cancelled"`.

- [ ] **Step 2: Run, verify FAIL** (`code` is `llm-error` today).

- [ ] **Step 3: Implement** at loop.go:295:

```go
if llmErr != nil {
    code := errcode.LLMError
    switch {
    case errors.Is(llmErr, context.DeadlineExceeded):
        code = errcode.Timeout
    case errors.Is(llmErr, context.Canceled):
        code = errcode.Cancelled
    }
    return a.failStream(sink, code, llmErr)
}
```

- [ ] **Step 4: Run package tests** → PASS.
- [ ] **Step 5: Commit** `fix(agent): mid-stream deadline surfaces as timeout, client cancel as cancelled (was llm-error)`

---

### Task 3: A2b — single mode maps timeout code to 504

**Files:**
- Modify: `pkg/workflow/builtin/natschat/trigger.go` (`toReply` inside `handleSingle`, ~line 282)
- Test: `pkg/workflow/builtin/natschat/trigger_test.go` (or the package's existing single-mode test file — follow its harness)

**Interfaces:**
- Consumes: `r.errCode == errcode.Timeout` produced by Task 2 (arrives via collectSink error frame).
- Produces: single-mode NATS reply status 504 for timeouts.

- [ ] **Step 1: Failing test.** Follow the existing collectSink/handleSingle unit-test pattern: feed a `singleResult{errCode: errcode.Timeout, errMessage: "deadline"}` through `toReply`-equivalent path (or drive handleSingle with a sink that emits an error event `code:"timeout"`). Assert reply status 504, body code `"timeout"`. Keep the existing case: generic `llm-error` still → 500.

- [ ] **Step 2: Run, verify FAIL** (today Timeout code from the sink → generic 500 branch).

- [ ] **Step 3: Implement** in `toReply`:

```go
toReply := func(r singleResult) *nats_service.NatsServiceError {
    if r.errCode != "" {
        if r.errCode == errcode.Timeout ||
            (r.errCode == errcode.Cancelled && ctx.Err() != nil) {
            return serverErr(errcode.Timeout, "request timeout", 504)
        }
        return serverErr(r.errCode, r.errMessage, 500)
    }
    msg.ResponseBody = r.body
    return nil
}
```

- [ ] **Step 4: Run package tests** → PASS.
- [ ] **Step 5: Commit** `fix(natschat): single mode returns 504 for mid-stream timeouts`

---

### Task 4: A3 — opensearch tool description wording

**Files:**
- Modify: `pkg/workflow/builtin/opensearch/factory.go:159` (the `queryBody` description string)
- Test: adjust any test pinning the old string (`grep -rn "security/date" pkg/`)

- [ ] **Step 1:** `grep -rn "security/date" pkg/ docs/` — note every occurrence (factory.go + possibly docs/CHAT-ENDPOINTS.md).
- [ ] **Step 2:** Replace "injects security/date constraints into must" → "injects security constraints into must" in factory.go and any lib docs hit. Do NOT touch app repos here.
- [ ] **Step 3:** `go build ./... && go test ./pkg/workflow/builtin/opensearch/` → PASS (fix any test pinning the old text).
- [ ] **Step 4: Commit** `docs(opensearch): tool description says security constraints — no date clause is injected`

---

### Task 5: B1+B2 — extends resolves by workflow id; duplicate id guard

**Files:**
- Modify: `pkg/workflow/engine/engine.go:100-147` (LoadAll pass 1/pass 2)
- Test: `pkg/workflow/engine/engine_test.go` (existing isolation tests live here — follow their fake-Source pattern)

**Interfaces:**
- Consumes: `loader.ExtendsTarget(raw)`, `e.cfg.Source.List/Load`.
- Produces: pass-1 builds `byWorkflowID map[string][]byte` keyed by each doc's JSON `"id"`; extends resolves against it. Task 6 modifies the same region — do Task 5 first.

- [ ] **Step 1: Failing tests** (two, in engine_test.go, using the existing in-memory Source fake):
  1. `TestExtendsResolvesByWorkflowID`: base doc has JSON `"id":"parentWf"` but is listed by the Source under source id `"zz-parent-file"`; overlay declares `"extends":"parentWf"`. Assert both workflows register (today: fails with "base workflow not found").
  2. `TestDuplicateWorkflowIDFails`: two docs both declare `"id":"dupWf"` under different source ids. Assert exactly one registers, and the log (pin via the test logger the file already uses) contains `workflow dupWf: register failed: duplicate workflow id`.

- [ ] **Step 2: Run, verify both FAIL.**

- [ ] **Step 3: Implement.** In pass 1, after `raws[id] = raw`, peek the document id and build the second index; first-seen wins (Source.List order — pin with a sort if List isn't already deterministic, check its contract):

```go
type peekedDoc struct{ sourceID string; raw []byte }
byWorkflowID := make(map[string]peekedDoc, len(ids))
dupes := make(map[string]bool)
// inside the pass-1 loop, after raws[id] = raw:
var docPeek struct{ ID string `json:"id"` }
_ = json.Unmarshal(raw, &docPeek)
if docPeek.ID != "" {
    if _, seen := byWorkflowID[docPeek.ID]; seen {
        dupes[id] = true // this SOURCE id is a duplicate declaration
    } else {
        byWorkflowID[docPeek.ID] = peekedDoc{sourceID: id, raw: raw}
    }
}
```

In pass 2: before anything else, `if dupes[id] { log "workflow %s: register failed: duplicate workflow id %q (first definition wins)"; failed = append(...); continue }` — where the second `%s`/`%q` is the doc's JSON id. Then change extends resolution to `byWorkflowID[baseID]` (falling back to nothing — `raws` lookup is removed for extends). Chained-extends check uses the resolved doc's raw.

- [ ] **Step 4: Run** `go test ./pkg/workflow/engine/...` → PASS (existing isolation tests must stay green — their log pins don't change).
- [ ] **Step 5: Commit** `fix(engine): extends resolves by workflow id, not source id; duplicate workflow ids fail loudly`

---

### Task 6: B3 — wrong-type extends is an error

**Files:**
- Modify: `pkg/workflow/engine/loader/derive.go:11-25` (`ExtendsTarget`), `pkg/workflow/engine/engine.go` (call sites)
- Test: `pkg/workflow/engine/loader/derive_test.go`, `pkg/workflow/engine/engine_test.go`

**Interfaces:**
- Produces (signature change): `func ExtendsTarget(raw []byte) (id string, present bool, err error)` — `err` non-nil when the `extends` key exists but is not a non-empty string. Both engine.go call sites updated.

- [ ] **Step 1: Failing tests.** loader test: `{"id":"x","extends":123}` → err non-nil; `{"id":"x","extends":""}` → err non-nil (present but empty); `{"id":"x"}` → ("", false, nil); `{"id":"x","extends":"p"}` → ("p", true, nil). Engine test: a Source doc with `"extends": 42` fails registration with log `register failed: invalid extends`, siblings still load.

- [ ] **Step 2: Run, verify FAIL** (compile error on new signature is the failure — adjust callers minimally to get a true red test first if practical; otherwise accept compile-driven red).

- [ ] **Step 3: Implement:**

```go
func ExtendsTarget(raw []byte) (string, bool, error) {
    var peek map[string]json.RawMessage
    if err := json.Unmarshal(raw, &peek); err != nil {
        return "", false, nil // parse errors surface later in the normal pipeline
    }
    ev, ok := peek["extends"]
    if !ok {
        return "", false, nil
    }
    var s string
    if err := json.Unmarshal(ev, &s); err != nil || s == "" {
        return "", false, fmt.Errorf("invalid extends: must be a non-empty string, got %s", string(ev))
    }
    return s, true, nil
}
```

Engine call sites: `baseID, isDerived, extErr := loader.ExtendsTarget(raw)`; `if extErr != nil { log "workflow %s: register failed: %v"; failed; continue }`. The chained check's second call handles err the same way (treat as failure of the OVERLAY, not the base).

- [ ] **Step 4: Run** `go test ./pkg/workflow/engine/... ./pkg/workflow/engine/loader/...` → PASS.
- [ ] **Step 5: Commit** `fix(loader): non-string extends is a loud per-workflow error, not silently non-derived`

---

### Task 7: B4 — deepMerge copies maps; $remove never leaks

**Files:**
- Modify: `pkg/workflow/engine/loader/derive.go` (`deepMerge` ~30-49; node-merge around line 138)
- Test: `pkg/workflow/engine/loader/derive_test.go`

- [ ] **Step 1: Failing tests:**
  1. `TestDeepMergeNoAliasing`: merge; mutate a nested map in the RESULT; assert the base input map is unchanged.
  2. `TestRemoveKeyNeverInOutput`: overlay node `{"id":"n1","$remove":false,"config":{...}}` → merged node JSON contains no `$remove` key.

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement.** deepMerge: replace `out[k] = v` base-copy loop with a copy helper:

```go
func copyValue(v any) any {
    if m, ok := v.(map[string]any); ok {
        out := make(map[string]any, len(m))
        for k, mv := range m { out[k] = copyValue(mv) }
        return out
    }
    return v // arrays/scalars replace wholesale on overlay; base arrays are never mutated
}
// base loop becomes: out[k] = copyValue(v)
// the final non-map assignment stays: out[k] = ov
```

Node merge: after deciding a node is NOT removed, `delete(on, "$remove")` before merging it.

- [ ] **Step 4: Run loader tests + engine tests** → PASS.
- [ ] **Step 5: Commit** `fix(loader): deepMerge returns fresh maps; $remove marker stripped from merged output`

---

### Task 8: B5 — passthrough golden test

**Files:**
- Test: `pkg/workflow/engine/loader/derive_passthrough_test.go` (create)

- [ ] **Step 1: Write the test** (should PASS immediately — it's a regression net):

```go
func TestMergePassthroughUntouchedFields(t *testing.T) {
    base := []byte(`{"id":"p","version":1,"description":"d",
      "settings":{"a":1,"b":[1,2,3],"c":{"deep":true},"zero":0,"empty":""},
      "nodes":[{"id":"n1","type":"t","config":{"x":1,"arr":["q"],"nested":{"k":"v"}}},
               {"id":"n2","type":"t","config":{"y":2}}],
      "connections":[{"from":"n1","to":"n2"}]}`)
    overlay := []byte(`{"id":"c","extends":"p","nodes":[{"id":"n2","config":{"y":9}}]}`)
    merged, err := MergeDerived(base, overlay)
    if err != nil { t.Fatal(err) }
    var got, want map[string]any
    _ = json.Unmarshal(merged, &got)
    _ = json.Unmarshal(base, &want)
    // untouched subtrees must be deep-equal
    for _, k := range []string{"version", "description", "settings", "connections"} {
        if !reflect.DeepEqual(got[k], want[k]) {
            t.Fatalf("field %q altered by merge:\n got %#v\nwant %#v", k, got[k], want[k])
        }
    }
    // n1 untouched; n2.config.y overridden
    // (extract nodes by id and assert accordingly)
}
```

- [ ] **Step 2: Run** → expect PASS; if it FAILS, that is a real pre-existing merge bug: STOP and report before fixing.
- [ ] **Step 3: Commit** `test(loader): golden passthrough — untouched fields survive derivation merge`

---

### Task 9: C1 — two deferred natschat tests

**Files:**
- Test: `pkg/workflow/builtin/natschat/trigger_test.go` (or package convention file)

- [ ] **Step 1: Test A — terminator beats emitErr.** Drive `handleSingle` with a sink whose `Emit` BOTH pushes a valid complete payload through the collectSink AND returns a non-nil error. Assert the reply is the terminator's body (success), not `executor-failed` — pins the documented "prefer terminator over Emit's wrapped error" ordering.
- [ ] **Step 2: Test B — client cancel is not a timeout.** Feed `singleResult{errCode: errcode.Cancelled}` with a live (non-expired) ctx → assert 500 + code `cancelled`, NOT 504. (Guards the Task 3 conditional.)
- [ ] **Step 3: Run, both PASS** (they pin existing behavior; failures mean a real bug — report, don't paper over).
- [ ] **Step 4: Commit** `test(natschat): pin terminator-vs-emitErr precedence and cancel≠timeout mapping`

---

### Task 10: CHANGELOG, full validation, wrap-up

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1:** Add at top:

```markdown
## v1.5.1

- fix(agent): empty tool results are padded ("(tool returned no output)") — Bedrock
  rejects empty content blocks (was a 500 to the caller).
- fix(agent/natschat): a request timeout that fires mid-LLM-call now surfaces as
  `timeout` (single mode: HTTP-style 504) instead of `llm-error`/500. **Caller-visible
  status change for this failure mode.** Client cancellation still maps to `cancelled`.
- fix(engine): `extends` resolves by workflow id (not source/file id); duplicate
  workflow ids and non-string `extends` values now fail that workflow loudly
  (isolation preserved).
- fix(loader): deepMerge no longer aliases input maps; `$remove` markers are stripped
  from merged configs.
- docs(opensearch): tool description corrected — the server injects security
  constraints only (no date clause exists).
- tests: derivation passthrough golden test; natschat terminator-precedence and
  cancel-vs-timeout pins.
```

- [ ] **Step 2:** `go build ./... && go vet ./... && go test -race ./...` → ALL green.
- [ ] **Step 3:** Commit `docs: CHANGELOG for v1.5.1 (deferred hardening)`.
- [ ] **Step 4:** Report: branch ready, release gated on user (push → Development merge → Prod PR, NO version label = patch). Consumer follow-ups after release: chatApi bump + JSON prompt wording edits (A3 app half); webApp optional alignment bump.
