# Bedrock Auto-Update Hardening — Design

**Date:** 2026-08-04
**Repos:** `ai-agent-go-service` (all code), `opensearchAiChatApi` (dep bump + docs + terraform only)
**Target version:** v1.3.0

## Goal

Port the four operational-hardening behaviors from powerlineAIApi's `internal/modelupdate/` into the existing per-node auto-updater in `pkg/workflow/builtin/bedrock/`, without changing the framework design (per-node config, gateway-first resolve, catalog-scan fallback, echo-tool validation probe, hot swap, NATS notifications all stay as-is).

## Context (current state, v1.2.1)

The `ai/bedrock` node's updater (`autoupdate.go`) runs at startup + daily at 02:00 server-local. Each cycle: resolve via `<INFERENCE_GATEWAY_BASE_PATH>.resolveModel` → same-family gate → echo-tool probe (3 attempts, 5s/15s backoff) → mutex-guarded swap read once per request in `Stream` → notify `<basePath>.modelAutoUpdate`. Gateway failure falls through to a Bedrock `ListInferenceProfiles` catalog scan. Any failure keeps the current model.

Gaps vs powerlineAIApi: server-local schedule with no jitter; only candidates are probed (never the current model); no request-time fallback if a fresh model breaks mid-day; no operator break-glass short of editing workflow JSON and redeploying.

## Non-goals

- No engine-level "model update process" or `node.ModelUpdatable` interface (revisit later).
- No push-based gateway broadcasts (gateway is org-owned; pull/poll stays).
- No production-prompt probes (app-specific; the echo probe is the right generic choice).
- No `tool/opensearch` `tlsServerName` change (separate cycle).
- No removal of the catalog-scan fallback (deliberate divergence from powerlineAIApi).

## Design

All changes are additive inside `pkg/workflow/builtin/bedrock/`. The `autoUpdater` struct keeps its injected-effects style (`current`, `swap`, `resolve`, `list`, `validate`, `publish`, `now`, `sleep`); new effects are added as func fields the same way so `runOnce` stays testable without AWS/NATS.

### 1. Schedule: 07:00 UTC + jitter

- Replace `checkHour = 2` (server-local) with `anchorHourUTC = 7` computed in `time.UTC`, matching powerlineAIApi's `nextRunAt`: next strictly-future 07:00 UTC.
- Add `jitter func() time.Duration` field; production value `rand.Int63n(30min)`, freshly drawn each cycle. Tests inject zero.
- Startup `runOnce` remains immediate and un-jittered.
- Each `ai/bedrock` node instance draws its own jitter, so multiple workflows in one process also stagger against each other.

### 2. Daily health check of the current model + recovery

Appended to `runOnce` after the resolve/upgrade stage, every cycle — except cycles whose outcome is `upgraded` (the model in use was just validated 3× seconds earlier; re-probing is pure cost):

- Probe `current()` with the existing `validateModel` (3 attempts, 5s/15s).
- On pass: nothing new logged beyond the existing summary line.
- On fail, the probe error is **classified** first — recovery and alerting are reserved for failures that definitively indict the model:
  - **Conclusive**: the model is gone or unentitled (AWS `ValidationException` / `ResourceNotFoundException`, i.e. `isModelUnavailable`), or the model responded but violated the probe's tool contract (no/invalid `tool_use` — wrapped as `errProbeContract` by `validateModel`).
  - **Inconclusive** (everything else: `ThrottlingException`, timeouts, 5xx, transport errors): outcome `health-check-inconclusive`, log `health check inconclusive for current model <id> (transient error, keeping model): <err>`, **no notification, no recovery**, keep the model. A per-model throttle must never move live traffic onto a model the gateway never approved.
- On a **conclusive** failure:
  - If this cycle already **declined** a candidate (either resolve stage), skip recovery (a re-resolve would return the same declined answer). Log `health check FAILED for current model <id>: <err>`.
  - Else re-resolve via gateway, apply `gatewayCandidateOK`, reject candidates that `sameModel` the broken current (or the candidate declined this cycle), probe the candidate, and on pass swap via **`recoverSwap`** (see §3) — the broken model is never recorded as last-known-good. Outcome `recovered`, log `current model <id> failed its health check, recovering` then `model updated from X to Y (resolver=recovery)`.
  - If recovery is impossible (gateway down, candidate rejected, or probe fails): `health check FAILED for current model <id>: <err>`, outcome `health-check-failed`. When a recovery candidate was probed and also failed, the logged/notified error carries **both** causes (`health check: <healthErr>; recovery candidate <id> also failed: <candErr>`). Existing rule holds: keep the current model, retry next cycle.
- Recovery resolution is two-stage exactly like the upgrade path: gateway first, catalog scan second (this diverges from powerlineAIApi, which is gateway-only — consistent with keeping our catalog fallback).
- A conclusively-failed current model stays flagged (`curUnhealthy`) until it is replaced: if a **later** cycle finds an upgrade, that upgrade is promoted with `recoverSwap`, not `swap`, so the broken model can never be recorded as last-known-good after the fact. The flag clears on a passing health check, a successful recovery, or a swap.
- **Cancellation** (shutdown) is never a verdict: when the context is cancelled during a 3-attempt loop the outcome is `cancelled` and no `declined` / `health-check-failed` notification is emitted.
- The `declined` guard covers **both** resolve stages — a candidate rejected by the gateway stage *or* by the catalog stage is remembered for the rest of the cycle, so it is not re-probed by the health check's recovery.
- Model comparisons use `sameModel` (parsed), not string equality: the bare and dated forms of one release (`…claude-opus-4-7` vs `…claude-opus-4-7-20260101-v1:0`) are the same model, so a gateway answer naming the current release ends the cycle `already-latest` instead of being probed as a candidate.

New outcomes added to the summary-line vocabulary: `recovered`, `health-check-failed`, `health-check-inconclusive`.

### 3. Last-known-good fallback at request time

State on `bedrockLLM`, guarded by the existing `modelMu`:

- `lastKnownGood string` — set by the normal upgrade swap (`swap` demotes the outgoing model into it, then promotes the candidate); **cleared/never written** by `recoverSwap` (a model that failed its health check must not become the fallback; if `lastKnownGood` equals the model being promoted by recovery, clear it).
- Accessor `lastKnownGoodModel() string` under `RLock`.

`Stream` change:

- Read `model := b.currentModel()` as today. If the initial `InvokeModelWithResponseStream` **call** returns a model-related error — AWS error codes `ValidationException` or `ResourceNotFoundException` — and `lastKnownGood` is non-empty and differs from `model`, log `model <cur> failed (<err>), retrying with last-known-good <lkg>` (the full error, not just the code) and retry the invoke once with `lastKnownGood`.
- The retry applies only before any `LLMEvent` has been emitted (the invoke call itself failing). Mid-stream errors are not retried — streaming semantics unchanged.
- Errors other than the two listed codes are returned as today (the engine-level retry policy still applies unchanged around `Stream`).

State is in-memory: a container restart clears `lastKnownGood`; the startup run immediately re-resolves. Accepted, same as powerlineAIApi.

### 4. Break-glass env pin: `AI_BEDROCK_MODEL_ID`

- Read once in `Init` (before `startAutoUpdate`). When non-empty:
  - `b.model` is set to the env value verbatim (overrides the workflow JSON `model`).
  - The updater goroutine is never started, regardless of `autoUpdate` in JSON.
  - One banner log per node: `bedrock: model pinned to <id> via AI_BEDROCK_MODEL_ID — auto-update disabled`.
- Process-wide by design (all `ai/bedrock` nodes in the process), matching powerlineAIApi. Per-node `"autoUpdate": false` continues to work independently.
- No length/format validation beyond trim + non-empty (operator-controlled; same trust level as powerlineAIApi).

### 5. Notifications

`noteEvent` gains two event values: `recovered` (fields as `upgraded`, `resolver:"recovery"`) and `health-check-failed` (`from` = current model, `to` empty, `error` set). Payload shape otherwise unchanged; consumers of `<basePath>.modelAutoUpdate` are backward compatible.

`health-check-failed` fires **only on conclusive** probe failures (§2): an inconclusive failure (throttle, timeout, 5xx) publishes nothing — it is a fault of the moment, not of the model, and alerting on it would train operators to ignore the event. Cancellation (shutdown) likewise publishes nothing.

## Config / env surface (after)

| Item | Where | Default | Notes |
|---|---|---|---|
| `model` | workflow JSON node config | required | starting model |
| `autoUpdate` | workflow JSON node config | `true` | per-node opt-out |
| `INFERENCE_GATEWAY_BASE_PATH` | env | `example.inferenceGateway` (placeholder → gateway off) | org value `trx.inferenceGateway` |
| `MODEL_AUTOUPDATE_NOTIFY_SUBJECT` | env | `<NATS_BASE_PATH>.modelAutoUpdate` | unchanged |
| `AI_BEDROCK_MODEL_ID` | env | `""` | **new** — pin + disable updater, process-wide |

Schedule constants (07:00 UTC, 30 min max jitter, 3 probe attempts, 5s/15s backoffs) are code constants, not config — same stance as both existing implementations.

## Error handling summary

Unchanged prime rule: **every failure keeps the current model and retries next cycle; `runOnce` never returns an error.** Recovery is the single exception that swaps away from a failing current model, and only after a **conclusive** probe failure (§2) plus a successful probe of the replacement. `Stream`'s last-known-good retry is single-shot and never loops.

The catalog scan is bounded so a control-plane fault cannot stall the updater for the life of the process: a 60s timeout per scan and a 50-page pagination cap (exceeding it fails the scan → `list-failed`, retry next cycle). If the updater goroutine ever dies, that is logged loudly (`autoupdate: goroutine terminated, auto-update DISABLED for this process`) instead of being silently discarded.

## Testing

Extend the existing fake-injection tables (`autoupdate_test.go`, `gateway_test.go` untouched):

- `nextRunAt`: UTC anchor cases incl. exactly-07:00 and DST-irrelevance; jitter injected as fixed value.
- Health check: pass (no-op), conclusive fail→recover success, fail→recovery declined (probe fail), fail→gateway error falls to catalog scan, fail→candidate `sameModel` broken current, fail after same-cycle decline (skip recovery), **inconclusive fail → no recovery, no event, outcome `health-check-inconclusive`**.
- Error classification (`healthCheckConclusive`): model-unavailable and tool-contract errors conclusive; throttle/timeout/transport inconclusive.
- Cancellation during the final validation attempt → outcome `cancelled`, no notification.
- `recoverSwap` semantics: broken model never stored as last-known-good; clears stale equal value; a later cycle's upgrade after a conclusive failure also promotes via `recoverSwap`.
- `sameModel` at the gateway-answer comparison: dated form of the current release → `already-latest`, candidate never probed.
- Env pin: set → no updater started, model overridden, banner logged once; unset → unchanged behavior.
- `Stream` fallback: `ValidationException` before first event → one retry with lkg; other codes → no retry; lkg empty/equal → no retry; mid-stream error → no retry. Fake bedrock client via the existing seam.
- Notification: `recovered` / `health-check-failed` payloads.

## Consumer repo changes (opensearchAiChatApi)

1. `go.mod`/`go.sum`/`vendor`: bump to v1.3.0.
2. Docs: add `INFERENCE_GATEWAY_BASE_PATH` and `AI_BEDROCK_MODEL_ID` to `.envTemplate` and `docs/configuration.md` (the former is currently undocumented there).
3. Terraform: `variable "AI_BEDROCK_MODEL_ID"` (default `""`) → ECS container env; export in both deployment workflows.
4. Workflow JSONs: no changes.
5. Operator checklist (not code): verify GitHub environment var `INFERENCE_GATEWAY_BASE_PATH` is set to `trx.inferenceGateway` in Development/Production (not visible from the repo).

## Compatibility

- No breaking changes to `Config`, node contracts, engine, or NATS payloads.
- Consumers that never set the new env var and never read the new notify event values see identical behavior except the schedule shift (02:00 server-local → 07:00 UTC + jitter) — called out in the release notes.
