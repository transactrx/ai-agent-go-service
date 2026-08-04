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
- On fail:
  - If this cycle already **declined** a gateway candidate, skip recovery (a re-resolve would return the same declined answer). Log `[ERROR] health check FAILED for current model <id>`.
  - Else re-resolve via gateway, apply `gatewayCandidateOK`, reject candidates that `sameModel` the broken current, probe the candidate, and on pass swap via **`recoverSwap`** (see §3) — the broken model is never recorded as last-known-good. Outcome `recovered`, log `[WARN] current model <id> failed its health check, recovering` then `model updated from X to Y (resolver=recovery)`.
  - If recovery is impossible (gateway down, candidate rejected, or probe fails): `[ERROR] health check FAILED for current model <id>`, outcome `health-check-failed`. Existing rule holds: keep the current model, retry next cycle.
- Recovery resolution is two-stage exactly like the upgrade path: gateway first, catalog scan second (this diverges from powerlineAIApi, which is gateway-only — consistent with keeping our catalog fallback).

New outcomes added to the summary-line vocabulary: `recovered`, `health-check-failed`.

### 3. Last-known-good fallback at request time

State on `bedrockLLM`, guarded by the existing `modelMu`:

- `lastKnownGood string` — set by the normal upgrade swap (`swap` demotes the outgoing model into it, then promotes the candidate); **cleared/never written** by `recoverSwap` (a model that failed its health check must not become the fallback; if `lastKnownGood` equals the model being promoted by recovery, clear it).
- Accessor `lastKnownGoodModel() string` under `RLock`.

`Stream` change:

- Read `model := b.currentModel()` as today. If the initial `InvokeModelWithResponseStream` **call** returns a model-related error — AWS error codes `ValidationException` or `ResourceNotFoundException` — and `lastKnownGood` is non-empty and differs from `model`, log `[WARN] bedrock: model <cur> failed (<code>), retrying with last-known-good <lkg>` and retry the invoke once with `lastKnownGood`.
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

Unchanged prime rule: **every failure keeps the current model and retries next cycle; `runOnce` never returns an error.** Recovery is the single exception that swaps away from a failing current model, and only after a successful probe of the replacement. `Stream`'s last-known-good retry is single-shot and never loops.

## Testing

Extend the existing fake-injection tables (`autoupdate_test.go`, `gateway_test.go` untouched):

- `nextRunAt`: UTC anchor cases incl. exactly-07:00 and DST-irrelevance; jitter injected as fixed value.
- Health check: pass (no-op), fail→recover success, fail→recovery declined (probe fail), fail→gateway down, fail→candidate `sameModel` broken current, fail after same-cycle gateway decline (skip recovery).
- `recoverSwap` semantics: broken model never stored as last-known-good; clears stale equal value.
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
