# Bedrock model resolution via org inferenceGateway — design

Date: 2026-07-30
Status: approved

## Problem

The `ai/bedrock` node's auto-updater (`pkg/workflow/builtin/bedrock/autoupdate.go`) discovers
newer Claude models by scanning the Bedrock control plane itself (`ListInferenceProfiles` +
same-prefix/family strictly-newer parsing). The org now has a dedicated source of truth for
"latest release of a model family": the **inferenceGateway** NATS service
(github.com/transactrx/inferenceGateway), already consumed the same way by
batchAiIntegrationTestJob. This service should ask the gateway first and keep the scan only as
a fallback.

## Decisions (user-approved)

1. **Gateway primary, catalog-scan fallback** — gateway outage or missing wiring never blocks
   updates; behavior degrades to exactly today's logic.
2. **Lab/family derived from the node's current model** — no new node-config fields. E.g.
   `us.anthropic.claude-opus-4-7` → request `{"lab":"anthropic","family":"claude-opus"}`
   (gateway tokenizer splits dashes; matches catalog family `claude opus`).
3. **Follow gateway always** — when the gateway's `invokeId` differs from the current model
   (upgrade, org rollback, or geo-prefix change), it becomes the candidate. Every swap is still
   gated by the existing ping validation. The fallback scan keeps its strictly-newer rule.
4. **Generic subject default + env override** (repo convention, cf. commit e38389b):
   `INFERENCE_GATEWAY_BASE_PATH`, default `example.inferenceGateway`; request subject is
   `<basePath>.resolveModel`. Org deployments set it to `trx.inferenceGateway`.
5. **Approach A** — injected resolver func on `autoUpdater` (repo's established test seam),
   not a resolver interface; no separate Init-time resolution path (the updater already runs
   once at startup).

## Gateway wire contract (verified against inferenceGateway source)

- Subject: `<basePath>.resolveModel` (gateway deploys `trx.inferenceGateway`).
- Request: `{"lab":"anthropic","family":"claude-opus"}`. Family words must all appear in the
  catalog family; a version prefix pins a line ("opus 4.5").
- Success reply: `ModelInfo` JSON; `invokeId` is the exact ID inference must use (on-demand
  model ID, or `us.`/`global.` cross-region profile ID). May be empty → not invocable.
- Error reply: nats-service convention — `STATUS` header ≠ "200" (e.g. 4001 invalid body,
  4002 missing field, 4004 no match) with `NatsServiceError` JSON body.
- Transport: plain NATS request/reply on the node's existing conn
  (`env.Host("nats")` → `NatService.GetNatsService()`), 10 s timeout — same pattern as
  `pkg/idt/validator.go`.

## Architecture

`runOnce` becomes two-stage resolution; validation, swap, notify, scheduling are untouched:

```
runOnce:
  cur = current(); parseModelID(cur)          # unparseable → skip (unchanged)
  1. gateway (when resolve != nil):
       invokeId, err = resolve(ctx)
       err/empty → warn, fall through to 2
       invokeId == cur → outcome already-latest (resolver=gateway), return
       else candidate = invokeId, resolver=gateway
  2. fallback: ids = list(ctx); latestCandidate(ids, cur)   # unchanged strictly-newer
       none → already-latest
  3. validate candidate (3 attempts, 5s/15s backoffs, ping, no temperature)
       ok → swap + notify "upgraded"; else notify "declined"
```

## Components

**New `pkg/workflow/builtin/bedrock/gateway.go`**
- `resolveViaGateway(ctx, nc *nats.Conn, subject, lab, family string) (string, error)` — one
  `RequestWithContext` (10 s), returns `invokeId`.
- `parseResolveReply(status string, body []byte) (string, error)` — pure function: non-200
  status → error carrying body text; missing/empty `invokeId` → error. Unit-testable without
  a conn.
- `deriveGatewayQuery(modelID string) (lab, family string, err error)` — wraps `parseModelID`;
  lab is `anthropic` (the only lab `parseModelID` accepts), family e.g. `claude-opus`.

**`autoupdate.go` changes**
- `autoUpdater` gains `resolve func(ctx context.Context) (string, error)`; nil = gateway
  disabled (missing env default is fine — the request just fails and falls back; nil is for
  missing NATS conn).
- `runOnce` orchestrates the two stages and tracks `resolver` (`gateway`/`fallback`) for the
  summary log line and events.
- `noteEvent` gains additive `"resolver"` field.
- `startAutoUpdate` wires `resolve` from the env-resolved subject + NATS conn; logs the
  gateway subject when enabled, or the reason it is log/scan-only.
- `cfg.Model` doc comment updated: configured starting model; gateway is the source of truth
  thereafter.

**Unchanged**: startup + daily 02:00 schedule, ping validation (3 attempts, no temperature),
swap mutex semantics (in-flight requests keep their model), notify plumbing
(`MODEL_AUTOUPDATE_NOTIFY_SUBJECT` / `<nats_basePath>.modelAutoUpdate`), `Config` shape.

## Error handling

`runOnce` still never returns an error: every failure keeps the current model until the next
cycle. Gateway failure modes — env unset (default subject has no responder), timeout, non-200
reply, empty `invokeId`, malformed JSON — each log one warning and fall back to the scan.
After a gateway-issued prefix change (`us.` → `global.` or on-demand no-prefix), the next
cycle's family derivation still works (`parseModelID` accepts all three shapes) and the
fallback scan compares within the new prefix.

## Deployment / env wiring

This repo ships a library; the consuming project deploys the binary and its environment
(docs/DEPLOYMENT.md). Deliverables:
- `docs/DEPLOYMENT.md` env table: add `INFERENCE_GATEWAY_BASE_PATH` (optional, default
  `example.inferenceGateway`, org value `trx.inferenceGateway`) — consuming services set it in
  their terraform task definitions / GitHub environment variables, whichever drives their env.
- `docker-compose.yml`: local example value for the agent service.
- No terraform exists in this repo; nothing to change here beyond docs.

## Testing

Extend `autoupdate_test.go` (injected-func harness):
- gateway answer differs → validated → swapped; event `resolver=gateway`.
- gateway answer == current → no validation call; outcome already-latest.
- gateway error → fallback scan path used; event `resolver=fallback`.
- nil `resolve` → behavior identical to today (regression guard; existing tests unchanged).
- gateway candidate fails validation ×3 → declined, current model kept.
- `parseResolveReply` table: 200+invokeId, 200+empty invokeId, 4004 error body, malformed JSON.
- `deriveGatewayQuery` table: `us.` / `global.` / no-prefix / legacy-unparseable IDs.
