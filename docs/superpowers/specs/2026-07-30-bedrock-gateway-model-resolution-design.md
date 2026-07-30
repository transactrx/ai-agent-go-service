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
- Error reply: nats-service convention — header `STATUS` carries the transport status
  (`NatsServiceError.Status`, 400/500); the finer-grained codes (4001 invalid body, 4002
  missing field, 4004 no match) are the BODY's `apiStatusCode` field, which this client does
  not read.
- Transport: plain NATS request/reply on the node's existing conn
  (`env.Host("nats")` → `NatService.GetNatsService()`), 10 s timeout — same pattern as
  `pkg/idt/validator.go`.

## Architecture

`runOnce` becomes two-stage resolution; validation, swap, notify, scheduling are untouched:

```
runOnce:
  cur = current(); parseModelID(cur)          # unparseable → skip (unchanged)
  declined = ""
  1. gateway (when resolve != nil):
       invokeId, err = resolve(ctx)
       err → warn, fall through to 2
       invokeId == cur → outcome already-latest (resolver=gateway), return
       else: gatewayCandidateOK(invokeId, cur)?        # parseable + same family
         rejected → warn, fall through to 2 (never validated)
         ok → tryUpgrade(invokeId, resolver=gateway)
           validated → swap + notify "upgraded", return
           declined  → notify "declined", declined = invokeId, fall through to 2
       ctx cancelled between stages → outcome cancelled, return
  2. fallback: ids = list(ctx); latestCandidate(ids, cur)   # unchanged strictly-newer
       none, or candidate == declined → already-latest / declined (nothing new to try)
       else → tryUpgrade(candidate, resolver=fallback)
  tryUpgrade: validate candidate (3 attempts, 5s/15s backoffs, ping, no temperature)
       ok → swap + notify "upgraded"; else notify "declined"
```

Up to two notify events per cycle are possible — a declined gateway candidate followed by an
upgraded/declined fallback outcome — by design, so both are visible.

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
  summary log line and events. A declined gateway candidate falls through to the catalog scan
  in the same cycle instead of ending it; if the scan's candidate is the same ID already
  declined, it is not re-validated.
- `gatewayCandidateOK(id, current parsedModel) error` guards gateway candidates before
  validation: rejects IDs outside the modern parseable naming and cross-family answers, so an
  unmanageable or wrong-family gateway answer never reaches the ping-validate/swap path.
- `tryUpgrade(ctx, cur, cand, resolver string, outcome *string) bool` extracts the
  validate-with-retries → swap|decline → notify sequence shared by both stages; `runOnce` calls
  it once per candidate it decides to try.
- `noteEvent` gains additive `"resolver"` field. Up to two notify events per cycle are now
  possible (gateway declined + fallback upgraded/declined).
- `startAutoUpdate` wires `resolve` from the env-resolved subject + NATS conn (via
  `newGatewayResolve`); logs the gateway subject when enabled, or the reason it is
  log/scan-only.
- `hostLookup` interface (`Host(kind string) (any, bool)`) narrows `natsConn`'s parameter to
  just what it needs, so tests can fake it without implementing the full `node.NodeEnv`;
  `node.NodeEnv` satisfies it implicitly.
- `newGatewayResolve(nc, subject, current func() string) func(ctx) (string, error)` extracts
  the stage-1 resolver closure construction out of `startAutoUpdate` so it is independently
  testable.
- `cfg.Model` doc comment updated: configured starting model; gateway is the source of truth
  thereafter.

**Unchanged**: startup + daily 02:00 schedule, ping validation (3 attempts, no temperature),
swap mutex semantics (in-flight requests keep their model), notify plumbing
(`MODEL_AUTOUPDATE_NOTIFY_SUBJECT` / `<nats_basePath>.modelAutoUpdate`), `Config` shape.

## Error handling

`runOnce` still never returns an error: every failure keeps the current model until the next
cycle. Gateway failure modes — env unset (default subject has no responder), timeout, non-200
reply, empty `invokeId`, malformed JSON, an unparseable or cross-family candidate
(`gatewayCandidateOK`) — each log one warning and fall back to the scan. A gateway candidate
that IS parseable/same-family but fails ping validation (3 attempts) is also not a dead end: it
is declined (notified) and the cycle falls through to the catalog scan, so a bad gateway answer
can never mask an upgrade the scan would otherwise find. If the scan's own candidate is the same
ID already declined, it is skipped rather than re-validated. After a gateway-issued prefix
change (`us.` → `global.` or on-demand no-prefix), the next cycle's family derivation still
works (`parseModelID` accepts all three shapes) and the fallback scan compares within the new
prefix. Cancellation is checked between stages, and inside `tryUpgrade`'s validation loop, so a
cancelled cycle cannot be misreported as a completed decline.

## Deployment / env wiring

This repo ships a library; the consuming project deploys the binary and its environment
(docs/DEPLOYMENT.md). Deliverables:
- `docs/DEPLOYMENT.md` env table: add `INFERENCE_GATEWAY_BASE_PATH` (optional, default
  `example.inferenceGateway`, org value `trx.inferenceGateway`) — consuming services set it in
  their terraform task definitions / GitHub environment variables, whichever drives their env.
- `docker-compose.yml`: no change — the compose file only runs NATS + the test
  suite (no agent service); the placeholder default already makes local runs
  fall back to the catalog scan.
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
