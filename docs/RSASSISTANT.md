# Publishing workflows to RSAssistant

When `RSASSISTANT_ENABLED=true`, the service publishes every **eligible** workflow
as its own agent on the NATS agent mesh (`nats-agent` protocol v1.1). RSAssistant
discovers each one on `trx.agent.discover`, shows it to users who hold the
agent's identity function (the workflow's index function when it has one, otherwise
the service's `APP_FUNCTION_ID`), and consults it on `trx.agent.<name>.chat`.

## Eligibility (decided at startup from the loaded workflow)

1. Trigger is `trigger/nats-chat`.
2. `responseMode` is `streaming`, or `allowResponseModeOverride` is `true`.
3. No client-only UI tool nodes (`tool/ui-*`). Machine callers cannot answer them.
4. `rsassistant.enabled` is not `false`.

Skipped workflows are logged: `RSASSISTANT event=workflow.skipped workflow=<id> reason="..."`.

## Optional manifest (top-level `rsassistant` in the workflow JSON)

```json
"rsassistant": {
  "enabled": true,
  "name": "powerlineClaimSearch",
  "displayName": "PowerLine Claim Search",
  "description": "Answers questions about PowerLine pharmacy claims.",
  "version": "1.0.0",
  "tags": ["powerline", "claims"],
  "skills": [{"name": "claim-search", "description": "Search and aggregate claims", "examples": ["How many claims were rejected today?"]}]
}
```

All fields optional. Defaults: `name` = workflow id, `description` = workflow
description, `displayName` = name, `version` = `RSASSISTANT_AGENT_VERSION` or
`1.0.0`. `name` must match `^[a-zA-Z0-9_-]+$`. A block on a base workflow is
inherited by every workflow that `extends` it; the derived file may override
fields or set `"enabled": false`. Duplicate names: first workflow id (sorted)
wins, others are skipped.

## Identity

Every card declares `access{appId: APP_ID, functionId: <function>}`. `<function>` is the
workflow's **index function** when it has one (see "Per-index function" below) and
`APP_FUNCTION_ID` otherwise. The agent validates `X-TRX-IDT` with identity before
acknowledging (403 otherwise).
The bridge then calls the workflow with `X-Account-Id` / `X-User-Id` taken **only**
from the verified identity and forwards `X-TRX-IDT`. The workflow's own scope
policy runs unchanged. `IDT_OBSERVE_ONLY=true` (the same switch the trigger's token
check reads) makes the nats-agent layer log instead of block, but the bridge still
refuses any consult whose identity did not resolve to an account and a user. The
former `RSASSISTANT_IDT_OBSERVE_ONLY` is retired and ignored.

## Per-index function

A workflow whose `tool/opensearch` policy peer implements `node.IndexFunctionProvider`
gets its function from that policy node, once, when the workflow loads (the engine logs
`engine: workflow=<id> index=<pattern> access=bound|unbound|none function=<id> cause=<text>`).

| access | agent card and agent token check | trigger token check |
|---|---|---|
| `bound` | that function | that function |
| `unbound` | **not published**, `workflow.skipped … reason="index function unbound: <cause>"` | denied `INDEX_FUNCTION_UNBOUND` without asking identity; logged as `IDT_TRACE event=validate.observed ... would_allow=false reason="INDEX_FUNCTION_UNBOUND"` while `IDT_OBSERVE_ONLY=true` |
| `none` | `APP_FUNCTION_ID` | `APP_FUNCTION_ID` |

The trigger token check only runs when `IDT_VALIDATION=true`.
A change to the function takes effect after a restart. See `docs/EXTENDING.md` for the interfaces.

## Environment

| Var | Default | Meaning |
|---|---|---|
| `RSASSISTANT_ENABLED` | `false` | master switch |
| `IDT_OBSERVE_ONLY` | `false` | observe phase for the agent token check — shared with the trigger (`pkg/idt`); `RSASSISTANT_IDT_OBSERVE_ONLY` is retired |
| `IDT_VALIDATE_CACHE_SECONDS` | `0` (off) | reuse a successful validation of the same token + function for this many seconds (`pkg/idt`). Denies and observe-only results are never cached |
| `RSASSISTANT_CONSULT_TIMEOUT_SECONDS` | `400` | per-consult timeout (below RSAssistant's 420s) |
| `RSASSISTANT_AGENT_VERSION` | `1.0.0` | default card version |
| `APP_ID`, `APP_FUNCTION_ID` | required | card `appId`; `functionId` fallback for workflows without an index function |
| `NATS_IDENTITY_BASE_PATH`, `NATS_IDENTITY_VALIDATE_SUBJECT` | `trx.identityservice`, `validateInternalToken` | identity subject |
| `NATS_URL`, `NATS_JWT`, `NATS_KEY` | required | agent connections (same NATS user as the engine) |

NATS permissions (same user as the engine), for each published agent name `<name>`:

| Direction | Subjects | Why |
|---|---|---|
| sub | `trx.agent.discover` | discovery (queue group `agent.<name>`) |
| sub | `trx.agent.<name>.>` | `card`, `ping`, `chat`, `cancel`, `_api_docs`, `_stats` |
| sub + pub | `trx.agent.<name>_.>` | nats-service chunked large responses |
| pub | `trx.agent.<name>.>` | same pattern other agents use (e.g. copayAssistant) |
| pub + sub | `_INBOX.>` | replies, RSAssistant stream subject, identity replies |
| pub | `<NATS_IDENTITY_BASE_PATH>.>` (`trx.identityservice.>`) | IDT validation |
| sub | `_discovery.all` | nats-service discovery (engine already needs it) |
| pub | `<NATS_BASE_PATH>.>`, `<NATS_BASE_PATH>_.>` | bridge → workflow chat subject and stream cancel |

Grant per name rather than `trx.agent.>`: a broad `sub` would let the service
receive other agents' traffic. A newly published workflow name needs its own entries.

## Logs

- `RSASSISTANT event=agent.published workflow= name= subject= mode= observe_only=`
- `RSASSISTANT event=workflow.skipped workflow= reason=`
- `RSASSISTANT event=consult.start|refused|done|error name= run= ...`
- nats-agent: `IDT_METRIC event=validate.allow|deny|observe ...`

## Bridge behavior worth knowing

- The bridge uses its own natsstream client (`pkg/rsassistant/stream.go`): a
  pre-stream rejection from the workflow (nats-service error reply, e.g. a 400/403)
  or a NATS no-responders notice ends the consult immediately with an upstream
  error instead of waiting for the consult timeout.
- Attachments are relayed as `data` events; a `url`/`imageUrl` in the payload is
  also appended to the text, because RSAssistant only keeps text.
- Tool outputs that embed URLs relative to a web app (for example a chart URL built
  from a `chartUrlPrefix`) are relayed verbatim and are only resolvable from that
  web app.

## Requirements

Go 1.27.0 or newer (module `go` directive; nats-agent v0.2.0 itself needs 1.26.5).
