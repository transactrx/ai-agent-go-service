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
  "category": "Claims",
  "version": "1.0.0",
  "tags": ["powerline", "claims"],
  "skills": [{"name": "claim-search", "description": "Search and aggregate claims", "examples": ["How many claims were rejected today?"]}],
  "askUser": false
}
```

All fields optional. Defaults: `name` = workflow id, `description` = workflow
description, `displayName` = name, `version` = `RSASSISTANT_AGENT_VERSION` or
`1.0.0`. `category` (free string, short) groups the agent in RSAssistant's
"Your specialists" panel; empty = ungrouped. `name` must match `^[a-zA-Z0-9_-]+$`. A block on a base workflow is
inherited by every workflow that `extends` it; the derived file may override
fields or set `"enabled": false`. Duplicate names: first workflow id (sorted)
wins, others are skipped.

## Asking the user (`rsassistant.askUser`, opt-in)

`askUser: true` lets the workflow's model ask the RSAssistant user with the
standard `request_user_input` tool (nats-agent protocol 1.7, `pkg/ask`). It is
separate from PowerLine's web UI tools (`tool/ui-*`), which stay web-only
(rule 3 above).

**Before opting in, update the workflow's own prompt.** The library never
rewrites a workflow's instructions. A prompt that forbids follow-up questions
(e.g. "There is no UI … never wait for user input") makes the model refuse the
offered tool. In the same change that sets `askUser: true`, allow it, e.g.:
"If request_user_input is available, use it when the request is ambiguous;
otherwise state the interpretation you chose and answer."

- Card: `capabilities.userInput`, `progress` and `renew` (RSAssistant needs all
  three). Without `askUser` the card is exactly as before.
- Per turn: only when the caller sent `userInput.supported` (RSAssistant web
  turns) does the bridge set `"askUser": true` on the workflow request. Only a
  `trigger/nats-chat` endpoint that RSAssistant enabled at startup (an
  `askUser: true` workflow) honors it: its streaming path then offers
  `request_user_input` (name, description + `ask.Guidance`, input schema) to
  the run's agent loop as a client-only tool. Only the exact key `askUser` with
  the value `true` counts; any other value is ignored (never a 400). Every
  other endpoint ignores the flag and strips every case variant of the key
  (`askUser`, `AskUser`, `ASKUSER`, ...) from the body before the run, so
  nothing changes for it. On an enabled endpoint the key stays in the body, so
  templates that render the whole trigger body see `"askUser": true`.
  A workflow that already has a tool named `request_user_input` keeps it and
  gets no `askUser` (`event=ask_user.off`).
- The bridge answers each call: `ask.ParseAsk` → `Stream.AskUser`; results as
  RSAssistant's own tool: the answer JSON, `{"status":"skipped"}`,
  `{"status":"not_answered","userWrote":"..."}` when the user typed instead;
  "can't ask the user here" or a malformed ask is a tool error.
- Live wait: `AGENT_ASK_WAIT` (default 5m), capped 10s below the shorter of the
  agent node's `humanResponseTimeoutSeconds` and the trigger's
  `requestTimeoutSeconds`. The request timeout also counts the run's work before
  the ask, so give it room for the expected think time. If the workflow run
  ends anyway while a form is open (timeout, error), the bridge ends the ask at
  once (the card closes as parked), publishes nothing to the ended run and
  relays the run's end.
- No answer within the live wait: the ask parks and the turn ends
  `awaiting_input`. The workflow gets `{"_parked":true}`: its agent loop saves
  the turn (the user's request plus "I asked the user: <title>. Waiting for
  their answer.") to its memory and ends at once, running nothing more. If it
  doesn't end within 30s, the bridge cancels it. On the next turn the answer
  arrives as context (`[Earlier you asked the user: …]`); with memory the model
  also has the original request. A workflow without a memory node keeps no
  history, so it gets the answer without the original request.
- `askUser` on a direct request to an enabled endpoint is not authenticated; it
  only offers the fixed tool, whose result comes from that same caller's stream.

Known limits:
- If the workflow run fails while a form is open (request timeout, error), the
  card is closed as parked but the workflow wrote nothing to memory. Answering
  it later starts a turn whose model sees the answer without the request that
  led to the ask. Rare: needs a run failure mid-ask.
- The session drain lock that makes a resume wait for a parked run is per
  process. A resume that lands on another replica within milliseconds of the
  park could load memory before the parked turn is saved.

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
| `IDT_VALIDATE_CACHE_SECONDS` | `0` (off) | reuse a successful validation of the same token + function for this many seconds (`pkg/idt`). Valid `0`–`3600`; an invalid or negative value logs a `WARNING` and keeps the cache off; above `3600` logs a `WARNING` and is capped at `3600`. Denies and observe-only results are never cached. A cached allow keeps working for up to this many seconds after the grant is revoked (revocation lag) |
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

- `RSASSISTANT event=agent.published workflow= name= subject= mode= observe_only= function= user_input=`
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
