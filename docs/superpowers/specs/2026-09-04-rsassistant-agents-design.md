# RSAssistant agents: publish eligible workflows on the NATS agent mesh

**Date:** 2026-09-04
**Library:** github.com/transactrx/ai-agent-go-service — target release v1.7.0
**First consumer:** github.com/transactrx/opensearchAiChatApi (see its adoption spec)
**Status:** design approved in conversation; awaiting plan review

## 1. Problem

RSAssistant discovers specialist agents over NATS using the `nats-agent` protocol
(`trx.agent.discover`, one card per agent, `trx.agent.<name>.chat`), and shows a
user only the agents whose identity function that user holds. Services built on
this library (opensearchAiChatApi today) already run one or more chat workflows
over NATS, but speak a different protocol (`<NATS_BASE_PATH>.<workflowId>` with
`X-Account-Id` / `X-User-Id` headers and the `natsstream` event protocol). They are
invisible to RSAssistant.

The library must let every **eligible** workflow appear to RSAssistant as its own
agent, driven purely by workflow configuration, without changing how existing
callers (the powerline webapp) reach those workflows, and without ever answering a
user with data the workflow's own security scope would not give them.

## 2. Verified facts the design rests on

| Fact | Where |
|---|---|
| RSAssistant polls `trx.agent.discover`, builds `ask_<card.name>` tools, filters cards per user by `access.functionId` via identity | RSAssistant `pkg/orchestrator/roster.go:102`, `agenttool.go:37`, `pkg/agentaccess/agentaccess.go:191-294` |
| RSAssistant's consult concatenates `text` deltas, forwards `toolUse`/`status`, fails on `error`, reads `done.stopReason`; ignores `toolResult` and `data` | RSAssistant `pkg/orchestrator/agenttool.go:105-145` |
| nats-agent v0.2.0: N agents per process OK, own NATS conn each from `NATS_URL`/`NATS_JWT`/`NATS_KEY`, per-agent queue group on discover, `Start()` non-blocking, `Shutdown()` drains | `nats-agent@v0.2.0/pkg/agent/agent.go:144-166, 258-377` |
| nats-agent validates `X-TRX-IDT` before ack (403 `4031`), overwrites `UserID` with verified id; missing token is always denied when enabled and not observe; observe returns empty identity on deny and `Verified=false` on allow | `nats-agent@v0.2.0/pkg/agent/idt.go:221-318`, `agent.go:425-427` |
| nats-agent `Config` accepts explicit `Access *wire.AgentAccess` and `IDTValidation *IDTValidation`; when explicit, env `APP_ID`/`APP_FUNCTION_ID`/`IDT_*` are not read | `agent.go:58-81, 132-142` |
| Engine loader is tolerant of unknown top-level keys but drops them; `MergeDerived` deep-merges every top-level key except `nodes`, `connections`, `extends` | `pkg/workflow/engine/loader/loader.go:85`, `derive.go:113-115` |
| Engine `Run` blocks on SIGTERM after `natservice.Start()`; the engine handle `eng` is local to `Run` | `agent/service.go:93-152` |
| `trigger/nats-chat` honors body `responseMode` only when `allowResponseModeOverride` is true | `pkg/workflow/builtin/natschat/trigger.go:135-146` |
| Client-only UI tools implement `node.ClientUITool` with `ClientOnly()==true`; the agent loop blocks waiting for a client tool result for them | `pkg/workflow/node/clientuitool.go:8-11`, `builtin/agent/loop.go:447-452` |
| Streaming client `natsstream.DoStreamingRequest(nc, subject, headers, body, timeout, logger)` delivers events on a channel; cancel via `sub.Cancel()` after the start event | `pkg/transport/natsstream/client.go:39-152` |
| Engine stream payloads: `delta {text}`, `tool_call {toolUseId,name,input}`, `tool_result {toolUseId,output,isError}`, `attachment {toolUseId,kind,payload}`, `complete {finalText,messageStop}`, `error {code,message}` | `builtin/agent/loop.go:271-283, 430-436, 521-526, 565`, `natsstream/protocol.go:51-68` |
| Account scoping for OpenSearch workflows is structurally mandatory and fail-closed on empty account | `builtin/opensearch/factory.go:92`, `opensearch/tool.go:47-64` |

## 3. Design

### 3.1 One nats-agent per eligible workflow

At the end of `Run`, after `natservice.Start()`, when `RSASSISTANT_ENABLED=true`,
the new package `pkg/rsassistant` walks `eng.WorkflowIDs()` and, for each eligible
workflow, creates one `nats-agent` agent whose `OnChat` handler bridges into the
workflow's existing NATS subject. Each agent answers discovery independently with
its own card. Nothing is registered when the flag is off; existing consumers of the
library see no behavior change.

### 3.2 Eligibility (computed from loaded engine objects, not from JSON text)

A loaded `*engine.Workflow` is eligible when all hold:

1. `wf.Trigger` implements `natschat.ChatEndpoint` (new exported interface, §3.5).
2. `ep.ResponseMode() == "streaming"` **or** `ep.AllowsResponseModeOverride()`.
3. No node in `wf.Nodes` implements `node.ClientUITool` with `ClientOnly()==true`.
4. The workflow's `rsassistant` manifest (§3.3) is absent, or present with
   `enabled` not `false`.

Ineligible workflows are logged once at startup with the reason and never
published. For opensearchAiChatApi today: `powerlineSearch` and `eprescribeSearch`
fail rule 3; `SinglePowerlineSearch` and `SingleEprescribeSearch` pass and are
consulted with body `responseMode: "streaming"`.

### 3.3 Optional `rsassistant` manifest in the workflow JSON

```json
"rsassistant": {
  "enabled": true,
  "name": "powerlineClaimSearch",
  "displayName": "PowerLine Claim Search",
  "description": "Answers questions about PowerLine pharmacy claims.",
  "version": "1.0.0",
  "tags": ["powerline", "claims"],
  "skills": [
    {"name": "claim-search", "description": "Search and aggregate PowerLine claims", "examples": ["How many claims were rejected today?"]}
  ]
}
```

- Every field optional. Defaults: `name` = workflow `id`; `description` = workflow
  `description`, or `Workflow <id>` if empty; `displayName` = `name`;
  `version` = `1.0.0`; `tags`/`skills` empty; `enabled` = true.
- `name` must match nats-agent's `^[a-zA-Z0-9_-]+$` and must not be `discover` or
  `announce`; otherwise the workflow is skipped with a logged reason.
- The block goes through the same `${VAR}` substitution and sensitive-key check as
  the rest of the file. Its keys (`enabled,name,displayName,description,version,tags,skills`)
  do not match the sensitive-name regex.
- **Inheritance:** because `MergeDerived` deep-merges top-level keys, a block on a
  base workflow flows into every workflow that `extends` it. A derived file may
  override any field, including `enabled: false`.
- **Duplicate names:** two eligible workflows resolving to the same `name` would
  share a NATS queue group and load-balance silently (nats-agent performs no
  collision check). The runtime publishes the first (sorted by workflow id) and
  skips the rest with a logged reason.
- Carrying the block requires three additive fields: `rawWorkflow.RSAssistant`,
  `LoadResult.RSAssistant`, `executor.Workflow.RSAssistant`, all
  `json.RawMessage` with `omitempty`. Files without the block decode to nil.

### 3.4 Identity: one access pair, three gates

All published agents declare the same `access{appId, functionId}` taken from
`APP_ID` and `APP_FUNCTION_ID` (the service's existing identity pair, e.g.
`OPENSEARCHAICHATAPIAPPID` / `OPENSEARCHAICHATAPIFUNCTIONID`). Per-workflow functions
were rejected by the product owner: they would break the "dynamic by config" goal.
Per-account data isolation remains the workflow's job (scope policy).

Gate 1, RSAssistant: users lacking the function never see the tools.

Gate 2, nats-agent: explicit `IDTValidation{Enabled: true, ObserveOnly: <env>,
FailOpen: false, Subject: <NATS_IDENTITY_BASE_PATH>.<NATS_IDENTITY_VALIDATE_SUBJECT>,
Timeout: 5s, CacheTTL: 300s}`. In strict mode a missing, invalid, or ungranted
token is a 403 before ack.

Gate 3, the bridge handler, independent of any env var:
- strict mode (`RSASSISTANT_IDT_OBSERVE_ONLY` unset or not `true`): refuse unless
  `turn.Identity.Verified`.
- always: refuse unless `turn.Identity.AccountID != ""` and `turn.Identity.UserID != ""`.
- Refusal = `stream.Error(<reason>, 4031)`; the engine is never called.

The engine is then called with `X-Account-Id` and `X-User-Id` set **only** from
`turn.Identity`, never from `turn.UserID` as sent by RSAssistant, plus `X-TRX-IDT`
forwarded when present so the engine's own validator logs the real caller.

**Observe phase semantics.** In observe mode nats-agent returns `Identity{IDT}` with
empty user and account on any deny, and a populated identity with `Verified=false`
on allow. Gate 3 therefore still refuses every denied caller (no account), and only
relaxes the `Verified` requirement for valid, granted callers. Observe adds the
library's `IDT_METRIC event=validate.observe` log lines; it does not open data
access.

### 3.5 `natschat.ChatEndpoint` (additive accessors)

```go
// ChatEndpoint is implemented by trigger/nats-chat. Read-only view for
// out-of-band publishers (pkg/rsassistant). ChatSubject is valid after Init.
type ChatEndpoint interface {
    ChatSubject() string              // "<basePath>.<subject>"
    ResponseMode() string             // "streaming" | "single"
    AllowsResponseModeOverride() bool
    RequestTimeout() time.Duration
}
```

### 3.6 Bridge: consult → engine stream → nats-agent stream

Request body to the engine: `{"message": <joined text blocks>, "sessionId": turn.SessionID, "responseMode": "streaming"}`.
The `responseMode` field is ignored by workflows that do not allow override (they
already stream). `turn.SessionID` is the nats-agent session (fresh UUID when
RSAssistant sends none); RSAssistant stores the acked id per agent and reuses it,
so engine memory continuity per RSAssistant conversation is preserved.

Transport: `natsstream.DoStreamingRequest(agent.Conn(), ep.ChatSubject(), headers,
body, consultTimeout, logger)` on the agent's own connection (same NATS user).
`consultTimeout` defaults to 400s (`RSASSISTANT_CONSULT_TIMEOUT_SECONDS`), below
RSAssistant's 420s consult timeout and the trigger's 600s default.

Event mapping (engine → nats-agent):

| engine event | nats-agent call |
|---|---|
| `start` | remember stream id; nothing emitted |
| `delta {text}` | `stream.Text(text)` |
| `tool_call {toolUseId,name,input}` | `stream.ToolUse(id, name, input)`; remember id→name |
| `tool_result {toolUseId,output,isError}` | `stream.ToolResult(id, name, output, errText)` (errText = output when isError) |
| `attachment {toolUseId,kind,payload}` | `stream.Data("attachment", payload)`; if payload has string `url` or `imageUrl`, also `stream.Text("\n" + url + "\n")` so RSAssistant's text answer keeps it |
| `thought` | `stream.Status(text)` (never emitted today; harmless) |
| `complete {finalText,messageStop}` | if no `delta` was seen and `finalText != ""`: `stream.Text(finalText)`; then `stream.Done(mapStop(messageStop), nil)` where `max_tokens`→`maxTokens`, else `endTurn` |
| `error {code,message}` | `stream.Error(code + ": " + message, 5001)` |
| channel closed without terminator (timeout/close) | `stream.Error("workflow stream ended without completion: " + err, 5001)` |

Cancellation: when the handler `ctx` is cancelled (RSAssistant `trx.agent.<name>.cancel`
or agent shutdown), the bridge calls `sub.Cancel()` (publishes to the engine's
`_Stream_Cancel_Subject`) and `sub.Close()`, then returns; nats-agent emits
`done/cancelled`.

### 3.7 Wiring in `agent/service.go`

After `logger.Print("service started")`:

```go
var rsa *rsassistant.Runtime
if rsassistant.EnabledFromEnv(os.LookupEnv) {
    r, err := rsassistant.Start(ctx, rsassistant.Deps{
        Engine: eng, Logger: logger, RepositoryURL: s.repositoryURL, LookupEnv: os.LookupEnv,
    })
    if err != nil {
        logger.Printf("boot: rsassistant agents disabled: %v", err)
    } else {
        rsa = r
    }
}
```

and in shutdown, before `eng.Shutdown`: `if rsa != nil { rsa.Shutdown() }`.
A failure to start any single agent is logged and skipped; the engine keeps
serving. A failure in `Start` itself (e.g. missing `APP_ID`) disables the feature
with a log line; `Run` does not fail.

### 3.8 Environment contract (all new names prefixed `RSASSISTANT_`)

| Var | Default | Meaning |
|---|---|---|
| `RSASSISTANT_ENABLED` | `false` | master switch |
| `RSASSISTANT_IDT_OBSERVE_ONLY` | `false` (strict) | observe phase for the agents' IDT validation |
| `RSASSISTANT_CONSULT_TIMEOUT_SECONDS` | `400` | bridge timeout per consult |
| `RSASSISTANT_AGENT_VERSION` | `1.0.0` | default card version when manifest has none |
| reused: `APP_ID`, `APP_FUNCTION_ID` | required when enabled | card `access` |
| reused: `NATS_IDENTITY_BASE_PATH`, `NATS_IDENTITY_VALIDATE_SUBJECT` | `trx.identityservice`, `validateInternalToken` | identity subject |
| reused: `NATS_URL`, `NATS_JWT`, `NATS_KEY` | required | agent connections (same user as the engine) |

Operational prerequisite: the service's NATS user must be allowed to publish and
subscribe on `trx.agent.>`, `_INBOX.>`, and `trx.identityservice.>`.

### 3.9 Logging (one line per hop, greppable)

- `RSASSISTANT event=agent.published workflow=<id> name=<name> subject=<chatSubject> mode=<streaming|override>`
- `RSASSISTANT event=workflow.skipped workflow=<id> reason=<...>`
- `RSASSISTANT event=consult.start name=<name> run=<runId> user=<id> account=<id> verified=<bool>`
- `RSASSISTANT event=consult.refused name=<name> run=<runId> reason=<...>`
- `RSASSISTANT event=consult.done name=<name> run=<runId> stop=<reason> events=<n> ms=<n>`
- `RSASSISTANT event=consult.error name=<name> run=<runId> err=<...>`
- nats-agent's own `IDT_METRIC event=validate.*` lines appear alongside.

## 4. Backward compatibility

- Flag off: no new NATS connections, subscriptions, or log lines beyond none.
- Loader: new fields are `omitempty` raw JSON; existing files decode identically.
- `natschat`: four new methods, no signature changes.
- `service.go`: additions only after `service started`; shutdown order adds one call.
- Existing chat subjects, headers, stream protocol, and scope enforcement untouched.

## 5. Out of scope

Per-workflow IDT configuration of `trigger/nats-chat`; publishing `single`-only
workflows; serving client-only UI tools to machine callers; changes to RSAssistant.

## 6. Test strategy

Unit: manifest parsing/defaults/name validation; eligibility with fake nodes;
event mapping with a recording emitter; identity gate table; loader/engine carry
the manifest through `extends`. Integration: embedded nats-server, fake engine
responder speaking the natsstream protocol, fake identity responder, real
nats-agent agent, `agentclient` consult; asserts verified identity headers reach
the responder and events arrive in RSAssistant's shape. Manual: Dev smoke via
RSAssistant after opensearchAiChatApi adoption.
