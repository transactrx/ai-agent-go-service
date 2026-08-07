# Chat endpoint contract — calling a workflow over NATS

How to call a `trigger/nats-chat` workflow endpoint and consume its response, in both
response modes. This is the caller-facing contract; for wiring a service that HOSTS
workflows, see [EXTENDING.md](EXTENDING.md).

## One subject, two response modes

Every workflow registers ONE NATS subject: `<NATS_BASE_PATH>.<workflowId>` (or the
trigger's `subject` config when set). The same subject serves both response modes —
which one you get is decided per request:

1. The workflow's trigger config `responseMode` sets that endpoint's default
   delivery. It is a REQUIRED field with exactly two legal values — `"streaming"`
   or `"single"` — and there is no implicit fallback: a workflow file without it
   (or with any other value) is rejected at load time. Whatever the file says is
   what every request gets unless overridden per rule 2. (Deployed examples: the
   PowerLine UI workflows set `"streaming"`; the machine-caller `Single*` workflows
   set `"single"`.)
2. If — and only if — the workflow's trigger config sets
   `allowResponseModeOverride: true` (added in v1.4.0; this flag DOES have a
   default: false), the request body's `responseMode` field overrides the
   configured default for that one request. On workflows without the flag the body
   field is silently ignored, so existing deployments cannot change behavior by
   accident.

Mode selects DELIVERY only. The agent run underneath (LLM, tools, policy scoping,
session memory) is identical in both modes.

## The request (both modes)

One NATS message, published with a reply inbox:

Headers:

| Header | Required | Meaning |
|---|---|---|
| `X-Account-Id` | yes (400 if missing) | Security scope for everything the agent can query |
| `X-User-Id` | yes (400 if missing) | Keys chat session memory and audit logging |
| `X-User-Name` | no | Display name, used for personalized answers |
| `X-Time-Zone` | no | IANA zone; resolves date words like "today" |
| IDT credential | per deployment | Validated when `IDT_VALIDATION=true` (deny = 403 when enforcing) |
| `_Message_Id` | no | Becomes the request id; auto-generated otherwise |

Body:

```json
{"message": "How are the transactions doing today?",
 "sessionId": "<optional>",
 "responseMode": "<optional: single|streaming>",
 "attachments": [{"mediaType": "image/png", "filename": "x.png", "data": "<base64>"}]}
```

- `message` — required (400 if empty).
- `sessionId` — omit on the first message; the server mints a UUID and returns it
  (in the `start` event when streaming, in the reply body when single). Send it back
  on the next call to continue the conversation with memory. Memory is partitioned by
  `{workflowId, accountId, userId, sessionId}` — sessions never leak across workflows,
  accounts, or users.
- `responseMode` — see above. Invalid value on an opted-in workflow → 400.

## Single mode — plain request/reply

One request in, ONE reply back. In Go, plain `nc.RequestMsg(msg, timeout)` works.

**Set your client timeout at or above the workflow's `requestTimeoutSeconds`** —
agent runs can take minutes. If your client gives up early, the server still finishes
(and bills the tokens) but the answer is lost.

Success reply (`status` 200):

```json
{"sessionId": "e1f0c9a2-...",
 "finalText": "Today you have 1,204 paid claims...",
 "stopReason": "end_turn",
 "attachments": []}
```

- `finalText` is the complete answer. Chart URLs arrive as markdown image links
  inside it.
- `attachments` passes through raw tool attachment events (`{toolUseId, kind,
  payload}`). No built-in tool emits them today, so expect `[]`.

Error reply: nats-service error JSON; the wire `status` header is always 400
(validation) or 500 (server) — the finer-grained code (`llm-error`, `timeout`,
`cancelled`, `executor-failed`, `policy-deny`, IDT 403, timeout 504) travels inside
the error body.

Limits of single mode: no progress visibility, no cancel, and client-side interactive
`ui-*` tools cannot round-trip (a workflow that carries them degrades gracefully —
the tool call fails fast and the model answers without it — but machine-facing
workflows should simply not include `ui-*` nodes).

## Streaming mode — event stream on your reply inbox

You cannot use plain `Request()`: MANY messages arrive on the reply inbox. Subscribe
to your own inbox, publish the request with that inbox as the reply subject, then
consume events until a terminator. Go callers should use the shipped client, which
does all of this:

```go
sub, err := natsstream.DoStreamingRequest(nc, subject, headers, body, timeout, logger)
for evt := range sub.Events() { ... }   // closes after complete|error
```

Each event message carries headers:

| Header | Meaning |
|---|---|
| `_Stream_Event` | `start` → (`delta` \| `thought` \| `tool_call` \| `tool_result` \| `attachment`)* → `complete` \| `error` |
| `_Stream_Sequence` | 0 = start, N-1 = terminator; consumers must track order |
| `_Stream_Id` | Correlates all events of one stream |
| `_Stream_Cancel_Subject` | On `start` only — publish anything there to cancel the run |
| `_Stream_Tool_Result_Prefix` | On `start` — publish client-tool answers to `<prefix>.<toolCallId>` |

Key payloads: `start` = `{sessionId, workflowId, requestId}`; `complete` =
`{finalText, messageStop}`; `error` = `{code, message}`. Unsubscribe after the
terminator and enforce your own overall timeout.

Full wire protocol: `pkg/transport/natsstream/doc.go`.

## Choosing a mode

| | single | streaming |
|---|---|---|
| Call style | request/reply, 1 response | publish + own-inbox subscription, N events |
| Progress / thoughts / tool calls | invisible | visible live |
| Cancel mid-run | no | yes |
| Interactive `ui-*` tools | not usable | supported |
| Typical caller | another service or agent | a chat UI |

## Backward compatibility (v1.4.0)

The single entry point (`handle`) and the two-mode dispatch have existed since the
first release; v1.4.0 finished the previously non-functional single path
(`handleSingle` used to leave the executor's stream sink nil) and added the opt-in
override flag. With no `responseMode` in the body — or on any workflow that doesn't
set `allowResponseModeOverride` — behavior is bit-identical to v1.3.x. Existing
streaming callers need no changes.

## Derived workflows (`extends`)

A workflow file may declare `"extends": "<baseId>"` to overlay onto an existing
workflow instead of duplicating it whole (used by the machine-caller `Single*`
workflows, which overlay the UI `powerlineSearch`/`eprescribeSearch` files).

- Two-pass load: pass 1 reads every workflow file and indexes it by its JSON
  `"id"` field — not its file/source id, so `extends` resolves against the
  workflow's declared id regardless of what file it lives in. Pass 2 resolves
  `extends` (if any) and registers; a bad merge fails at startup, never at
  request time.
- Merge rules: objects deep-merge (derived wins); `null` deletes a key;
  `nodes[]`/`connections[]` merge by node id; `"$remove": true` deletes a node
  and all connections touching it.
- **Prompt carve-out (load-bearing):** `ai/agent` prompt fields
  (`systemMessageFixed`, `systemMessageFlexible`) are NEVER inherited — each
  derived file must define its own, or load fails loudly:
  `derived workflow %q: node %q (ai/agent): systemMessageFixed and
  systemMessageFlexible must be defined in the derived file — prompts are never
  inherited`
- No chained extends — a derived file's base must be a non-derived workflow:
  `extends %q: base is itself derived (chained extends is not supported)`. A
  missing base fails with `extends %q: base workflow not found`.
- **Duplicate workflow ids fail loudly, isolated:** if two files declare the
  same JSON `"id"`, the first (by sorted source id) wins and registers
  normally; every later definition fails to register — `workflow %s: register
  failed: duplicate workflow id %q (first definition wins)` — without
  affecting any other workflow's load.
- **Invalid `extends` fails loudly, isolated:** a non-string or empty
  `extends` value fails only that workflow — `workflow %s: register failed:
  invalid extends: ...`. If the failure is discovered while resolving an
  overlay's base (the base's own `extends` is invalid), the log names the
  base too: `workflow %s: register failed: base %q: invalid extends: ...`.
  Either way, siblings still load.
- Debug: `WORKFLOW_DERIVE_DEBUG=true` logs the merged config (pre-envsubst) per
  derived workflow at load time: `workflow <id>: derived from <base>, merged
  config: <json>`.

## Internals, briefly

The trigger hands each validated request to the engine through `node.TriggerSink`
(`sink.Emit(ctx, TriggerEvent)`) — the one interface between transport and executor,
wired at startup via `Trigger.Subscribe`. The agent loop always emits stream events;
the two modes differ only in the `node.StreamSink` placed in the event: streaming
uses a NATS-backed session bound to your reply inbox, single uses an in-memory
collector (`collectsink.go`) that buffers the terminator into the one reply. That is
why the executor and agent loop are mode-agnostic.
