# NATS Endpoint Documentation Improvement — Design

Date: 2026-07-15
Repo: ai-agent-go-service (only)
Status: awaiting approval

## Problem

Endpoint docs render poorly in the discovery viewers. Request-body contracts
and examples are crammed into the one-line `Description` string (unreadable
blob), header docs lack examples, and `Response` docs are missing entirely.

The nats-service doc schema already has everything needed — `HeaderDoc`
(name/description/required/example) and `ResponseDoc`
(description/contentType/example) — the declaration sites just don't fill
them in. Additionally, nats-discover-web auto-parses a trailing
`Request body: {field1, field2}` sentence out of the description into a
structured table, and pretty-prints `Response.Example` in a `<pre>` block.

## Decision (approved in brainstorming)

Approach B: improve documentation **content only**, at the declaration sites
in ai-agent-go-service. No logic changes anywhere.

Explicitly out of scope:
- nats-service library (shared; untouched)
- Viewer UIs (natstoolwebapp, nats-discover-web; untouched)
- Workflow JSON files / trigger config schema (no new `docs` block)
- Any handler/runtime behavior; only `EndpointRegistration` literal values
  (Description, Headers, Response) change

## Authoring conventions

1. `Description`: 1–2 plain sentences of purpose, ending with a
   `Request body: {field1, field2}` sentence (viewer-parsable). Field list
   matches the real request struct.
2. `Headers`: every header the handler reads, each with Description,
   Required, and a realistic Example.
3. `Response`: Description (incl. streaming event sequence where relevant),
   ContentType `application/json`, Example = compact real-shaped JSON string
   (viewer pretty-prints it).

## Changes per file

### 1. `pkg/workflow/builtin/natschat/trigger.go` — chat endpoints

Registration built per workflow (id known at registration time).

- Description:
  `AI chat endpoint for workflow '<id>'. Streams the agent's answer as NATS events on the reply inbox. Request body: {message, sessionId} — message required; sessionId optional, server generates one and returns it in the start event.`
- Headers (from `IdentitySource`, required flags from config as today):

| Name | Required | Description | Example |
|---|---|---|---|
| X-Account-Id | per config | Account id — security scope for all data the agent can see | 5480 |
| X-User-Id | per config | Human user id — chat session memory + audit | yulexis |
| X-User-Name | no | Display name shown to the agent | Yul Celeiro |
| X-Time-Zone | no | IANA timezone used for date questions | America/New_York |
| _User_Id | no | Service identity; set automatically by the nats-service client | powerlineWebApp |

- Response: Description explains
  `start → (delta | tool_call | tool_result)* → complete | error`, event type
  in `_Stream_Event` header, cancel subject in `_Stream_Cancel_Subject` on
  start. Example (one JSON object showing each event payload):

```json
{
  "start":       {"sessionId": "e1f0c9a2-4b7d-4f7e-9c1a-8f2d3e4a5b6c"},
  "delta":       {"text": "Today you have 1,204 paid claims..."},
  "tool_call":   {"toolUseId": "toolu_01", "name": "opensearch_query", "input": {"query": "..."}},
  "tool_result": {"toolUseId": "toolu_01", "output": {"hits": "..."}, "isError": false},
  "complete":    {"finalText": "Today you have 1,204 paid claims...", "messageStop": "end_turn"},
  "error":       {"code": "cancelled", "message": "stream cancelled by client"}
}
```

(Payload keys verified against agent loop.go — the wire format; NOTE the
opensearchAiChatApi repo's docs/api-reference.md documents stale shapes for
tool_call/tool_result/complete. Event sequence also includes thought and
attachment event types.)

Note: the existing `headerDocs` slice stays config-driven; only descriptions,
examples, and the added optional headers (X-User-Name, X-Time-Zone) change.
Adding entries to `headerDocs` is documentation data, not logic.

### 2. `pkg/promptadmin/admin.go` — PromptGet / PromptSave / PromptHistory

**PromptGet**
- Description: `Get the prompt for one overridable node field: fixed part, workflow-JSON default, and current override with its source. Request body: {workflowId, nodeId, field}`
- Response example:
```json
{"fixed": "You are a pharmacy claims assistant...", "defaultFlex": "Answer using the search tool...", "currentFlex": "Answer using the search tool... (edited)", "source": "db"}
```
  (`source` is `db` or `default`.)

**PromptSave**
- Description: `Save a new version of a flexible prompt. Content is template-validated before saving; the change is broadcast and hot-applied on all instances. Request body: {workflowId, nodeId, field, content}`
- Headers: `X-User-Id` — required — `Author recorded in version history` — example `yulexis`
- Response example:
```json
{"version": "v#0001717500000000"}
```

**PromptHistory**
- Description: `List saved versions of a prompt field, newest first. Request body: {workflowId, nodeId, field, limit} — limit optional, default 20.`
- Response example (real marshaled shape of `promptstore.Version`, no json
  tags → capitalized keys; documented as-is, struct NOT modified):
```json
[{"Version": "v#0001717500000000", "Content": "Answer using the search tool...", "SavedBy": "yulexis", "CreatedAt": "2026-07-14T10:00:00Z"}]
```

### 3. `pkg/workflowlist/workflowlist.go` — ListWorkflows

- Description: `List currently-loaded chat workflows. Use each id as the chat subject: <basePath>.<id>. Request body: {} (empty JSON object)`
- Response example:
```json
[{"id": "eprescribeSearch", "description": "AI chat over ePrescribe transactions"}, {"id": "powerlineSearch", "description": "AI chat over PowerLine pharmacy claims"}]
```

## Error handling

None — no runtime behavior changes. Doc strings cannot fail.

## Testing / verification

1. `go build ./... && go test ./...` in ai-agent-go-service (existing tests;
   any test asserting on registration descriptions updated to match).
2. Run opensearchAiChatApi locally against the modified lib (go.mod replace),
   request `<basePath>._api_docs`, confirm JSON carries the new
   Headers/Response docs.
3. Open the discovery viewer, eyeball: description short + body table parsed,
   headers with examples, response pretty-printed.

## Rollout

1. Changes on a feature branch in ai-agent-go-service; no commit/push until
   explicit approval.
2. After approval: tag new lib version, bump go.mod in opensearchAiChatApi
   (and webApp when convenient — same lib serves both).

## Unresolved questions

None — all decided in brainstorming.
