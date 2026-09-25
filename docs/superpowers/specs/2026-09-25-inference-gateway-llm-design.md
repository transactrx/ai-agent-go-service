# ai/inference-gateway LLM provider — design

Date: 2026-09-25
Status: approved design (brainstorm 2026-09-25)
Repos: ai-agent-go-service (primary), opensearchAiChatApi (adoption + bench)

## 1. Goal

Add a second, parallel way for agents to call a model: through the org
`inferenceGateway` NATS service, selecting the model by **alias** (tier) instead
of a pinned Bedrock model id. The existing direct path (`ai/bedrock`) stays
untouched so both can run side by side and be timed on the same prompts.

After the comparison the direct path is deleted (separate task). Every agent
in the org then calls models the same way, and re-pointing a tier is an
operational change with no deploy.

Out of scope (separate analysis, recorded here only so the intent is not
lost): a configurable parameter-payload layer so each workflow can declare
which model parameters it sends, keeping the app's requests accurate when a
model version changes its accepted payload.

## 2. Gateway contract consumed

Base path from `INFERENCE_GATEWAY_BASE_PATH` (org value `trx.inferenceGateway`;
default placeholder `example.inferenceGateway`, same rule as the existing
`resolveModel` client in `pkg/workflow/builtin/bedrock/gateway.go`).

- `<base>.invokeStream` request body (`models.InvokeStreamRequest` in the
  gateway repo): `alias` | `modelId` | `lab`+`family`, `system`, `messages[]`
  (`role`, `content[]` of `text` | `image{format,base64}` |
  `toolUse{toolUseId,name,input}` | `toolResult{toolUseId,content[],status}`),
  `maxTokens`, `temperature`, `stopSequences`, `tools[]`
  (`name,description,inputSchema`), `toolChoice` + `toolChoiceName`,
  `streamSubject`.
- Ack (reply): `{accepted, modelId, invokeId, streamSubject}` with nats-service
  STATUS header. Non-2xx carries a `NatsServiceError` body (`errorMessage`).
- Events published on `streamSubject`, JSON `StreamEvent` with contiguous
  `seq` from 0: `messageStart`, `toolUseStart{contentIndex,toolUseId,toolName}`,
  `delta{contentIndex,text | toolInputDelta}`, `contentBlockStop{contentIndex}`,
  `messageStop{stopReason}`, `metadata{usage}`, then exactly one `done` or
  `error{error}`.
- Selection precedence in the gateway: alias > modelId > lab+family.
- Live Dev tiers: `MAX_MODEL` → anthropic / claude-opus (today
  `anthropic.claude-opus-5-5`, invoke id `us.anthropic.claude-opus-5-5`),
  `LARGE_EXPENSIVE_MODEL` → claude-sonnet, `LARGE_CHEAP_MODEL` → claude-haiku.

Gateway limitations that shape this design (fixes belong in the gateway repo):

- No document content block. The node rejects `BlockDocument` turns.
- Converse backend sends no `thinking` field. Since Bedrock rejects
  `thinking: disabled` on Claude 5.5+, `ai/bedrock` also sends none for 5.5+
  (kept only for 5.0–5.4), so on the pinned opus 5.5 both paths run adaptive
  thinking at the default effort and the comparison is apples to apples; the
  bench still reports output tokens.

## 3. Library changes (ai-agent-go-service → v1.8.0)

### 3.1 New node `ai/inference-gateway`

Package `pkg/workflow/builtin/inferencegateway`. Implements `node.LLMProvider`
(`Spec`, `Init`, `Close`, `Stream`). Registered in
`pkg/workflow/builtin/register.go` as `ai/inference-gateway`.

Config (validated in the factory; load fails loudly on a bad config):

| key | rule |
|---|---|
| `alias` / `modelId` / `lab`+`family` | exactly one selector; `lab` and `family` only together |
| `maxTokens` | default 4096; request `MaxTokens` overrides per call as today |
| `temperature` | optional, forwarded |
| `streamTimeoutSeconds` | default 600 (gateway `STREAM_TIMEOUT_SECONDS`); bound on one full response |
| `idleTimeoutSeconds` | default 120; max silence between two stream events |
| `basePath` | optional; overrides `INFERENCE_GATEWAY_BASE_PATH` for this node |

Init: takes the shared `*nats.Conn` from `env.Host("nats")`
(`*nats_service.NatService`, same lookup as `bedrock.natsConn`). Missing or
disconnected host → Init error (workflow load fails, never silent). Logger,
node id and workflow id captured for log lines.

No auto-update, no `AI_BEDROCK_MODEL_ID` pin: model choice is the gateway's
job. `Reconfigurable` is not implemented.

### 3.2 Request translation (`payload.go`)

`node.LLMRequest` → gateway `InvokeRequest`:

- `System`, `MaxTokens` (request > config default), `Temperature`, `Stop` →
  `stopSequences`.
- `BlockText` → `text`; `BlockImage` → `image{format,base64}` where format is
  derived from `MediaType` (`image/png` → `png`, jpeg, gif, webp; other →
  error); `BlockToolUse` → `toolUse` (empty input → `{}`);
  `BlockToolResult` → `toolResult` with one `{text: <raw JSON as string>}`
  content (same stringify rule as `ai/bedrock`, empty → `""`) and
  `status: "error"` when `IsError`; `BlockDocument` → error
  `ai/inference-gateway: document blocks are not supported by the gateway`.
- `Tools` → `tools[]` (`inputSchema` raw); `ToolChoiceName` non-empty →
  `toolChoice: "tool"`, `toolChoiceName`.
- Selector fields copied from config. `streamSubject` set by `Stream`.

Any translation error returns before any NATS traffic.

### 3.3 Stream (`stream.go`)

1. Build the request (3.2). `subject := nc.NewInbox()`; `sub, _ :=
   nc.SubscribeSync(subject)`; `defer sub.Unsubscribe()`.
2. `nc.RequestWithContext(ctx, <base>.invokeStream, body)` with a 10 s request
   timeout (same as `gatewayRequestTimeout`). Ack parsed with the STATUS rule
   of `parseResolveReply` (empty or 2xx numeric = success). Failure → return
   error (nothing emitted, so the agent's retry policy may retry).
3. Consume `sub.NextMsgWithContext` under two deadlines: the stream timeout
   from step 1 and an idle timer reset on every event. Per event:
   - `seq` must equal the expected counter; a gap → error
     `stream seq gap: want N got M`.
   - `delta.text` → `LLMTextDelta`; `toolUseStart` → new `LLMToolUse` keyed by
     `contentIndex`, emit `LLMToolUseStart`; `delta.toolInputDelta` → append,
     emit `LLMToolUseDelta`; `contentBlockStop` → emit `LLMToolUseStop` when an
     accumulator exists for that index; `messageStop` → `LLMMessageStop` with
     the Converse `stopReason` verbatim (`end_turn`, `tool_use`, `max_tokens`
     are the same strings the agent loop already handles); `metadata` →
     usage kept for the timing line; `messageStart` → ttfb mark;
     `done` → return nil; `error` → emit `LLMError`, return error.
4. `out` is closed on return (same contract as `ai/bedrock`).

Mid-stream errors are surfaced as they happen; the agent loop already refuses
to retry once an event was emitted.

### 3.4 Timing line (both providers)

Shared helper in `pkg/workflow/node` (`LLMTiming` struct + `LogLLMTiming`)
emitting, at the end of every call, success or failure:

```
llm-timing wf=<id> node=<id> provider=<ai/bedrock|ai/inference-gateway> model=<resolved id> ttfb_ms=<n> total_ms=<n> in_tok=<n> out_tok=<n> stop=<reason> err=<msg|->
```

- `ai/bedrock`: model = the id actually invoked (after any last-known-good
  retry); ttfb at first chunk; usage parsed from `message_start`
  (`message.usage.input_tokens`) and `message_delta` (`usage.output_tokens`),
  both currently ignored by `handleAnthropicChunk`.
- `ai/inference-gateway`: model = `invokeId` from the ack; ttfb at the first
  stream event after the ack (request send is t0 for both providers); usage
  from `metadata`.

Unknown values print as `-`.

### 3.5 Docs

README node list, `docs/EXTENDING.md` LLM entry, `docs/DEPLOYMENT.md` env
table (`INFERENCE_GATEWAY_BASE_PATH` now also feeds `ai/inference-gateway`),
CHANGELOG entry for v1.8.0.

## 4. App changes (opensearchAiChatApi)

- `go.mod`: ai-agent-go-service v1.8.0 (after the version is minted).
- `workflows/powerlineSearch.json` and `workflows/eprescribeSearch.json`:
  `ai/bedrock` node `model` → `us.anthropic.claude-opus-5-5`,
  `autoUpdate: false`. The `Single*` variants inherit this.
- New `workflows/powerlineSearchGateway.json`, `extends: "powerlineSearch"`:
  - node `bedrock1` keeps its id, `type` becomes
    `ai/inference-gateway`, config `{ "alias": "${INFERENCE_GATEWAY_ALIAS:MAX_MODEL}",
    "maxTokens": 4096, "model": null, "region": null, "autoUpdate": null }`.
    Connections are inherited unchanged.
  - `rsassistant: null` so the variant is never published as an agent.
  - `systemMessageFixed` / `systemMessageFlexible` copied verbatim from the base
    (loader rule: prompts are never inherited). `id` and `description` updated.
  - Endpoint becomes `<NATS_BASE_PATH>.powerlineSearchGateway`, next to the
    existing `.powerlineSearch`. Not wired into the WebApp.
- `compose.local.env`: add `INFERENCE_GATEWAY_BASE_PATH=trx.inferenceGateway`
  (`.envTemplate` already has it; terraform already plumbs it).
- `pkg/workflow` manifest test extended: the gateway variant must load, carry
  no `rsassistant` block, and have an `ai/inference-gateway` LLM node.
- `docs/configuration.md`: note that `INFERENCE_GATEWAY_BASE_PATH` is now
  required for the gateway workflow.

## 5. Comparison bench (throwaway, scratchpad only)

A Go program in the session scratchpad, not committed, that:

- connects with the local ecosystem's NATS credentials and uses the lib's
  `pkg/transport/natsstream` client;
- sends the same prompt set (N=10 per path, fixed order, alternating paths) to
  `<base>.powerlineSearch` and `<base>.powerlineSearchGateway` with the
  identity headers from the NATS test contract;
- records per call: time to first streamed token, total time, answer length;
- prints median / p95 per path, and a side-by-side of the `llm-timing` lines
  collected from the app log (pure model time, tokens, stop reason).

The result is reported in chat with the thinking caveat from §2 attached.

## 6. Error handling summary

| condition | behaviour |
|---|---|
| bad config | factory error, workflow fails to load |
| no NATS host | Init error |
| document block | error before request |
| request timeout / no responder | error, nothing emitted (retryable by policy) |
| ack non-2xx | error with gateway `errorMessage` |
| `error` event | `LLMError` emitted, error returned |
| seq gap | error |
| idle or stream timeout | error |
| ctx cancelled | ctx error, unsubscribe |

## 7. Testing

Unit (lib, embedded `nats-server` as in `gateway_test.go`, fake gateway
responder that acks and publishes scripted events):

- text-only happy path with usage → events + timing line;
- tool-use round trip (start / input deltas / stop / `messageStop: tool_use`);
- `error` event; non-2xx ack; no responder; seq gap; idle timeout; stream
  timeout; ctx cancel;
- payload translation table (all block kinds, image format mapping, tool
  choice, stop sequences, document rejection);
- config validation table;
- `ai/bedrock` usage parsing and timing line (fake invoke stream).

App: manifest test as in §4. `go vet`, `go test ./...` in both repos.

Live: local ecosystem against the Dev gateway (`trx.inferenceGateway`), one
manual question to each endpoint, then the bench of §5.

## 8. Ship order

1. Lib branch `feature/inference-gateway-llm`: implement, tests, docs,
   CHANGELOG. Commit per task. Push and release **v1.8.0 only with explicit
   approval** (push triggers deploy).
2. App branch: bump to v1.8.0, workflows, env, test. Validate locally. Push
   only with approval.
3. Bench, report. Deletion of `ai/bedrock` and auto-update is a later task.
