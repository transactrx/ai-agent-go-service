# Alias-only model calls with gateway `paramPolicy` — design

Date: 2026-09-29
Status: approved in conversation (brainstorm 2026-09-29), pending written review
Repos: ai-agent-go-service (lib, primary), opensearchAiChatApi (app, consumer),
inferenceGateway (alias row = data only, no code change here)
Follows: `2026-09-25-inference-gateway-llm-design.md` (side-by-side comparison,
done and benched). This spec is the follow-on, not a replacement.

## 1. Problem

The 2026-09-25 design put the app on the gateway but left model knowledge on
the client side:

- `ai/inference-gateway` accepts `modelId` and `lab`+`family` as alternatives
  to `alias` (`inferencegateway/factory.go:30-44`), so a workflow can still
  name a model here.
- It always sends `maxTokens` (agent loop hardcodes 4096 at
  `agent/loop.go:203`; node defaults 4096 at `factory.go:23,100`), and
  forwards `temperature` / `stopSequences` when the request carries them
  (`payload.go:102-113`).
- `Config.Temperature` is parsed and never read (dead, both providers).
- `ai/bedrock` proved what happens when model rules live in the app: Claude
  5.5 rejects `thinking:disabled` and forced `tool_choice`, so auto-update
  cannot promote to 5.5+ and had to be switched off (`CHANGELOG v1.8.0`).

The gateway branch `feat/alias-param-policy` (inferenceGateway, 9 commits over
Development, HEAD e31a851) now lets an alias carry a `paramPolicy`
(`drop → set → default → exclusive → clamp`) that compiles the caller's body
before inference, only on the alias branch of `invoke` / `invokeStream`
(`pkg/events/handler.go:215-218`, `pkg/events/policy.go`). The
`invokeStream` ack returns `appliedParams` and `paramWarnings` when a policy
ran (`pkg/models/models.go` `InvokeStreamAck`). Its spec names the consumer
rule: *when calling by alias, send only `alias`, `system`, `messages`,
`tools`; leave knobs to the alias policy.*

## 2. Goal

The app and the lib never name a model and never send a model parameter. They
send an alias; the gateway resolves the model (`resolveAliasTarget`,
`handler.go:296-355`) and owns every parameter through `paramPolicy`. A
future model with different rules is an alias edit, never a lib or app
deploy. Everything that already keeps the project flexible stays: provider
swap by port, derived workflows, env substitution.

## 3. What stays as is (the flexibility that already works)

| mechanism | where | kept |
|---|---|---|
| Agent binds to any `LLMProvider` on port `ai_languageModel` by role | `agent/factory.go:111-122` | unchanged |
| `extends` + null-delete + `$remove` derived workflows | `engine/loader/derive.go` | unchanged |
| `${VAR:default}` substitution on every string at load | `engine/loader/envsubst.go` | unchanged, now carries the alias |
| `ai/bedrock` direct provider + auto-update | `builtin/bedrock` | untouched; deletion is a separate task |
| `admin/prompt-rewrite` direct Bedrock call | `builtin/promptrewrite` | untouched; migration to a gateway alias is a separate task |
| `ToolChoiceName` forwarded as `toolChoice: "tool"` | `inferencegateway/payload.go:155-158` | kept: per-call intent, not a model knob; sole setter today is the bedrock probe |

## 4. Library changes (ai-agent-go-service → v1.8.0, unreleased, amend entry)

### 4.1 `ai/inference-gateway` config becomes alias-only

`pkg/workflow/builtin/inferencegateway/factory.go`:

```go
type Config struct {
    Alias                string `json:"alias"`
    StreamTimeoutSeconds int    `json:"streamTimeoutSeconds,omitempty"`
    IdleTimeoutSeconds   int    `json:"idleTimeoutSeconds,omitempty"`
    BasePath             string `json:"basePath,omitempty"`
}
```

- Removed: `ModelID`, `Lab`, `Family`, `MaxTokens`, `Temperature`,
  `defaultMaxTokens`.
- `validate()`: `alias` required and must match the gateway's name rule
  `^[A-Z][A-Z0-9_]{1,63}$` (README "Names are …"), so a model id pasted into
  `alias` fails at load, not at the gateway. Timeouts non-negative as today.
- Factory decodes with `DisallowUnknownFields`: a workflow that still carries
  `maxTokens`, `temperature`, `modelId`, `lab`, `family` or `model` fails to
  load with `ai/inference-gateway: parse config: json: unknown field "maxTokens"`.
  Fail-loud is the project rule for bad config (2026-09-25 spec §3.1). The
  `${VAR}` substitution runs before the factory, so an unresolved alias
  variable already fails at load.
- Init log line drops `modelId/lab/family`:
  `ai/inference-gateway wf=%s node=%s ready (alias=%q subject=%s)`.

### 4.2 Request body carries no knobs

`payload.go` `buildRequest(req node.LLMRequest, cfg Config)` emits exactly:
`alias`, `system`, `messages`, `tools`, `toolChoice` + `toolChoiceName` (when
`req.ToolChoiceName != ""`), and `streamSubject` (set by `Stream`).

- `invokeStreamRequest` wire struct loses `Lab`, `Family`, `ModelID`,
  `MaxTokens`, `Temperature`, `StopSequences`.
- `req.MaxTokens`, `req.Temperature`, `req.Stop` are ignored by this
  provider. Documented in the `LLMRequest` field comments (`node/llm.go`):
  "honoured by `ai/bedrock`; `ai/inference-gateway` leaves these to the
  gateway alias `paramPolicy`".
- Message / tool translation (§3.2 of the 2026-09-25 spec) unchanged.

### 4.3 Agent loop stops hardcoding `maxTokens`

`agent/loop.go:199-204`: remove `MaxTokens: 4096`. `ai/bedrock` already
defaults `cfg.MaxTokens` to 4096 when the request carries 0
(`bedrock/factory.go:72-73`, `payload.go:80-83`), so the direct wire payload
is byte-identical. Proof: existing bedrock payload tests plus one new test
asserting a zero-`MaxTokens` request yields `max_tokens: 4096`.

### 4.4 Ack policy fields surfaced

`client.go` `streamAck` gains:

```go
AppliedParams map[string]any `json:"appliedParams,omitempty"`
ParamWarnings []string       `json:"paramWarnings,omitempty"`
```

`stream.go`, right after `timing.Model = ack.InvokeID`: when either is
non-empty, one log line
`gateway-policy wf=<id> node=<id> alias=<name> model=<invokeId> applied=<compact json> warnings=<"; "-joined>`.
Nothing is logged when no policy ran (older gateway, alias without policy):
byte-identical behaviour to today.

### 4.5 Docs

- `docs/EXTENDING.md:115-120` LLM (gateway) entry: alias only; config
  `alias`, `streamTimeoutSeconds`, `idleTimeoutSeconds`, `basePath`; "model
  and every inference parameter are owned by the gateway alias
  (`paramPolicy`)"; `gateway-policy` log line.
- `docs/DEPLOYMENT.md`: `gateway-policy` line next to `llm-timing`
  (`:54-59`).
- `CHANGELOG.md` v1.8.0 first bullet rewritten: alias-only, no knobs,
  agent no longer sends `maxTokens`, `gateway-policy` line. Mark the agent
  change explicitly: "ai/bedrock payload unchanged (node default 4096)".
- `README.md:56` unchanged (type list only).

## 5. App changes (opensearchAiChatApi)

- `workflows/powerlineSearchGateway.json` `bedrock1.config`:
  ```json
  { "alias": "${POWERLINE_MODEL_ALIAS:POWERLINE_CLAIM_SEARCH_MODEL}",
    "basePath": "${INFERENCE_GATEWAY_BASE_PATH:trx.inferenceGateway}",
    "model": null, "region": null, "autoUpdate": null, "maxTokens": null }
  ```
  `maxTokens: null` is required: the base `powerlineSearch` node carries
  `maxTokens: 4096` (`powerlineSearch.json:50`) and deep-merge would inherit
  it, which §4.1 now rejects at load.
- Env var renamed from `INFERENCE_GATEWAY_ALIAS` to per-workflow
  `POWERLINE_MODEL_ALIAS` so each workflow can be pointed at its own tier
  (flexibility rule: one alias per workflow, chosen per environment).
- `pkg/workflow/rsassistant_manifest_test.go:181` updated: alias
  `${POWERLINE_MODEL_ALIAS:POWERLINE_CLAIM_SEARCH_MODEL}`, no `maxTokens` key, `basePath`
  unchanged.
- `docs/configuration.md:25` row → `POWERLINE_MODEL_ALIAS`, default
  `POWERLINE_CLAIM_SEARCH_MODEL`, "model and parameters come from the gateway alias row".
  `docs/api-reference.md:14` row → alias `POWERLINE_CLAIM_SEARCH_MODEL`.
- `.envTemplate`: add commented `POWERLINE_MODEL_ALIAS`.
- `go.mod`: bump to v1.8.0 once minted (keeps the local `replace` until then;
  `go mod vendor` after any lib change, app vendors deps).

## 6. Gateway alias row (data, not code)

Written with the nats CLI against `<base>.setAlias`, after
`feat/alias-param-policy` is deployed to that environment:

```json
{ "alias": "POWERLINE_CLAIM_SEARCH_MODEL",
  "modelId": "us.anthropic.claude-opus-5-5",
  "paramPolicy": { "set": { "maxTokens": 4096 } } }
```

- Target, parity choice (user 2026-09-29): pin `"modelId": "us.anthropic.claude-opus-5-5"`,
  the exact model the direct path uses, so both approaches answer with the
  same model. `catalog.InvokeIDFor` accepts the invoke id as well as the
  catalog id. Switching to `lab`+`family` (follow newest release) is an ops
  choice later, never an app change. The user writes the row via the gateway
  webapp; the nats CLI recipe in the plan is the fallback.
- Policy: `set`, not `default`, so the gateway owns the value even if a
  caller sends one. `maxTokens: 4096` is the only parameter the code sends
  today (§1). No `temperature` / `topP` / `stopSequences` (never sent), no
  thinking knob (no gateway field on the Converse path).
- Environments: the local gateway started with `MODEL_ALIAS_TABLE=transactrx_model_alias`
  and `AWS_PROFILE=Development` writes the **Dev** global table, so the local
  validation step is the Dev provisioning. Production is a separate account
  and a separate, approval-gated `setAlias`.

### 6.1 Parity table: what the direct path sends today → alias policy

Requirement (user, 2026-09-29): the alias `paramPolicy` must reproduce every
parameter the direct `ai/bedrock` path hardcodes, so both approaches produce
the same answers. Source: `bedrock/payload.go:65-160` envelope for the pinned
`us.anthropic.claude-opus-5-5`, and the gateway's `pkg/inference/convert.go:34-41`.

| direct `ai/bedrock` field (InvokeModel envelope) | value today | gateway equivalent (Converse) | policy entry |
|---|---|---|---|
| `max_tokens` | 4096 (node default; agent no longer sends one) | `inferenceConfig.maxTokens` | `"set": {"maxTokens": 4096}` |
| `temperature` | absent (agent never sets; `Config.Temperature` dead) | `inferenceConfig.temperature` | none (absent on both) |
| `stop_sequences` | absent | `inferenceConfig.stopSequences` | none |
| `top_p` / `top_k` | never sent | `inferenceConfig.topP` | none |
| `thinking` | absent for 5.5+ (`thinking:disabled` only for 5.0–5.4) | no field on the Converse path → adaptive default | none; identical on 5.5+ |
| `tool_choice` | only when `ToolChoiceName` set (never in production) | `toolConfig.toolChoice` | none; forwarded per call by the node |
| `tools` / `system` / `messages` | same content, tool results stringified the same way | same | none |
| `anthropic_version` | `bedrock-2023-05-31` | not applicable (Converse envelope) | none |

Result: `{"set": {"maxTokens": 4096}}` is the complete parity set. Optional
guard for the future (not applied by default, because `drop` on an absent
field logs a `not present` warning on every call):
`"drop": ["temperature", "topP", "stopSequences"]` would pin the parity
even if a future caller started sending knobs.

Known non-parity, owned by the gateway: if the alias is re-pointed to a
Claude 5.0–5.4 release, the direct path would add `thinking:disabled` and the
gateway cannot (no thinking field on Converse). Only matters for forced
`tool_choice`, which production never uses.

## 7. Error handling

| condition | behaviour |
|---|---|
| `alias` missing / not matching name rule | factory error, workflow fails to load |
| stale knob key in node config | factory error `unknown field`, workflow fails to load |
| alias unknown at the gateway | ack 4004 → `Stream` error, nothing emitted, retryable by policy |
| policy produces an unbuildable request | ack 4002 with `paramPolicy <ALIAS>: …` → same path |
| gateway without policy support | ack has no policy fields → no `gateway-policy` line, call proceeds; Bedrock applies its own `maxTokens` default (ship order in §9 avoids this) |

## 8. Testing

Lib (`go test -mod=vendor ./...`):
- `factory_test.go`: alias required; name rule; unknown field rejected
  (`maxTokens`, `modelId`, `lab`); defaults for timeouts only.
- `payload_test.go`: body has no `maxTokens`/`temperature`/`stopSequences`
  keys even when `LLMRequest` carries them (marshal and assert on JSON keys);
  `toolChoice` still forwarded; block translation table unchanged.
- `stream_test.go`: fake gateway acks with `appliedParams`/`paramWarnings`
  → `gateway-policy` line logged; ack without them → no line; `seen` body
  has no knob keys.
- `agent` tests: request built by the loop has `MaxTokens == 0`.
- `bedrock/payload_test.go`: zero `MaxTokens` → `max_tokens: 4096`.

App (`go test ./...`): manifest test as §5.

Live, local ecosystem (`feedback-local-ecosystem-testing`):
1. Build and run the local gateway from `feat/alias-param-policy`
   (recipe in memory `project-inference-gateway-llm`).
2. `nats req trx.inferenceGateway.resolveAlias '{"alias":"MAX_MODEL"}'` →
   copy `lab`/`family`; `setAlias POWERLINE_CLAIM_SEARCH_MODEL` with the §6 body;
   `resolveAlias POWERLINE_CLAIM_SEARCH_MODEL` echoes the policy.
3. Restart chat API on the new lib (user restarts components), one question
   to `<base>.powerlineSearchGateway`; app log shows
   `gateway-policy … applied={"alias":"POWERLINE_CLAIM_SEARCH_MODEL","maxTokens":4096,…} warnings=maxTokens set`
   and `llm-timing … provider=ai/inference-gateway model=<invokeId>`.
4. Negative: workflow JSON with a leftover `maxTokens` fails to load with the
   `unknown field` message.

## 9. Ship order (each push approval-gated, `feedback-no-auto-commits`)

1. Gateway `feat/alias-param-policy` → Development → deployed (owner: user).
2. Dev alias row `POWERLINE_CLAIM_SEARCH_MODEL` written (§6).
3. Lib branch `feature/inference-gateway-llm` amended with §4, commit per
   task → PR to Development → `release:minor` on Dev→Production PR mints
   v1.8.0.
4. App branch: `go get …@v1.8.0`, drop `replace`, `go mod tidy && go mod
   vendor`, §5 changes → PR to Development.
5. Production: gateway promoted, Prod alias row written, app promoted, in
   that order.

## 10. Out of scope (recorded so intent is not lost)

- Delete `ai/bedrock` + auto-update once every workflow is on the gateway.
- Move `admin/prompt-rewrite` to a gateway alias (e.g. `PROMPT_REWRITE_ALIAS`).
- Per-family policy layer in the gateway (gateway spec non-goal).
- Per-call `tool_choice` control on the agent (cycle-4 candidate).
