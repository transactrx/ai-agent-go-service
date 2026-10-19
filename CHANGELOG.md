# Changelog

## Unreleased

All new behavior is off by default; consumers that bump without config changes behave as before.

- **Opt-in IDT validation cache.** `IDT_VALIDATE_CACHE_SECONDS` (default `0` = off) lets
  `pkg/idt` reuse a successful validation of the same token + function for that many seconds.
  Denies and observe-only results are never cached; a cached allow keeps working up to the TTL
  after a grant is revoked.
- **IDT cache value checks.** An invalid or negative `IDT_VALIDATE_CACHE_SECONDS` logs a
  `WARNING` naming the variable and value and keeps the cache off. Values above `3600` log a
  `WARNING` and are capped at `3600`.
- **Reasoning summary stream.** Reasoning is turned on by the gateway alias `paramPolicy`
  (`{"set":{"reasoning":"summarized"}}`), never by a workflow: `ai/inference-gateway` has no
  `reasoning` option (setting it fails the workflow at load, like any unknown key). When the
  alias enables it, the node turns the gateway's `reasoningDelta` stream events into
  `reasoning_delta` events. Reasoning never enters model history.
- **Agent `surfaceThoughts` thought lines.** With `surfaceThoughts: true` on the agent node, the
  reasoning summary (when the alias sends one) is relayed as `thought` stream lines (one per
  completed line, max 300 chars).
- **Time zones load in minimal images.** The template renderer embeds Go's time zone database
  (`time/tzdata`, ~450 KB), so `{{userTimeZone}}` / `{{nowLocal}}` honor `X-Time-Zone` on images
  without zoneinfo (e.g. `alpine:3`, the chat API's runtime image). Before, such images silently
  fell back to UTC.
- **RSAssistant user context.** Requires nats-agent v0.4.0 (protocol 1.3). Published
  workflow agents advertise `userContext`; a consult's display context (user time zone and
  name, already cleaned by nats-agent) is forwarded to the workflow as `X-Time-Zone` /
  `X-User-Name`, so `{{userName}}`, `{{userTimeZone}}` and `{{nowLocal}}` work on RSAssistant
  consults like on direct chat. Absent context behaves as before (UTC). Display only.
- **`node.LLMReasoningDelta`.** New `LLMEventKind` (`"reasoning_delta"`) carrying a
  reasoning-summary fragment; providers emit it only when their model sends one.

## v1.10.0

- **One observe switch for both token checks.** `rsassistant` now reads `IDT_OBSERVE_ONLY`, the
  same flag the `trigger/nats-chat` token check (`pkg/idt`) already reads, so a process is either
  observing or enforcing on every path at once. `RSASSISTANT_IDT_OBSERVE_ONLY` is retired and
  ignored. Operators: drop the old variable and set `IDT_OBSERVE_ONLY` (default `false` =
  enforce for the agent layer; the trigger check still needs `IDT_VALIDATION=true`).

## v1.9.0

- **feat: per-index function permission.** When a workflow's `tool/opensearch` policy peer
  implements `node.IndexFunctionProvider`, the engine resolves the identity function that guards
  the tool's index once at load (`bound` / `unbound` / `none`; logged as
  `engine: workflow=… index=… access=… function=… cause=…`).
  - `rsassistant`: the agent card declares the index function (`access.functionId`) instead of
    `APP_FUNCTION_ID`, so the roster and the agent's own token check follow it. An unbound index
    is not published (`RSASSISTANT event=workflow.skipped … reason="index function unbound: …"`);
    `agent.published` now ends with `function=<id>`.
  - `trigger/nats-chat`: the token check uses the index function. Unbound denies with
    `INDEX_FUNCTION_UNBOUND` without asking identity (logged as
    `IDT_TRACE event=validate.observed ... would_allow=false reason="INDEX_FUNCTION_UNBOUND"`
    while `IDT_OBSERVE_ONLY=true`; the trigger check only runs when `IDT_VALIDATION=true`).
    `IDT_TRACE` lines gain `function=<id>` (`<id>`, or `unbound`, or `app-default` for
    workflows without an index function).
  - New optional interfaces in `pkg/workflow/node`: `IndexScoped` (implemented by
    `tool/opensearch`), `IndexFunctionProvider`, `IndexAccessAware`; new
    `pkg/idt` `Validator.ValidateForFunction`. A policy node that does not implement
    `IndexFunctionProvider` keeps today's behavior (`none`, `APP_FUNCTION_ID`).
- **Fail-closed hardening:** an `IndexAccess` that is `bound` with a blank function id, or has an
  unrecognized status, is treated as unbound by `rsassistant` and the trigger.

## v1.8.0

- **feat: `ai/inference-gateway` LLM node (alias-only).** Model calls go through the org
  inferenceGateway NATS service (`<INFERENCE_GATEWAY_BASE_PATH>.invokeStream`). The node config
  is `alias` (a gateway tier) plus timeouts/basePath; the gateway alias row resolves the model
  and its `paramPolicy` owns every inference parameter. The body carries `alias`, `system`,
  `messages`, `tools` and forced tool choice only — never `maxTokens`, `temperature` or stop
  sequences. Unknown config keys fail the workflow at load. A `gateway-policy` log line reports
  the ack's `appliedParams`/`paramWarnings` when a policy ran. Streams text and tool-use events
  with seq checking, idle (120 s) and stream (600 s) timeouts. Document blocks are rejected.
- **refactor(agent): no hardcoded `maxTokens`.** The agent loop no longer sets `MaxTokens: 4096`;
  each provider decides. `ai/bedrock` payload is unchanged when its node `maxTokens` is unset or
  4096 (the default); a bedrock node with any other configured `maxTokens` now has it honoured (it
  was previously overridden by the agent's 4096). Custom `LLMProvider` implementations must apply
  their own default, since the agent now sends `MaxTokens == 0`. `ai/bedrock` is otherwise
  untouched and still available.
- **feat: `llm-timing` log line** from both LLM providers, one per call:
  `llm-timing wf= node= provider= model= ttfb_ms= total_ms= in_tok= out_tok= stop= err=`.
  `ai/bedrock` now parses token usage from `message_start` / `message_delta` for it.
- **fix(bedrock): Claude 5.5+ payload.** `thinking: disabled` is now sent only for Claude 5.0–5.4;
  Bedrock rejects it on 5.5 (`"thinking.type.disabled" is not supported for this model`). 5.5 and
  newer get no `thinking` field (adaptive default). **Auto-update and Claude 5.5+:** the daily
  auto-update health probe forces a tool call, and Claude 5.5+ rejects forced `tool_choice` (400
  ValidationException). The probe therefore always fails on 5.5+, is treated as conclusive, and
  recovery swaps live traffic to an older validated model. When pinning a 5.5+ model set
  `autoUpdate: false` (or `AI_BEDROCK_MODEL_ID`). Auto-update also cannot promote a node to 5.5+.
  Follow-up: skip the forced-tool probe on 5.5+.

## v1.7.3

- fix(opensearch): refuse document reads and keep single-object clauses (document-read guard).

## v1.7.2

- **security(opensearch): aggregation allowlist.** The OpenSearch tool now validates the
  model's `aggregations` before calling OpenSearch and only accepts types that are computed
  over the security-scoped query (terms, multi_terms, rare_terms, date_histogram,
  auto_date_histogram, histogram, range, date_range, filter, filters, missing, composite,
  nested, the metrics, top_hits, and bucket pipelines). `global`, `significant_terms`,
  `significant_text`, `sampler`, `diversified_sampler`, `scripted_metric` and any unknown
  type are rejected with an error that tells the model not to work around the security
  scope. Before, a `global` aggregation ignored the injected account filter and returned
  other accounts' counts (reproduced in Dev with a restricted SUBMITTER account).
  The whole tree (nested `aggs`/`aggregations`) is checked; a stringified object is decoded
  first; OpenSearch receives the re-serialized validated tree, never the raw model bytes.
  Replay of 90 days of Production queries: only the single `global` query is newly rejected.

## v1.7.1

- fix(agent): a model turn that stops with `max_tokens` (or `stop_sequence`) and makes no
  tool call now completes with its text and reports that `messageStop`. Before, the loop
  treated it as a tool turn, appended an empty tool_result user message, and the next
  Bedrock call failed with `user messages must have non-empty content` (`llm-error`),
  losing a long answer after the full run time. A guard now refuses to send an empty
  user message.
- feat(executor): one log line per workflow run with its timings, in both streaming and
  single mode:
  `RUN_METRIC event=run.done workflow= request= session= end=complete|error stop= tools= events= first_event_ms= first_text_ms= ms=`.
  No identity is logged; `request`/`session` correlate with the trigger's lines.

## v1.7.0

- feat(rsassistant): opt-in publication of eligible `trigger/nats-chat` workflows as
  RSAssistant-discoverable agents (nats-agent v0.2.0). One agent per eligible
  workflow (streaming or override-capable, no client-only UI tools), single
  `access{APP_ID, APP_FUNCTION_ID}` pair, identity verified before the workflow is
  called, engine headers derived only from the verified identity. Disabled unless
  `RSASSISTANT_ENABLED=true`. New optional top-level workflow key `rsassistant`
  (inherited through `extends`). The bridge's own stream client fails fast on a
  workflow's pre-stream rejection or NATS no-responders. See `docs/RSASSISTANT.md`.
- feat(natschat): `ChatEndpoint` read-only accessors on `trigger/nats-chat`.
- feat(loader): `LoadResult.RSAssistant` / `Workflow.RSAssistant` carry the raw
  manifest (nil when absent). No behavior change for existing files.
- build: add `github.com/transactrx/nats-agent v0.2.0` (requires Go 1.26.5).
  **Minimum Go is now 1.27.0** (`go 1.27.0` directive, latest Go family). Consumers
  building in older `golang:*` images (which set `GOTOOLCHAIN=local`) must move to
  `golang:1.27-alpine` or newer. CI (`go.yml`, `release.yaml`, `docker-compose.yml`)
  builds and tests on `1.27.x` / `golang:1.27-alpine`. Test-only
  `nats-server` v2.14.0 → v2.14.3.

## v1.6.2

- fix(postgres-query): the per-connection `statementTimeoutMs` is now applied with
  `SET statement_timeout` after connect instead of a startup parameter. The platform's
  pgbouncer sidecars reject unknown startup parameters ("FATAL: unsupported startup
  parameter: statement_timeout"), which made every query through a sidecar fail even
  after v1.6.1's sslmode fix. The sidecars run `pool_mode=session` with
  `server_reset_query=DISCARD ALL`, so the per-connection SET is safe and reset on
  release. Verified against the deployed pgbouncer image: timeout takes effect and
  cancels runaway statements with SQLSTATE 57014.

## v1.6.1

- fix(postgres-query): connection DSN `sslmode` is now configurable per connection
  (`sslMode`, validated against the libpq set) and defaults to **`prefer`** instead of
  the previous hardcoded `require`. `require` could never negotiate with the platform's
  localhost pgbouncer sidecars (no TLS listener), which made the tool unusable in the
  standard ECS topology; `prefer` matches the platform convention (powerlineClaimSearchApi
  DSNs carry no sslmode) — TLS is still negotiated whenever the server offers it, and
  deployments with an off-task DBHOST can pin `require`/`verify-full` via config.
- feat(mongo-query): the `database` input is now optional — it defaults to the database
  named in the connection URI's path (e.g. `.../devrepository?...`), so each environment's
  secret carries its own database. With no `allowedDatabases` configured, only that
  URI-default database is permitted (clear error otherwise); a non-empty `allowedDatabases`
  keeps the previous allowlist behavior unchanged.

## v1.6.0

- feat(builtin): five node types promoted from backendBatchProcessingAI into the
  library and registered by `RegisterDefaults` — `tool/postgres-query` (read-only
  SELECT over named connections), `tool/mongo-query` (read-only find/aggregate/count),
  `tool/rabbitmq-management` (read-only Management-API inspection), `tool/web-fetch`
  (HTTPS GET → stripped text), and `admin/prompt-rewrite` (streaming AI prompt editor).
  **Additive: no existing interface, node type, or option changed; all prior workflows
  behave identically.** New deps: `mongo-driver/v2`, `pgxmock/v4` (test).
- feat(web-fetch): SSRF guard — `https`-only and host-denylist checks run on the initial
  URL **and every redirect hop**, and a dial-time control blocks loopback / RFC1918 /
  link-local (incl. `169.254.169.254`) / CGNAT `100.64.0.0/10` / IPv6-ULA addresses.
  `allowPrivateHosts` opts out for internal endpoints.
- feat(mongo-query): refuses write/JS operators (`$out`, `$merge`, `$where`, `$function`,
  `$accumulator`) before reaching the driver; `find`/`aggregate` cap results and set a
  `truncated` flag; a bare `{"$date": …}` root returns a validation error instead of
  panicking.
- feat(postgres-query, mongo-query, rabbitmq-management): the declared per-connection
  timeouts (`statementTimeoutMs` / `queryTimeoutMs` / `requestTimeoutMs`) are now
  enforced (were parsed but ignored). Timeout/deadline errors surface as a typed
  `timeout` `ToolError`. `truncated` flag added to `postgres-query`.
- feat(tools): `failurePolicy` is configurable on `postgres-query`, `mongo-query`,
  `rabbitmq-management`, and `web-fetch` (default `surface-to-llm`).
- perf(prompt-rewrite): the Bedrock client is built once in `Init` (behind an interface)
  instead of per request.
- build(deps): all modules upgraded to latest minor/patch (within-major, no breaking
  bumps) — `pgx/v5` 5.10.0, `nats.go` 1.53.1, `nats-service` 1.4.46, the aws-sdk-go-v2
  suite (bedrock 1.66.5, bedrockruntime 1.57.2, dynamodb 1.63.2, s3 1.107.1, …),
  `smithy-go` 1.27.7, `gofiber` 2.52.14, `golang.org/x/*`.
- tests: per-node suites for every hardening path (SSRF redirect/dial guard, operator
  blocklist, timeout enforcement, truncation flags, panic-safety). Verified compatible
  with `opensearchAiChatApi` and `powerlineClaimSearchWebApp` (both compile clean).

## v1.5.1

- fix(agent): empty tool results are padded ("(tool returned no output)") — Bedrock
  rejects empty content blocks (was a 500 to the caller).
- fix(agent/natschat): a request timeout that fires mid-LLM-call now surfaces as
  `timeout` (single mode: HTTP-style 504) instead of `llm-error`/500. **Caller-visible
  status change for this failure mode.** Client cancellation still maps to `cancelled`.
- fix(engine): `extends` resolves by workflow id (not source/file id); duplicate
  workflow ids and non-string `extends` values now fail that workflow loudly
  (isolation preserved).
- fix(loader): deepMerge no longer aliases input maps; `$remove` markers are stripped
  from merged configs.
- docs(opensearch): tool description corrected — the server injects security
  constraints only (no date clause exists).
- tests: derivation passthrough golden test; natschat terminator-precedence and
  cancel-vs-timeout pins.

## v1.5.0

- `engine/loader`: derived workflows — a workflow JSON may declare `extends: "<source id>"`
  and be built as an overlay on its parent: deep merge with null-delete, `$remove` node
  list with connection pruning, and a prompt carve-out (prompts are never inherited;
  an overlay must own its prompts explicitly).
- `engine`: two-pass `LoadAll` resolves overlays after all sources load; failures
  isolate the overlay (parent workflows still start). `WORKFLOW_DERIVE_DEBUG=true`
  dumps each merged config at load for equivalence checks.
- Docs: derived-workflows section in `docs/CHAT-ENDPOINTS.md`.

## v1.4.0

- `trigger/nats-chat`: new `responseMode: "single"` — one request/reply JSON answer
  (`{sessionId, finalText, stopReason, attachments[]}`) instead of the event stream.
- `trigger/nats-chat`: new `allowResponseModeOverride` flag — callers may override the
  workflow's mode per request via `responseMode` in the body; invalid values get a 400.
- Docs: caller contract for machine (non-UI) workflows (`docs/CHAT-ENDPOINTS.md`).

(Release note added retroactively to re-trigger the release pipeline after the
2026-08-06 GitHub Actions outage left the original release run unrecoverable.)
