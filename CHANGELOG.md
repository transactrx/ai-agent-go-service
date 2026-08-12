# Changelog

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
