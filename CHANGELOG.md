# Changelog

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
