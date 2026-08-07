# Changelog

## v1.4.0

- `trigger/nats-chat`: new `responseMode: "single"` — one request/reply JSON answer
  (`{sessionId, finalText, stopReason, attachments[]}`) instead of the event stream.
- `trigger/nats-chat`: new `allowResponseModeOverride` flag — callers may override the
  workflow's mode per request via `responseMode` in the body; invalid values get a 400.
- Docs: caller contract for machine (non-UI) workflows (`docs/CHAT-ENDPOINTS.md`).

(Release note added retroactively to re-trigger the release pipeline after the
2026-08-06 GitHub Actions outage left the original release run unrecoverable.)
