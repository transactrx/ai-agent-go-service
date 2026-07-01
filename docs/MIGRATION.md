# Migration guide

How the two production apps moved onto this library, and the pattern to follow for any app that
adopts it. The migration was **behavior-preserving**: both apps kept working identically at every
step, verified by build + tests + a live round-trip.

## Strategy

- **One feature branch per repo** (`feature/ai-agent-go-service`) is the safety net — nothing lands
  on `Development` until the whole thing is proven.
- **Leave superseded local packages in-tree but unwired** during the migration (do not mass
  comment-out). The active binary imports the library; the old copies go dormant. A final
  cutover-cleanup phase deletes them once stable.
- No feature toggles / shims that need long-term maintenance — the git branch is the rollback.

## chatApi (agent half) — worked example

**Before:** `opensearchAiChatApi` carried the whole engine locally (workflow engine, node
interfaces + built-ins, the NATS chat trigger, prompt store, secret handling, identity).

**After:** `cmd/opensearchaichatapi/main.go` is ~20 lines calling `agent.NewService(...)` (see
[EXTENDING.md](EXTENDING.md) §1.2). It imports the library `agent` package and keeps only:

- `pkg/workflow/builtin/powerlinescope/` — the tenant `policy/powerline-scope` node, registered via
  `agent.WithNode(...)`.
- `workflows/*.json` — the tenant workflow definitions (`powerlineSearch`, `eprescribeSearch`).

Everything else (engine, `trigger/nats-chat`, `ai/bedrock`, `ai/agent`, memory/tool built-ins,
prompt store + admin) now comes from the library. The old local copies of those packages remain in
`pkg/` but are **dormant** — not imported by the active binary — pending deletion in cutover
cleanup. Verified with `go list -deps`: the active binary pulls the library packages; only
`powerlinescope` stays local.

## webApp (web half) — worked example

**Before:** `powerlineClaimSearchWebApp/pkg/aichatviewer/` held ~22 generic files (NATS stream,
chunked request, client frames, storage local/S3, upload, chart redirect, models, and the
`aichathandler.go` bridge core).

**After:** the generic files were extracted into the library `webbridge` package. `pkg/aichatviewer`
now contains just a thin **host shim**:

- `bridge_mount.go` — builds `webbridge.Options` and calls `webbridge.Mount(...)` (see
  [EXTENDING.md](EXTENDING.md) §2.2).
- `authorize.go` — the PowerLine workflow-access gate injected as `Authorizer`.
- `promptadmin_routes.go` — prompt-admin proxy that stays host-side (PowerLine fnids +
  `SessionDetails.AppFuncAccess` are tenant-specific, not part of the generic bridge).

The moved generic files remain in `pkg/aichatviewer` as **dormant** (`_`-prefixed) copies, pending
cutover deletion. The `aichathandler.go` core was split: generic orchestration went to
`webbridge/bridge.go` + `mount.go`; the ~6 session/IDT call sites became the injected
`Authenticator` interface (default adapter `GoFiberSessionAuth`).

UI note: webApp mounts with `UI: webbridge.NoUI` and keeps serving its own `webapp/aichat-webix`
assets — behavior-identical to before. The library also embeds those assets (`DefaultWebixUI`) for
new consumers; either way the host page still loads Webix v11 + marked (see
[EXTENDING.md](EXTENDING.md) §2.5).

## Steps to migrate a new app

1. `export GOPRIVATE=github.com/transactrx/*`; `go get github.com/transactrx/ai-agent-go-service@vX.Y.Z`.
2. Create `feature/ai-agent-go-service`.
3. **Agent half:** replace your engine bootstrap with `agent.NewService(...).Run(ctx)`; register any
   truly tenant-specific node types via `WithNode`; move your workflow JSON under `WithWorkflowsDir`.
4. **Web half:** replace your bridge with `webbridge.Mount(app, Options{...})`; implement/inject an
   `Authenticator` (or use `GoFiberSessionAuth`) and, if you gate workflow access, an `Authorizer`;
   choose `NoUI` (serve your own frontend) or `DefaultWebixUI`.
5. Leave superseded local packages in place but unwired; confirm the active binary imports the
   library (`go list -deps`), then `go build` + `go test` + a live round-trip.
6. Delete the dormant packages in a dedicated cleanup commit once stable.

## Behavior-preservation notes (from the real migration)

- **Identity headers** stay generic: `X-Account-Id` / `X-User-Id` / `X-User-Name` / `X-Time-Zone`.
- **NATS subject** unchanged: `<NATS_BASE_PATH>.<workflowId>`.
- **`NATS_BASE_PATH` (agent) must equal `NATSChatPath` (web).**
- Env var *names* for uploads/charts/chat-path are host-chosen and pass through `Options`, so you can
  keep your existing names (webApp kept its `POWERLINE_AICHAT_*` vars).
