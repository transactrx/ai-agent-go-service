# Extending & wiring `ai-agent-go-service`

This is the "how do I use it" guide. The library has **two halves joined by NATS**; you can
use either or both:

```
 Browser ──HTTP/WS──►  WEB HALF (webbridge)  ──NATS──►  AGENT HALF (agent engine)  ──► Bedrock / OpenSearch / …
                       package: webbridge                package: agent + pkg/workflow/…
                       (webApp mounts this)              (chatApi runs this)
```

- **Agent half** — the NATS-fronted workflow engine. You register node types, drop workflow
  JSON files in a directory, and call `agent.NewService(...).Run(ctx)`. Consumed by
  **opensearchAiChatApi** (chatApi).
- **Web half** — a Fiber router group that turns a browser WebSocket into NATS chat requests
  and streams the reply back. You call `webbridge.Mount(app, Options{...})`. Consumed by
  **powerlineClaimSearchWebApp** (webApp).

Module path: `github.com/transactrx/ai-agent-go-service` (Go 1.25). Private — consumers set
`GOPRIVATE=github.com/transactrx/*` and `go get github.com/transactrx/ai-agent-go-service@vX.Y.Z`.

---

## Part 1 — Agent half (the "API")

### 1.1 Minimal service

The whole agent half is one facade: `agent.NewService(opts...).Run(ctx)`. The generic reference
binary [`cmd/ai-agent-service`](../cmd/ai-agent-service/main.go) is literally:

```go
package main

import (
	"context"
	"log"

	"github.com/transactrx/ai-agent-go-service/agent"
)

func main() {
	if err := agent.NewService().Run(context.Background()); err != nil {
		log.Fatalf("ai-agent-service: %v", err)
	}
}
```

That already serves every library **built-in** node type (see §1.4). It loads workflow JSON from
`./workflows` and serves each over NATS. If you only use built-ins, this binary is all you need.

### 1.2 The real consumer: chatApi

When you need a **tenant-specific node** (e.g. a custom access-policy), write your own `main` and
register it with `WithNode`. This is the entire chatApi entrypoint
(`opensearchAiChatApi/cmd/opensearchaichatapi/main.go`), verbatim:

```go
func main() {
	svc := agent.NewService(
		agent.WithWorkflowsDir("./workflows"),
		// Preserve this service's existing log prefix and advertised repo metadata.
		agent.WithAppName("opensearchAiChatApi"),
		agent.WithRepositoryURL("https://github.com/transactrx/opensearchAiChatApi"),
		// Tenant-specific policy node stays local; registered alongside library defaults.
		agent.WithNode("policy/powerline-scope", powerlinescope.Factory),
	)
	if err := svc.Run(context.Background()); err != nil {
		log.Fatalf("opensearchAiChatApi: %v", err)
	}
}
```

**The pattern:** everything generic comes from the library; the tenant keeps only its custom
node factory (`powerlinescope.Factory`) and its workflow JSON files. Nothing is forked.

### 1.3 Service options (`agent` package)

| Option | Default | Purpose |
|---|---|---|
| `WithWorkflowsDir(dir)` | `./workflows` | Directory of workflow JSON files to load |
| `WithNode(typeKey, factory)` | — | Register (or override) a node type. Repeat per node. |
| `WithHost(key, handle)` | — | Inject an extra transport/host handle into the engine hosts map |
| `WithAppName(name)` | `ai-agent-service` | Log prefix / S3 bucket fallback name |
| `WithRepositoryURL(url)` | library repo | Advertised repo metadata |
| `WithLogger(l)` | built internally | Custom `*log.Logger` |

`Run(ctx)` loads NATS config from the environment (see [DEPLOYMENT.md](DEPLOYMENT.md)), builds the
node registry (library defaults + your `WithNode` factories), loads the workflows, registers the
NATS endpoints, and blocks until `SIGINT`/`SIGTERM`.

### 1.4 Node system & built-ins

A workflow is a graph of **nodes** wired by typed **ports**. Node type keys map to factories in a
`node.Registry`. Interfaces live in `github.com/transactrx/ai-agent-go-service/pkg/workflow/node`:

| Role | Interface | Key method |
|---|---|---|
| `trigger` | `Trigger` | `Subscribe(ctx, sink)` |
| `llm` | `LLMProvider` | `Stream(ctx, req, out)` |
| `memory` | `Memory` | `Load` / `Append` |
| `tool` | `Tool` | `ToolSpec()` / `Invoke(ctx, args)` |
| `agent` | `Agent` | `Process(ctx, in, sink)` |
| `policy` | `Policy` | `Resolve(ctx, req)` |
| (client tool) | `ClientUITool` | `Tool` + `ClientOnly()` |

Every node also implements `Node` (`Spec()`, `Init(ctx, env)`, `Close(ctx)`). Nodes never touch
engine internals — all coupling goes through the `NodeEnv` passed to `Init` (logger, secrets via
`env.Secret(envVar)`, template rendering, peer lookup, host handles).

**Built-in node types** (registered automatically, no wiring needed):

- **Trigger:** `trigger/nats-chat` — caller-facing contract of the endpoints it registers
  (request shape, single vs streaming consumption): [CHAT-ENDPOINTS.md](CHAT-ENDPOINTS.md)
- **LLM:** `ai/bedrock` (Claude via AWS Bedrock, optional daily model auto-update)
- **Agent orchestrator:** `ai/agent` (the LLM↔tool loop; `maxIterations`, fixed + admin-tunable system prompt)
- **Memory:** `memory/postgres`, `memory/dynamodb`
- **Data tools:** `tool/opensearch`, `tool/serpapi`, `tool/quickchart`
- **Client-UI tools:** `tool/ui-confirm`, `tool/ui-pick-one`, `tool/ui-pick-many`, `tool/ui-human-input`,
  `tool/ui-ask-date`, `tool/ui-ask-form`, `tool/ui-pick-row`, `tool/ui-ask-number`, `tool/ui-ask-long-text`

### 1.5 Writing a custom node

Implement the role interface and expose a `node.Factory`. `node.FactoryFunc` adapts a plain
function. This is the shape of chatApi's policy node
(`pkg/workflow/builtin/powerlinescope/factory.go`):

```go
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("policy/powerline-scope: parse config: %w", err)
	}
	// ...validate cfg...
	return &powerlineScope{cfg: cfg}, nil
})

// *powerlineScope implements node.Policy:
func (p *powerlineScope) Spec() node.NodeSpec { /* declare role, ports, schemas */ }
func (p *powerlineScope) Init(ctx context.Context, env node.NodeEnv) error { /* open DB via env.Secret(...) */ }
func (p *powerlineScope) Close(ctx context.Context) error { return nil }
func (p *powerlineScope) Resolve(ctx context.Context, req node.PolicyRequest) (node.PolicyResult, error) { /* … */ }
```

Register it with `agent.WithNode("policy/powerline-scope", powerlinescope.Factory)`. Use the same
type key in your workflow JSON. `WithNode` also **overrides** a built-in if you reuse its key.

### 1.6 Workflow JSON

Workflows are JSON graphs loaded from `WithWorkflowsDir`. Each file = one workflow; its filename
(minus `.json`) is the workflow id and the NATS sub-subject. Values support `${VAR}` and
`${VAR:default}` env substitution at load time. The canonical, working examples are chatApi's
`opensearchAiChatApi/workflows/powerlineSearch.json` and `eprescribeSearch.json` — copy their shape
(a `trigger/nats-chat` → `ai/agent` wired to an `ai/bedrock` LLM, a memory node, tools, and an
optional policy). See [DEPLOYMENT.md](DEPLOYMENT.md) for the env vars those workflows reference.

---

## Part 2 — Web half (the "WEBSERV-UI")

### 2.1 What `webbridge.Mount` does

`webbridge.Mount(router, Options)` registers the chat routes on your Fiber app: it mints a
one-time WS token, upgrades the WebSocket, publishes each turn to NATS (`<NATSChatPath>.<workflowId>`),
and streams the agent's reply frames back to the browser. It does **not** own auth, the frontend,
or storage — you inject those.

```go
func Mount(router fiber.Router, o Options) error
```

`Options` (only `NATSChatPath` and `Auth` are required):

| Field | Type | Default when omitted | Purpose |
|---|---|---|---|
| `NATSChatPath` | `string` | **required** | NATS base subject (must equal the agent's `NATS_BASE_PATH`) |
| `Auth` | `Authenticator` | **required** | Identity seam (see §2.2) |
| `Authorizer` | `Authorizer` | `AllowAll{}` | Per-stream workflow-access gate (see §2.3) |
| `Logger` | `*log.Logger` | `log.Default()` | |
| `UI` | `fs.FS` | `nil` → no UI route | `NoUI` or `DefaultWebixUI` (see §2.4) |
| `UIMountPath` | `string` | `/aichatviewer/ui` | Where `UI` is served |
| `Charts` | `*S3Charts` | `nil` → route off | Presigned chart redirect |
| `Uploads` | `AttachmentStorage` | `nil` → route off | Attachment upload storage |
| `UploadConfig` | `*UploadConfig` | 25 MB + image/pdf/csv/text/json | Upload limits |

**Routes registered:** `POST /aichatviewer/token`, `GET /aichatviewer/workflows`,
`GET /aichatviewer/stream` (WebSocket); conditionally `POST /aichatviewer/upload` +
`GET /aichatviewer/upload-local/:id/:filename` (if `Uploads` set), `GET /aichatviewer/chart/:name`
(if `Charts` set), and the static UI at `UIMountPath` (if `UI` set). Upload/chart routes are
gated by authentication; token/workflows/stream gate inside their handlers.

### 2.2 The real consumer: webApp

This is webApp's entire wiring (`powerlineClaimSearchWebApp/pkg/aichatviewer/bridge_mount.go`),
verbatim — the template to copy:

```go
func Mount(app fiber.Router, s *session.Session, logger *log.Logger, natsAIChatPath string) {
	natsBasePath = natsAIChatPath

	auth := webbridge.GoFiberSessionAuth{
		Session: s,
		Resolve: func(c *fiber.Ctx) (accountID, userID, userName string, err error) {
			accountID, userID, err = common.GetAccountAndUserFromStore(c)
			if err != nil {
				return "", "", "", err
			}
			userName, _ = common.GetNameFromStore(c)
			return accountID, userID, userName, nil
		},
	}

	opts := webbridge.Options{
		NATSChatPath: natsAIChatPath,
		Auth:         auth,
		Authorizer:   powerlineAuthorizer{},
		Logger:       logger,
		UI:           webbridge.NoUI, // keep serving local webapp/aichat-webix
		Uploads:      uploadsFromEnv(context.Background(), logger),
		Charts:       chartsFromEnv(),
	}
	if err := webbridge.Mount(app, opts); err != nil {
		log.Panicf("aichatviewer: webbridge mount: %v", err)
	}
}
```

### 2.3 Auth seam (`Authenticator`)

The bridge's only real host coupling. Interface:

```go
type Authenticator interface {
	Identify(c *fiber.Ctx) (Identity, error)          // who is this cookie-authed request
	WarmCredential(c *fiber.Ctx, sessionID string)    // capture live credential for the WS to use later
	OutboundMessage(sessionID, subject string, headers map[string]string, body []byte) *nats.Msg
	Release(sessionID string)                          // drop per-session state on WS close
}

type Identity struct{ AccountID, UserID, UserName, TimeZone, SessionID string }
```

The WS dial drops cookies, so `WarmCredential` stashes the live credential at token-mint time and
`OutboundMessage` re-attaches it per turn.

**Shipped default:** `GoFiberSessionAuth` wraps `trx-gofiber-session` + the IDT registry — you only
provide the session and a `Resolve` func that pulls `(accountID, userID, userName)` from your
session store (see webApp above). Fields: `Session *session.Session`, `Resolve func(...)`,
`ConnName string` (optional NATS client name, default `ai-agent-webbridge`). If you don't use
gofiber-session, implement the four-method interface yourself.

### 2.4 Authorization gate (`Authorizer`)

Optional per-stream, fail-closed gate. `nil` → `AllowAll{}`.

```go
type Authorizer interface {
	Authorize(accountID, indexName, workflowID string) (allow bool, reason string)
}
```

Called **once per stream** right after the WS opens. On deny the bridge sends an error frame and
closes:

```json
{ "event": "error", "data": { "code": "workflow-not-authorized",
  "message": "This AI assistant is not available for this Search" } }
```

webApp injects `powerlineAuthorizer{}`, which asks the PowerLine API over NATS whether
`(account, index, workflow)` is allowed (fail-closed on any error). Keep tenant authorization
logic in your host like this.

### 2.5 UI serving — and the Webix requirement

The chat frontend is framework-free protocol/state code plus a **Webix** renderer. Two modes:

- **`UI: webbridge.NoUI`** (webApp's choice) — the bridge registers **no** static route; you serve
  your own frontend assets. Use this when your app already ships the `aichat-webix` files (the whole
  TransactRx ecosystem loads Webix this way).
- **`UI: webbridge.DefaultWebixUI`** — the bridge serves the library's embedded `aichat-webix`
  asset tree at `UIMountPath` (default `/aichatviewer/ui`) via go:embed.

> **Important:** `DefaultWebixUI` embeds the *chat app* files only — **not** Webix itself. Either
> mode requires your host HTML page to load these globals before the chat modules:
> **Webix v11** (JS + CSS) and **marked** (Markdown rendering). These are referenced as globals
> (`webix.ajax`, `webix.template.escape`, `marked.parse`) by the UI code. (`turndown` appears in a
> test comment but is not used at runtime.) There is intentionally **no** fully self-contained
> standalone bundle — hosting Webix is the consumer's job, matching every other TransactRx app.

### 2.6 Uploads & charts (optional)

- **Uploads:** set `Uploads` to any `AttachmentStorage` (`Put(ctx, AttachmentInput) (AttachmentResult, error)`).
  Local FS: `webbridge.NewLocalStorage(root, "/aichatviewer/upload-local")` plus a background
  `webbridge.NewLocalJanitor(root, ttl, time.Hour, logger).Start(ctx)` (default TTL 24 h). S3:
  `webbridge.NewS3StorageWithClient(yourS3Client, bucket, region, ttl)` where you provide the
  `S3Client` (`PutObject` / `PresignGet`). Tune limits via `UploadConfig{MaxBytes, AllowedMime}`.
- **Charts:** set `Charts: &webbridge.S3Charts{Bucket, Region}` to enable the presigned
  `/aichatviewer/chart/:name` redirect (used by the `tool/quickchart` output).

webApp builds both from env (`POWERLINE_AICHAT_UPLOAD_DIR` / `_TTL_HOURS`,
`POWERLINE_AICHAT_CHART_BUCKET` / `_REGION`) — see [DEPLOYMENT.md](DEPLOYMENT.md).

### 2.7 Client protocol (for reference)

1. `POST /aichatviewer/token` (cookie-authed) → `{ "token": "<uuid>" }` (60 s TTL, single-use).
2. Open `GET /aichatviewer/stream?token=<uuid>` (WebSocket).
3. Send the first frame:
   ```json
   { "event": "user_message", "data": {
       "text": "…", "sessionId": "", "workflowId": "powerlineSearch",
       "indexName": "…", "timeZone": "America/New_York", "attachments": [] } }
   ```
   `workflowId` is resolved + authorized once and reused for the connection; `sessionId` threads
   multi-turn memory (send it back on later turns); `attachments[].data` is inline base64.
4. Receive frames: `start`, `delta`, `thought`, `tool_call`, `tool_result`, `complete`, `error`,
   plus a 20 s `ping` keepalive. The bridge publishes each turn to `<NATSChatPath>.<workflowId>`
   with identity headers `X-Account-Id` / `X-User-Id` / `X-User-Name` / `X-Time-Zone`.

The bundled `aichat-webix` UI already speaks this protocol
(`services/aichatWebixService.js`); you only need it if you write a custom frontend.

---

## See also

- [DEPLOYMENT.md](DEPLOYMENT.md) — environment variables, NATS wiring, secrets, release/versioning.
- [MIGRATION.md](MIGRATION.md) — how chatApi and webApp moved onto the library.
