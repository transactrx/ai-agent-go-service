# ai-agent-go-service

Current stable release: **v1.0.0** (first stable version, promoted from v0.0.3).

Private TransactRx company-standard library for building AI chat agents. Two halves joined by NATS —
use either or both:

```
 Browser ──HTTP/WS──►  WEB HALF (webbridge)  ──NATS──►  AGENT HALF (agent engine)  ──► Bedrock / OpenSearch / …
```

- **Agent half** (`agent`, `pkg/workflow/…`) — NATS-fronted workflow engine. Register node types,
  drop workflow JSON in a directory, call `agent.NewService(...).Run(ctx)`.
- **Web half** (`webbridge`) — a Fiber route group that turns a browser WebSocket into NATS chat
  requests and streams the reply back. Call `webbridge.Mount(app, Options{...})`. Ships an embedded
  Webix chat UI (`DefaultWebixUI`).

## Quick start

Consume the private module:

```sh
export GOPRIVATE=github.com/transactrx/*
go get github.com/transactrx/ai-agent-go-service@vX.Y.Z
```

Run the generic reference agent (built-in node types only, workflows from `./workflows`):

```sh
go run ./cmd/ai-agent-service   # needs NATS_URL, NATS_QUEUE_NAME, NATS_BASE_PATH
```

Minimal agent service with a tenant node:

```go
svc := agent.NewService(
	agent.WithWorkflowsDir("./workflows"),
	agent.WithNode("policy/my-scope", myscope.Factory),
)
log.Fatal(svc.Run(context.Background()))
```

Mount the web bridge on a Fiber app:

```go
webbridge.Mount(app, webbridge.Options{
	NATSChatPath: os.Getenv("NATS_AI_CHAT_PATH"), // == agent's NATS_BASE_PATH
	Auth:         myAuthenticator,                // or webbridge.GoFiberSessionAuth{...}
	Authorizer:   myAuthorizer,                   // optional; nil ⇒ allow all
	UI:           webbridge.DefaultWebixUI,        // or NoUI to serve your own frontend
})
```

## Built-in node types

Triggers: `trigger/nats-chat` · LLM: `ai/bedrock` · Orchestrator: `ai/agent` · Memory:
`memory/postgres`, `memory/dynamodb` · Tools: `tool/opensearch`, `tool/serpapi`, `tool/quickchart`
· Client-UI tools: `tool/ui-confirm`, `tool/ui-pick-one`, `tool/ui-pick-many`, `tool/ui-human-input`,
`tool/ui-ask-date`, `tool/ui-ask-form`, `tool/ui-pick-row`, `tool/ui-ask-number`, `tool/ui-ask-long-text`.

## Documentation

- **[docs/EXTENDING.md](docs/EXTENDING.md)** — how to wire both halves: service options, the node
  system, custom nodes, `webbridge.Mount` options, auth/authorization seams, UI serving (and the
  Webix requirement), and the client protocol.
- **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** — environment variables, NATS wiring, secrets,
  local dev (`docker-compose`), and the release/versioning flow.
- **[docs/MIGRATION.md](docs/MIGRATION.md)** — how chatApi and webApp moved onto the library, and
  the steps to migrate a new app.

## Consumers

- **opensearchAiChatApi** (chatApi) — agent half.
- **powerlineClaimSearchWebApp** (webApp) — web half.
