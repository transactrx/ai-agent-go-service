# ai-agent-go-service

Private TransactRx company-standard library for building AI chat agents.

- **Agent half** (`agent`, `pkg/workflow/...`): NATS-fronted workflow engine.
- **Web half** (`webbridge`): WebSocket↔NATS bridge + embedded chat UI.

Consumers set `GOPRIVATE=github.com/transactrx/*` and `go get github.com/transactrx/ai-agent-go-service@vX.Y.Z`.

See `docs/` for DEPLOYMENT, EXTENDING, and MIGRATION guides.
