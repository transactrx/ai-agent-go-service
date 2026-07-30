# Deployment

The library does **not** own deployment — consumers do. It ships copyable templates
(`docker-compose.yml`, `.goreleaser.yml`, `Makefile`) and this guide. Each consumer builds and
deploys its own binary and provides the environment below.

## Consuming the module

Private module — every consumer (and CI) needs:

```sh
export GOPRIVATE=github.com/transactrx/*
go get github.com/transactrx/ai-agent-go-service@vX.Y.Z
```

The module is **vendored** (`go build -mod=vendor`). Use the shipped `Makefile`:

```sh
make build   # go build -mod=vendor -v ./...
make test    # go test  -mod=vendor ./...
make vendor  # go mod vendor
make tidy    # go mod tidy
```

## Environment variables

### Agent half (`agent.NewService().Run`)

Read by the library at startup:

| Var | Required | Default | Purpose |
|---|---|---|---|
| `NATS_URL` | ✅ | — | NATS server URL |
| `NATS_QUEUE_NAME` | ✅ | — | Queue group (load balancing across replicas) |
| `NATS_BASE_PATH` | ✅ | — | NATS subject prefix; **must equal** the web half's `NATSChatPath` |
| `NATS_JWT` | — | `""` | NATS auth JWT |
| `NATS_KEY` | — | `""` | NATS auth seed |
| `AWS_REGION` | — | `unknown` | General AWS ops |
| `S3_FILES_BUCKET` | — | `<app_name>-assistant-files-<environment>` | Bucket for uploaded files/charts; service panics if it can't resolve one |
| `DYNAMODB_PROMPTS_TABLE` | — | `opensearchaichatapi-assistant-prompts` | Enables the prompt-admin override store; disabled if init fails |
| `AWS_REGION_DYNAMODB` | — | `us-east-1` | Region for the prompt store |
| `INFERENCE_GATEWAY_BASE_PATH` | — | `example.inferenceGateway` | Org inferenceGateway NATS base path; `ai/bedrock` auto-update asks `<base>.resolveModel` for the family's latest release (org value: `trx.inferenceGateway`). Unset/unreachable → Bedrock catalog-scan fallback. Set it in the consuming service's deployment env (Terraform task definition or GitHub environment vars). |
| `MODEL_AUTOUPDATE_NOTIFY_SUBJECT` | — | `<NATS_BASE_PATH>.modelAutoUpdate` | Subject for `ai/bedrock` auto-update upgraded/declined notifications |

**Workflow-referenced vars** are *not* library-core — they are read by the node configs inside your
workflow JSON via `${VAR}` / `${VAR:default}`. chatApi's `powerlineSearch.json` references, for
example: `AWS_REGION_BEDROCK`, `DYNAMODB_MEMORY_TABLE`, `OPENSEARCH_ADDR`, `OPENSEARCH_USER`,
`OPENSEARCH_PASSWORD`, `POWERLINE_CONFIG_DSN`, `POWERLINE_CPE_INDEX`, `SERPAPI_API_KEY`. Which ones
you need depends entirely on which node types your workflows use. Nodes resolve secret-bearing vars
lazily via `env.Secret("VAR_NAME")` at `Init`, so they stay redacted in logs.

### Web half (`webbridge.Mount`)

The bridge reads NATS connection settings from the environment; **everything else is passed via
`Options`** (so the env var *names* for uploads/charts/chat-path are the host's choice — the names
below are what webApp uses):

| Var | Read by | Required | Purpose |
|---|---|---|---|
| `NATS_URL` / `NATS_JWT` / `NATS_KEY` | library (`webbridge`) | `NATS_URL` ✅ | NATS connection for publishing chat turns |
| `NATS_AI_CHAT_PATH` | host → `Options.NATSChatPath` | ✅ | Base subject; **must equal** the agent's `NATS_BASE_PATH` |
| `POWERLINE_AICHAT_UPLOAD_DIR` | host → `Uploads` | — | Local upload dir; unset ⇒ upload route disabled |
| `POWERLINE_AICHAT_UPLOAD_TTL_HOURS` | host → janitor | — | Upload TTL (default 24 h) |
| `POWERLINE_AICHAT_CHART_BUCKET` | host → `Charts` | — | S3 bucket for chart presign; both bucket+region required or route off |
| `POWERLINE_AICHAT_CHART_REGION` | host → `Charts` | — | Region for chart presign |

> The two halves must agree: `NATS_BASE_PATH` (agent) == `NATS_AI_CHAT_PATH` (web) == `Options.NATSChatPath`.
> The bridge publishes to `<base>.<workflowId>`; the agent's `trigger/nats-chat` subscribes there.

## Secrets

Never wire GitHub Actions secrets into Terraform. The established pattern:

1. Terraform creates AWS Secrets Manager entries seeded with **placeholder** values and
   `lifecycle { ignore_changes = [secret_string] }`.
2. An operator populates the real value once, out of band.
3. Re-applying Terraform never overwrites the operator-set value.

Runtime pulls secrets from Secrets Manager into the env/DSNs the workflow references (e.g.
`OPENSEARCH_PASSWORD`, `POWERLINE_CONFIG_DSN`, `SERPAPI_API_KEY`).

## Local development

`docker-compose.yml` brings up NATS and runs the test suite against it:

```sh
docker compose up            # nats on :4222 (+ :8222 monitoring)
docker compose run --rm test # go test -mod=vendor ./... with NATS_URL=nats://nats:4222
```

To run the reference agent locally, export the required NATS vars and:

```sh
go run ./cmd/ai-agent-service    # loads ./workflows, serves over NATS
```

## Releasing the library

Mirrors the `nats-service` release flow:

1. Land work on `feature/ai-agent-go-service`; open a PR to `Development`.
2. `Development` → `Production` PR. The org `production-merge-policy` ruleset requires **1
   approval** + a verify-source-branch check (you cannot self-approve).
3. Merging to `Production` fires `release.yaml`: tests → `release-on-push` cuts the next patch tag
   (e.g. `v0.0.1` → `v0.0.2`) → `goreleaser` builds the reference binary.
4. Consumers bump their `require github.com/transactrx/ai-agent-go-service vX.Y.Z`, `go mod vendor`,
   rebuild, and deploy on their own schedule.

Versioning: pre-1.0 churn is allowed; promote to **`v1.0.0`** once both consumers run on the
library in production and the API is stable.
