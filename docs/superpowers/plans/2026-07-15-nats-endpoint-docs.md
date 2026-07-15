# NATS Endpoint Documentation Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill in the `Description`, `Headers`, and `Response` documentation on every `EndpointRegistration` passed to `AddEndpointWithDocs` in ai-agent-go-service so the discovery viewers render clear, example-rich docs.

**Architecture:** Documentation-content-only change. Three registration sites get richer literal values; zero handler/runtime logic changes. Viewers auto-parse a trailing `Request body: {field1, field2}` sentence from descriptions and `JSON.parse`+pretty-print the `Response.Example` string, so examples must be compact valid JSON strings.

**Tech Stack:** Go; nats-service doc structs (`HeaderDoc`, `ResponseDoc`) — already support everything needed.

## Global Constraints

- Repo: `/Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service`, work on branch `feature/endpoint-docs` off `Development`.
- **DO NOT COMMIT.** User must explicitly approve before any commit. Leave changes in the working tree. Every "commit" step normally in a plan is replaced by "stop; changes stay uncommitted".
- Only `EndpointRegistration` literal values change (Description, Headers, Response). No handler, struct, signature, or behavior changes. No changes to nats-service, viewers, workflow JSONs, or opensearchAiChatApi.
- `Response.Example` values must be compact, valid JSON strings (viewers `JSON.parse` them).
- No new tests (doc literals only); existing suite must stay green: `go build ./... && go test ./...`.

---

### Task 0: Create feature branch

**Files:** none

- [ ] **Step 1: Branch**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service
git checkout Development && git pull && git checkout -b feature/endpoint-docs
```

Expected: on clean `feature/endpoint-docs`.

---

### Task 1: Chat trigger registration docs

**Files:**
- Modify: `pkg/workflow/builtin/natschat/trigger.go:65-77` (the `headerDocs`/`reg` block in `Init`)

**Interfaces:**
- Consumes: existing `t.cfg.IdentitySource` fields (`AccountHeader`, `UserHeader`, `UserNameHeader`, `TimeZoneHeader`, `NatsUserHeader`, `RequireUser`, `RequireAccount` — all already populated with defaults by the factory), `nats_service.HeaderDoc{Example}`, `nats_service.ResponseDoc`.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Replace the registration block**

Replace this exact current code:

```go
	headerDocs := []nats_service.HeaderDoc{
		{Name: t.cfg.IdentitySource.NatsUserHeader, Description: "NATS user (service identity)", Required: false},
		{Name: t.cfg.IdentitySource.UserHeader, Description: "Application user id (human)", Required: *t.cfg.IdentitySource.RequireUser},
		{Name: t.cfg.IdentitySource.AccountHeader, Description: "Account id (security scope)", Required: *t.cfg.IdentitySource.RequireAccount},
	}
	reg := nats_service.EndpointRegistration{
		Path:        t.subject,
		Description: fmt.Sprintf("Workflow %s — chat trigger over NATS", t.workflowID),
		Headers:     headerDocs,
		Handler:     t.handle,
	}
	return t.natsHost.AddEndpointWithDocs([]nats_service.EndpointRegistration{reg})
```

with:

```go
	headerDocs := []nats_service.HeaderDoc{
		{Name: t.cfg.IdentitySource.AccountHeader, Description: "Account id — security scope for all data the agent can query", Required: *t.cfg.IdentitySource.RequireAccount, Example: "5480"},
		{Name: t.cfg.IdentitySource.UserHeader, Description: "Human user id — keys chat session memory and audit logging", Required: *t.cfg.IdentitySource.RequireUser, Example: "jdoe"},
		{Name: t.cfg.IdentitySource.UserNameHeader, Description: "Human display name, given to the agent for personalized answers", Required: false, Example: "John Doe"},
		{Name: t.cfg.IdentitySource.TimeZoneHeader, Description: "IANA timezone of the asker; used to resolve date questions like 'today'", Required: false, Example: "America/New_York"},
		{Name: t.cfg.IdentitySource.NatsUserHeader, Description: "Calling service identity; set automatically by the nats-service client", Required: false, Example: "powerlineWebApp"},
	}
	reg := nats_service.EndpointRegistration{
		Path: t.subject,
		Description: fmt.Sprintf(
			"AI chat endpoint for workflow '%s'. Streams the agent's answer as NATS events on the reply inbox. "+
				"Request body: {message, sessionId} — message is required; sessionId is optional, the server generates one and returns it in the start event.",
			t.workflowID),
		Headers: headerDocs,
		Response: &nats_service.ResponseDoc{
			Description: "Stream of NATS events on the reply inbox: start → (delta | thought | tool_call | tool_result | attachment)* → complete | error. " +
				"Event type is in the _Stream_Event header, ordering in _Stream_Sequence. " +
				"The start event carries _Stream_Cancel_Subject — publish any message on that subject to cancel. " +
				"The example below shows each event type's payload.",
			ContentType: "application/json",
			Example:     `{"start": {"sessionId": "e1f0c9a2-4b7d-4f7e-9c1a-8f2d3e4a5b6c"}, "delta": {"text": "Today you have 1,204 paid claims..."}, "tool_call": {"toolUseId": "toolu_01", "name": "opensearch_query", "input": {"query": "..."}}, "tool_result": {"toolUseId": "toolu_01", "output": {"hits": "..."}, "isError": false}, "complete": {"finalText": "Today you have 1,204 paid claims...", "messageStop": "end_turn"}, "error": {"code": "cancelled", "message": "stream cancelled by client"}}`,
		},

<!-- CORRECTED per final review: payload keys verified against agent loop.go
(toolUseId / output+isError / finalText+messageStop); original draft used the
stale shapes from opensearchAiChatApi docs/api-reference.md. -->,
		Handler: t.handle,
	}
	return t.natsHost.AddEndpointWithDocs([]nats_service.EndpointRegistration{reg})
```

Note: header order changed to importance (account, user first) — purely cosmetic in the rendered table. `Required` flags still come from config exactly as before.

- [ ] **Step 2: Verify build + tests**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service
go build ./... && go test ./pkg/workflow/builtin/natschat/...
```

Expected: build OK, tests PASS.

- [ ] **Step 3: Stop — no commit** (changes stay uncommitted for user review)

---

### Task 2: Prompt admin registration docs

**Files:**
- Modify: `pkg/promptadmin/admin.go:54-59` (the `regs` slice in `Register`)

**Interfaces:**
- Consumes: `nats_service.EndpointRegistration{Response}`, `nats_service.ResponseDoc`, `nats_service.HeaderDoc{Example}`.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Replace the regs slice**

Replace this exact current code:

```go
	regs := []nats_service.EndpointRegistration{
		{Path: "PromptGet", Description: "Get fixed + default + current flexible prompt for a node field", Handler: s.handleGet},
		{Path: "PromptSave", Description: "Save a new flexible prompt version (X-User-Id required)", Handler: s.handleSave,
			Headers: []nats_service.HeaderDoc{{Name: "X-User-Id", Description: "Saver identity for audit", Required: true}}},
		{Path: "PromptHistory", Description: "List saved versions for a node field", Handler: s.handleHistory},
	}
```

with:

```go
	regs := []nats_service.EndpointRegistration{
		{
			Path: "PromptGet",
			Description: "Get the prompt for one overridable node field: the fixed part, the workflow-JSON default, and the current override with its source ('db' or 'default'). " +
				"Request body: {workflowId, nodeId, field}",
			Handler: s.handleGet,
			Response: &nats_service.ResponseDoc{
				Description: "Fixed + default + current prompt text, and where the current value comes from.",
				ContentType: "application/json",
				Example:     `{"fixed": "You are a pharmacy claims assistant...", "defaultFlex": "Answer using the search tool...", "currentFlex": "Answer using the search tool... (edited)", "source": "db"}`,
			},
		},
		{
			Path: "PromptSave",
			Description: "Save a new version of a flexible prompt. Content is template-validated before saving; the change is broadcast and hot-applied on all running instances. " +
				"Request body: {workflowId, nodeId, field, content}",
			Handler: s.handleSave,
			Headers: []nats_service.HeaderDoc{{Name: "X-User-Id", Description: "Author recorded in version history", Required: true, Example: "jdoe"}},
			Response: &nats_service.ResponseDoc{
				Description: "Identifier of the newly saved version.",
				ContentType: "application/json",
				Example:     `{"version": "v#0001717500000000"}`,
			},
		},
		{
			Path: "PromptHistory",
			Description: "List saved versions of a prompt field, newest first. " +
				"Request body: {workflowId, nodeId, field, limit} — limit is optional, default 20.",
			Handler: s.handleHistory,
			Response: &nats_service.ResponseDoc{
				Description: "Saved versions, newest first. Content is the raw template text (${ENV} unresolved).",
				ContentType: "application/json",
				Example:     `[{"Version": "v#0001717500000000", "Content": "Answer using the search tool...", "SavedBy": "jdoe", "CreatedAt": "2026-07-14T10:00:00Z"}]`,
			},
		},
	}
```

Note: PromptHistory example keys are capitalized on purpose — `promptstore.Version` has no JSON tags and marshals as `Version/Content/SavedBy/CreatedAt`. Document reality; do NOT add tags (that would change the wire format).

- [ ] **Step 2: Verify build + tests**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service
go build ./... && go test ./pkg/promptadmin/...
```

Expected: build OK, tests PASS.

- [ ] **Step 3: Stop — no commit**

---

### Task 3: ListWorkflows registration docs

**Files:**
- Modify: `pkg/workflowlist/workflowlist.go:48-51` (the `regs` slice in `Register`)

**Interfaces:**
- Consumes: `nats_service.ResponseDoc`.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Replace the regs slice**

Replace this exact current code:

```go
	regs := []nats_service.EndpointRegistration{
		{Path: "ListWorkflows", Description: "List currently-loaded chat workflows as [{id,description}]", Handler: s.handleList},
	}
```

with:

```go
	regs := []nats_service.EndpointRegistration{
		{
			Path: "ListWorkflows",
			Description: "List currently-loaded chat workflows. Use each id as the chat request subject: <basePath>.<id>. " +
				"Request body: {} (empty JSON object)",
			Handler: s.handleList,
			Response: &nats_service.ResponseDoc{
				Description: "Loaded workflows sorted by id.",
				ContentType: "application/json",
				Example:     `[{"id": "eprescribeSearch", "description": "AI chat over ePrescribe transactions"}, {"id": "powerlineSearch", "description": "AI chat over PowerLine pharmacy claims"}]`,
			},
		},
	}
```

- [ ] **Step 2: Verify build + tests**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service
go build ./... && go test ./pkg/workflowlist/...
```

Expected: build OK, tests PASS.

- [ ] **Step 3: Stop — no commit**

---

### Task 4: Full verification

**Files:** none (read-only verification)

- [ ] **Step 1: Whole-repo build + full test suite**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service
go build ./... && go vet ./... && go test ./...
```

Expected: all PASS, no vet complaints.

- [ ] **Step 2: Validate every Response.Example is parseable JSON**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service
grep -rhoP 'Example:\s+`\K[^`]+' pkg/workflow/builtin/natschat/trigger.go pkg/promptadmin/admin.go pkg/workflowlist/workflowlist.go | python3 -c "import sys,json; [json.loads(l) for l in sys.stdin]; print('all examples valid JSON')"
```

Expected: `all examples valid JSON`.

- [ ] **Step 3: Live docs check (manual, optional if no local NATS)**

Point opensearchAiChatApi at the modified lib and inspect the served docs:

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/opensearchAiChatApi
go mod edit -replace github.com/transactrx/ai-agent-go-service=../ai-agent-go-service && go mod tidy
# start stack per compose.local.env, then:
nats request 'trx.local._api_docs' '' | python3 -m json.tool | head -80
# revert the replace afterwards:
go mod edit -dropreplace github.com/transactrx/ai-agent-go-service && go mod tidy
```

Expected: each endpoint shows the new description (with `Request body: {...}` tail), headers with examples, and response with contentType + example. Optionally open the discovery viewer and eyeball the rendered popup.

- [ ] **Step 4: Report to user, request commit approval**

Show `git diff --stat` and a sample rendered doc. Wait for explicit approval before any commit.
