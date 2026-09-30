# Alias-only model calls with gateway `paramPolicy` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The `ai/inference-gateway` node sends only an alias plus the conversation; the gateway alias row (with `paramPolicy`) owns the model and every inference parameter, so a model change never needs a lib or app deploy.

**Architecture:** Shrink the node `Config` to `alias` + transport settings with strict JSON decoding; strip every knob from the wire body; stop the agent loop from hardcoding `maxTokens`; surface the gateway's `appliedParams`/`paramWarnings` ack fields in one log line. The app's gateway workflow drops `maxTokens`, names its own alias via `${POWERLINE_MODEL_ALIAS:POWERLINE_CLAIM_SEARCH_MODEL}`, and an ops `setAlias` row carries `{"set":{"maxTokens":4096}}`.

**Tech Stack:** Go 1.x, `encoding/json` (`DisallowUnknownFields`), `regexp`, nats.go, embedded `nats-server` in tests, nats CLI for the alias row.

**Spec:** `docs/superpowers/specs/2026-09-29-alias-only-param-policy-design.md` (this repo). Executors read spec + plan.

Repos (both on branch `feature/inference-gateway-llm`):
- LIB = `/Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service`
- APP = `/Users/yceleiro/Documents/TransactRx/GitHub/opensearchAiChatApi`
- GW  = `/Users/yceleiro/Documents/TransactRx/GitHub/inferenceGateway` (branch `feat/alias-param-policy`, read-only here)

## Global Constraints

- LIB tests run with `go test -mod=vendor ./...` (repo vendors). APP vendors too: run `go mod vendor` in APP after ANY LIB change or GoLand builds stale code.
- Commit per task in the repo the task touches. **Never push** and never open a PR without the user's explicit approval (pushes trigger deploys).
- `ai/bedrock` wire payload must stay byte-identical (spec §3, §4.3). Do not edit `pkg/workflow/builtin/bedrock/` except adding one test.
- `admin/prompt-rewrite` untouched (spec §3).
- Alias name rule, copied from GW README: `^[A-Z][A-Z0-9_]{1,63}$`.
- Wire body allowed keys (spec §4.2): `alias`, `system`, `messages`, `tools`, `toolChoice`, `toolChoiceName`, `streamSubject`. Nothing else.
- `gateway-policy` log line format (spec §4.4): `gateway-policy wf=<id> node=<id> alias=<name> model=<invokeId> applied=<compact json> warnings=<"; "-joined>`. Emitted only when the ack carries `appliedParams` or `paramWarnings`.
- CHANGELOG: v1.8.0 is unreleased; amend its existing entry, do not add a version.
- APP env var name: `POWERLINE_MODEL_ALIAS`, default alias `POWERLINE_CLAIM_SEARCH_MODEL`. `INFERENCE_GATEWAY_ALIAS` is retired.
- Ship order (spec §9): GW deployed → alias row → LIB v1.8.0 → APP bump. Each push is user-gated.
- Parity rule (spec §6.1): the alias policy must reproduce every parameter the direct `ai/bedrock` path sends for the pinned opus 5.5. Verified set today: `{"set":{"maxTokens":4096}}` and nothing else (temperature/stop/topP/thinking absent on both paths). Re-check §6.1 before writing the row.

## Review Focus

1. A derived workflow that forgets `"maxTokens": null` inherits `maxTokens: 4096` from `powerlineSearch` and must fail at load with `unknown field "maxTokens"`, not silently ignore it. (Task 1 test `TestFactoryRejectsUnknownFields`; Task 7 manifest test asserts the key is absent.)
2. An `LLMRequest` that carries `MaxTokens`, `Temperature` or `Stop` (e.g. the bedrock probe shape, or a future agent) must still produce a body without those keys. (Task 2 test `TestBuildRequestNeverSendsKnobs` asserts on marshalled JSON keys.)
3. An alias resolved from an env var to a model id (`POWERLINE_MODEL_ALIAS=us.anthropic.claude-opus-5-5`) must fail at load with the name-rule error, never reach the gateway. (Task 1 test case `"model id in alias"`.)
4. An ack from a gateway build without policy support (no `appliedParams`, no `paramWarnings`) must behave byte-identically to today: no `gateway-policy` line, stream proceeds. (Task 4 test `TestStreamNoPolicyNoLogLine`.)
5. Removing the agent's hardcoded `MaxTokens: 4096` must not change the `ai/bedrock` payload: a zero `MaxTokens` request yields `max_tokens: 4096` from the node default. (Task 3 test `TestBuildPayloadZeroMaxTokensUsesConfigDefault`.)

---

## Part A — LIB (`ai-agent-go-service`)

### Task 1: Alias-only `Config`, strict decode, name rule

**Files:**
- Modify: `pkg/workflow/builtin/inferencegateway/factory.go:1-110,160-162`
- Test: `pkg/workflow/builtin/inferencegateway/factory_test.go`

**Interfaces:**
- Produces: `type Config struct { Alias string; StreamTimeoutSeconds, IdleTimeoutSeconds int; BasePath string }` (json tags `alias`, `streamTimeoutSeconds`, `idleTimeoutSeconds`, `basePath`). `newLLM(cfg Config) *gatewayLLM` unchanged signature. Tasks 2 and 4 construct `Config{Alias: ...}` only.

- [ ] **Step 1: Replace the validation test table and the defaults test**

Replace `TestFactoryConfigValidation` and `TestNewLLMDefaults` in `factory_test.go` with:

```go
func TestFactoryConfigValidation(t *testing.T) {
	cases := []struct {
		name, raw, wantErr string
	}{
		{"alias ok", `{"alias":"POWERLINE_CLAIM_SEARCH_MODEL"}`, ""},
		{"alias with timeouts and basePath", `{"alias":"A1","streamTimeoutSeconds":5,"idleTimeoutSeconds":2,"basePath":"trx.x"}`, ""},
		{"none", `{}`, "alias is required"},
		{"empty alias", `{"alias":""}`, "alias is required"},
		{"model id in alias", `{"alias":"us.anthropic.claude-opus-5-5"}`, "alias must match"},
		{"lowercase alias", `{"alias":"powerline"}`, "alias must match"},
		{"one char alias", `{"alias":"A"}`, "alias must match"},
		{"bad json", `{"alias":1}`, "parse config"},
		{"negative timeout", `{"alias":"A1","idleTimeoutSeconds":-1}`, "timeouts must be positive"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, err := Factory.New(json.RawMessage(c.raw))
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if n.Spec().Type != "ai/inference-gateway" || n.Spec().Role != node.RoleLLM {
					t.Fatalf("spec = %+v", n.Spec())
				}
				if _, ok := n.(node.LLMProvider); !ok {
					t.Fatal("node must implement LLMProvider")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, c.wantErr)
			}
		})
	}
}

// Every knob and selector the gateway alias now owns must be refused at load,
// so a stale workflow JSON (or an inherited base key) fails loudly.
func TestFactoryRejectsUnknownFields(t *testing.T) {
	for _, key := range []string{"maxTokens", "temperature", "modelId", "lab", "family", "model", "region", "autoUpdate"} {
		raw := `{"alias":"A1","` + key + `":1}`
		_, err := Factory.New(json.RawMessage(raw))
		if err == nil || !strings.Contains(err.Error(), `unknown field "`+key+`"`) {
			t.Fatalf("%s: err = %v, want unknown field", key, err)
		}
	}
}

func TestNewLLMDefaults(t *testing.T) {
	g := newLLM(Config{Alias: "A1"})
	if g.streamTimeout != 600*time.Second || g.idleTimeout != 120*time.Second {
		t.Fatalf("defaults = stream %s idle %s", g.streamTimeout, g.idleTimeout)
	}
	g = newLLM(Config{Alias: "A1", StreamTimeoutSeconds: 5, IdleTimeoutSeconds: 2})
	if g.streamTimeout != 5*time.Second || g.idleTimeout != 2*time.Second {
		t.Fatalf("overrides not applied: %+v", g)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd LIB && go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -run 'TestFactory|TestNewLLM' -v`
Expected: FAIL (`"none"` case gets the old "exactly one of" message; `TestFactoryRejectsUnknownFields` fails because unknown keys are ignored; `TestNewLLMDefaults` fails to compile on removed `MaxTokens` — that is fine, the package must compile after Step 3).

- [ ] **Step 3: Rewrite `Config`, `validate`, `Factory`, `newLLM`, the Init log line**

In `factory.go` replace lines 1-4 (package comment), the `const` block, `Config`, `validate`, `Factory`, `newLLM` and the `Init` log line with:

```go
// Package inferencegateway implements the ai/inference-gateway LLMProvider:
// model calls go through the org inferenceGateway NATS service
// (<base>.invokeStream). The node names an alias (tier) only; the gateway
// alias row resolves the model and, through its paramPolicy, owns every
// inference parameter. Specs: docs/superpowers/specs/2026-09-25-inference-gateway-llm-design.md,
// docs/superpowers/specs/2026-09-29-alias-only-param-policy-design.md
package inferencegateway
```

```go
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
)

const (
	nodeType             = "ai/inference-gateway"
	defaultStreamTimeout = 600 * time.Second
	defaultIdleTimeout   = 120 * time.Second
)

// aliasName is the gateway's alias rule (inferenceGateway README): a tier can
// never be mistaken for a model id, so a model id in this field fails at load.
var aliasName = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,63}$`)

// Config is the per-instance node config. Only the alias selects the model;
// model id, family and every inference parameter (maxTokens, temperature, …)
// live in the gateway alias row and its paramPolicy. Unknown keys are
// rejected so a stale workflow JSON fails at load instead of being ignored.
type Config struct {
	Alias string `json:"alias"`

	StreamTimeoutSeconds int `json:"streamTimeoutSeconds,omitempty"`
	IdleTimeoutSeconds   int `json:"idleTimeoutSeconds,omitempty"`

	// BasePath overrides INFERENCE_GATEWAY_BASE_PATH for this node.
	BasePath string `json:"basePath,omitempty"`
}

func (c Config) validate() error {
	if c.Alias == "" {
		return errors.New("ai/inference-gateway: alias is required")
	}
	if !aliasName.MatchString(c.Alias) {
		return fmt.Errorf("ai/inference-gateway: alias must match %s, got %q", aliasName, c.Alias)
	}
	if c.StreamTimeoutSeconds < 0 || c.IdleTimeoutSeconds < 0 {
		return errors.New("ai/inference-gateway: timeouts must be positive")
	}
	return nil
}

// Factory builds an ai/inference-gateway node from rawConfig.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(rawConfig))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("ai/inference-gateway: parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newLLM(cfg), nil
})
```

```go
// newLLM applies transport defaults.
func newLLM(cfg Config) *gatewayLLM {
	g := &gatewayLLM{cfg: cfg, streamTimeout: defaultStreamTimeout, idleTimeout: defaultIdleTimeout}
	if cfg.StreamTimeoutSeconds > 0 {
		g.streamTimeout = time.Duration(cfg.StreamTimeoutSeconds) * time.Second
	}
	if cfg.IdleTimeoutSeconds > 0 {
		g.idleTimeout = time.Duration(cfg.IdleTimeoutSeconds) * time.Second
	}
	return g
}
```

Init log line (was `factory.go:160-161`):

```go
	g.logger.Printf("ai/inference-gateway wf=%s node=%s ready (alias=%q subject=%s)",
		g.wfID, g.nodeID, g.cfg.Alias, g.subject)
```

Note: `json.Decoder` on `{"alias":1}` returns `json: cannot unmarshal number into Go struct field Config.alias of type string`, which is wrapped by `parse config`, so the `"bad json"` case still passes.

- [ ] **Step 4: Fix the compile breakage in sibling tests (temporary, Task 2 rewrites them)**

`payload_test.go:34,54,67,77` and `stream_test.go:78` pass `MaxTokens`, `Lab`, `Family`, `ModelID` in `Config{}`; `payload.go:95-113` reads `cfg.Lab/Family/ModelID/MaxTokens`. Task 2 replaces those. To keep this task independently green, temporarily change only `payload.go:95-98` to `Alias: cfg.Alias,` (delete the `Lab`, `Family`, `ModelID` lines) and `payload.go:102` `maxTok := 0` — and in the tests delete the `MaxTokens:`/`Lab:`/`Family:`/`ModelID:` config fields (keep `Alias: "A"`-style values valid: rename `"A"` → `"A1"`). The assertions in `TestBuildRequestSelectorsAndDefaults` about `got.Lab`, `got.MaxTokens == 4096` and `got.ModelID` will fail; delete that test now (Task 2 writes its replacement).

- [ ] **Step 5: Run the package tests**

Run: `cd LIB && go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/`
Expected: PASS. `stream_test.go:127` asserts `seen.MaxTokens == 64`: with `maxTok := 0` the body has no maxTokens → that assertion fails. Change line 127 to `if seen.Alias != "MAX_MODEL" || seen.StreamSubject == "" {` (Task 2 tightens it). Re-run: PASS.

- [ ] **Step 6: Commit**

```bash
cd LIB && git add pkg/workflow/builtin/inferencegateway && git commit -m "feat(inferencegateway): alias-only config, strict decode, alias name rule

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 2: Wire body without knobs

**Files:**
- Modify: `pkg/workflow/builtin/inferencegateway/payload.go:53-73,90-113`
- Modify: `pkg/workflow/node/llm.go:16-29`
- Test: `pkg/workflow/builtin/inferencegateway/payload_test.go`, `stream_test.go:78,127`

**Interfaces:**
- Produces: `invokeStreamRequest{Alias, System, Messages, Tools, ToolChoice, ToolChoiceName, StreamSubject}`; `buildRequest(req node.LLMRequest, cfg Config) (*invokeStreamRequest, error)` unchanged signature. Task 4's fake gateway unmarshals into this struct.

- [ ] **Step 1: Write the failing tests**

In `payload_test.go`: in `TestBuildRequestFullShape` change the `buildRequest` call to `buildRequest(req, Config{Alias: "MAX_MODEL"})` and the `want` literal to drop the knob segment, i.e. the line

```go
		`"maxTokens":512,"temperature":0.2,"stopSequences":["END"],` +
```

is deleted so `want` reads `…{"text":"\"\""}]}]}],"tools":[…`. Delete `TestBuildRequestTemperatureConverted`. Add:

```go
// The gateway alias paramPolicy owns every knob: whatever the LLMRequest
// carries, the body never names a model parameter.
func TestBuildRequestNeverSendsKnobs(t *testing.T) {
	temp := 0.2
	req := node.LLMRequest{
		MaxTokens:   512,
		Temperature: &temp,
		Stop:        []string{"END"},
		Messages:    []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "x"}}}},
	}
	got, err := buildRequest(req, Config{Alias: "POWERLINE_CLAIM_SEARCH_MODEL"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(b, &keys); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"maxTokens", "temperature", "stopSequences", "modelId", "lab", "family"} {
		if _, present := keys[k]; present {
			t.Fatalf("body must not carry %q: %s", k, b)
		}
	}
	for _, k := range []string{"alias", "messages", "streamSubject"} {
		if _, present := keys[k]; !present {
			t.Fatalf("body must carry %q: %s", k, b)
		}
	}
	if got.Alias != "POWERLINE_CLAIM_SEARCH_MODEL" || got.ToolChoice != "" || got.Tools != nil {
		t.Fatalf("body = %+v", got)
	}
}

// Forced tool choice is per-call intent, not a model parameter: still forwarded.
func TestBuildRequestForwardsToolChoice(t *testing.T) {
	req := node.LLMRequest{
		Tools:          []node.ToolSpec{{Name: "echo", InputSchema: json.RawMessage(`{"type":"object"}`)}},
		ToolChoiceName: "echo",
		Messages:       []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "x"}}}},
	}
	got, err := buildRequest(req, Config{Alias: "A1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ToolChoice != "tool" || got.ToolChoiceName != "echo" || len(got.Tools) != 1 {
		t.Fatalf("tool choice = %+v", got)
	}
}
```

In `stream_test.go:78` set `newLLM(Config{Alias: "MAX_MODEL", StreamTimeoutSeconds: 5, IdleTimeoutSeconds: 1})`. At `:127` assert the body is knob-free:

```go
	if seen.Alias != "MAX_MODEL" || seen.StreamSubject == "" {
		t.Fatalf("request = %+v", seen)
	}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd LIB && go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -run 'TestBuildRequest' -v`
Expected: `TestBuildRequestNeverSendsKnobs` FAIL (`temperature` / `stopSequences` still present); others may fail to compile until Step 3.

- [ ] **Step 3: Strip the wire struct and builder**

`payload.go` — replace `invokeStreamRequest` with:

```go
// invokeStreamRequest is the body sent to <base>.invokeStream. It carries no
// inference parameter on purpose: the gateway alias paramPolicy owns them.
type invokeStreamRequest struct {
	Alias string `json:"alias"`

	System   string        `json:"system,omitempty"`
	Messages []wireMessage `json:"messages"`

	Tools          []wireTool `json:"tools,omitempty"`
	ToolChoice     string     `json:"toolChoice,omitempty"`
	ToolChoiceName string     `json:"toolChoiceName,omitempty"`

	StreamSubject string `json:"streamSubject"`
}
```

Replace the head of `buildRequest` (comment through the temperature block, `payload.go:90-113`) with:

```go
// buildRequest converts the provider-agnostic request into the gateway body:
// alias, system, messages, tools and forced tool choice. MaxTokens,
// Temperature and Stop on req are ignored here — the gateway alias
// paramPolicy decides them (spec 2026-09-29 §4.2). Only the stream subject
// is left for Stream to fill.
func buildRequest(req node.LLMRequest, cfg Config) (*invokeStreamRequest, error) {
	out := &invokeStreamRequest{
		Alias:  cfg.Alias,
		System: req.System,
	}
```

The message/tool loops and the `ToolChoiceName` block stay as they are.

`pkg/workflow/node/llm.go:16-22` — replace the three knob fields with commented ones:

```go
type LLMRequest struct {
	System   string
	Messages []Message
	Tools    []ToolSpec

	// MaxTokens, Temperature and Stop are honoured by ai/bedrock only.
	// ai/inference-gateway never forwards them: the gateway alias paramPolicy
	// owns every inference parameter, so a model change is an alias edit.
	MaxTokens   int
	Temperature *float64
	Stop        []string
```

- [ ] **Step 4: Run the package tests**

Run: `cd LIB && go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ ./pkg/workflow/node/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd LIB && git add pkg/workflow/builtin/inferencegateway pkg/workflow/node/llm.go && git commit -m "feat(inferencegateway): body carries alias + conversation only; knobs belong to the alias paramPolicy

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 3: Agent loop stops hardcoding `maxTokens`; bedrock default proven

**Files:**
- Modify: `pkg/workflow/builtin/agent/loop.go:199-204`
- Test: `pkg/workflow/builtin/agent/loop_test.go:19-34,113-143`
- Test: `pkg/workflow/builtin/bedrock/payload_test.go` (add one test; no non-test bedrock change)

**Interfaces:**
- Consumes: `fakeLLM` in `loop_test.go` (`scripts [][]node.LLMEvent`, `calls int`); `buildAnthropicPayload(req node.LLMRequest, cfg Config, modelID string) ([]byte, error)` and `testModelGen4` in `bedrock/payload_test.go`.

- [ ] **Step 1: Write the failing agent test**

In `loop_test.go` add a `last node.LLMRequest` field to `fakeLLM` and record it:

```go
type fakeLLM struct {
	scripts [][]node.LLMEvent
	calls   int
	last    node.LLMRequest // request of the most recent Stream call
}

func (l *fakeLLM) Stream(_ context.Context, req node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	l.last = req
	if l.calls >= len(l.scripts) {
		return fmt.Errorf("fakeLLM: no more scripts")
	}
	for _, ev := range l.scripts[l.calls] {
		out <- ev
	}
	l.calls++
	return nil
}
```

Add after `TestAgentTextOnlyHappyPath`:

```go
// The agent sends no inference parameter: each LLM provider decides them
// (ai/bedrock from its node config, ai/inference-gateway via the gateway
// alias paramPolicy).
func TestAgentRequestCarriesNoKnobs(t *testing.T) {
	llm := &fakeLLM{scripts: [][]node.LLMEvent{{
		{Kind: node.LLMTextDelta, Delta: "ok"},
		{Kind: node.LLMMessageStop, Stop: "end_turn"},
	}}}
	a := &agentNode{
		cfg:        Config{MaxIterations: 5, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		llm:        llm,
	}
	a.mem = &fakeMem{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, &recordingSink{}); err != nil {
		t.Fatal(err)
	}
	if llm.last.MaxTokens != 0 || llm.last.Temperature != nil || llm.last.Stop != nil || llm.last.ToolChoiceName != "" {
		t.Fatalf("agent request must carry no knobs: %+v", llm.last)
	}
	if llm.last.System == "" || len(llm.last.Messages) == 0 {
		t.Fatalf("agent request must still carry system and messages: %+v", llm.last)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd LIB && go test -mod=vendor ./pkg/workflow/builtin/agent/ -run TestAgentRequestCarriesNoKnobs -v`
Expected: FAIL with `agent request must carry no knobs: {… MaxTokens:4096 …}`.

- [ ] **Step 3: Remove the hardcode**

`loop.go:199-204` becomes:

```go
			// No inference parameter here: the provider decides (ai/bedrock
			// node config; ai/inference-gateway → gateway alias paramPolicy).
			req := node.LLMRequest{
				System:   sys,
				Messages: msgs,
				Tools:    toolSpecs,
			}
```

- [ ] **Step 4: Write the bedrock proof test**

Append to `pkg/workflow/builtin/bedrock/payload_test.go`:

```go
// The agent no longer sends MaxTokens; the node default must fill max_tokens
// so the direct payload stays byte-identical (spec 2026-09-29 §4.3).
func TestBuildPayloadZeroMaxTokensUsesConfigDefault(t *testing.T) {
	cfg := Config{Model: "us.anthropic.claude-opus-4-7", MaxTokens: 4096, AnthropicVersion: "bedrock-2023-05-31"}
	req := node.LLMRequest{
		System:   "you are helpful",
		Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "hi"}}}},
	}
	body, err := buildAnthropicPayload(req, cfg, testModelGen4)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["max_tokens"].(float64) != 4096 {
		t.Fatalf("max_tokens: %v, want node default 4096", got["max_tokens"])
	}
	if _, present := got["temperature"]; present {
		t.Fatalf("temperature must be absent: %v", got["temperature"])
	}
}
```

- [ ] **Step 5: Run agent + bedrock tests**

Run: `cd LIB && go test -mod=vendor ./pkg/workflow/builtin/agent/ ./pkg/workflow/builtin/bedrock/`
Expected: PASS (bedrock test passes on first run: `payload.go:80-83` already falls back to `cfg.MaxTokens`).

- [ ] **Step 6: Commit**

```bash
cd LIB && git add pkg/workflow/builtin/agent pkg/workflow/builtin/bedrock/payload_test.go && git commit -m "refactor(agent): stop hardcoding maxTokens; providers own inference parameters

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 4: Surface `appliedParams` / `paramWarnings` from the ack

**Files:**
- Modify: `pkg/workflow/builtin/inferencegateway/client.go:35-42`
- Modify: `pkg/workflow/builtin/inferencegateway/stream.go:63-67`
- Test: `pkg/workflow/builtin/inferencegateway/stream_test.go`

**Interfaces:**
- Produces: `streamAck{Accepted, ModelID, InvokeID, ErrorMessage, AppliedParams map[string]any, ParamWarnings []string}`; `func policyLine(wf, nodeID, alias, model string, ack streamAck) (string, bool)`.

- [ ] **Step 1: Write the failing tests**

Append to `stream_test.go`:

```go
const policyAck = `{"accepted":true,"modelId":"anthropic.claude-opus-5-5","invokeId":"us.anthropic.claude-opus-5-5",` +
	`"appliedParams":{"alias":"POWERLINE_CLAIM_SEARCH_MODEL","maxTokens":4096},"paramWarnings":["maxTokens set"]}`

var doneEvents = []string{
	`{"seq":0,"type":"messageStart"}`,
	`{"seq":1,"type":"delta","contentIndex":0,"text":"ok"}`,
	`{"seq":2,"type":"messageStop","stopReason":"end_turn"}`,
	`{"seq":3,"type":"done"}`,
}

// A gateway alias with a paramPolicy reports what it compiled; the node logs
// it once so a policy change is visible in the app log with no code change.
func TestStreamLogsGatewayPolicy(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: policyAck, events: doneEvents}
	fg.serve(t, nc)
	var logs bytes.Buffer
	if _, err := collect(t, newTestLLM(nc, &logs), textReq()); err != nil {
		t.Fatal(err)
	}
	line := logs.String()
	for _, want := range []string{
		"gateway-policy wf=wf1 node=n1 alias=MAX_MODEL model=us.anthropic.claude-opus-5-5",
		`applied={"alias":"POWERLINE_CLAIM_SEARCH_MODEL","maxTokens":4096}`,
		"warnings=maxTokens set",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("policy line missing %q in:\n%s", want, line)
		}
	}
}

// An ack without policy fields (alias without policy, or an older gateway)
// must leave the log exactly as before.
func TestStreamNoPolicyNoLogLine(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: okAck, events: doneEvents}
	fg.serve(t, nc)
	var logs bytes.Buffer
	if _, err := collect(t, newTestLLM(nc, &logs), textReq()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "gateway-policy") {
		t.Fatalf("no policy line expected:\n%s", logs.String())
	}
}

func TestPolicyLineFormat(t *testing.T) {
	ack := streamAck{AppliedParams: map[string]any{"maxTokens": float64(4096), "alias": "A1"}, ParamWarnings: []string{"maxTokens set", "topP dropped"}}
	got, ok := policyLine("wf", "n", "A1", "us.m", ack)
	want := `gateway-policy wf=wf node=n alias=A1 model=us.m applied={"alias":"A1","maxTokens":4096} warnings=maxTokens set; topP dropped`
	if !ok || got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
	if _, ok := policyLine("wf", "n", "A1", "us.m", streamAck{}); ok {
		t.Fatal("empty ack must produce no line")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd LIB && go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -run 'TestStreamLogsGatewayPolicy|TestStreamNoPolicyNoLogLine|TestPolicyLineFormat' -v`
Expected: compile error `undefined: policyLine` / unknown field `AppliedParams`.

- [ ] **Step 3: Implement**

`client.go` — replace `streamAck` and add `policyLine`:

```go
// streamAck is the invokeStream reply (gateway models.InvokeStreamAck) plus
// the NatsServiceError field a failure carries. AppliedParams and
// ParamWarnings are present only when the alias carries a paramPolicy.
type streamAck struct {
	Accepted      bool           `json:"accepted"`
	ModelID       string         `json:"modelId"`
	InvokeID      string         `json:"invokeId"`
	ErrorMessage  string         `json:"errorMessage"`
	AppliedParams map[string]any `json:"appliedParams,omitempty"`
	ParamWarnings []string       `json:"paramWarnings,omitempty"`
}

// policyLine renders the gateway-policy log line, or ok=false when the ack
// carried no policy result (nothing to report; older gateways never do).
// encoding/json sorts map keys, so applied= is stable for grep and tests.
func policyLine(wf, nodeID, alias, model string, ack streamAck) (string, bool) {
	if len(ack.AppliedParams) == 0 && len(ack.ParamWarnings) == 0 {
		return "", false
	}
	applied, err := json.Marshal(ack.AppliedParams)
	if err != nil {
		applied = []byte(`{}`)
	}
	return fmt.Sprintf("gateway-policy wf=%s node=%s alias=%s model=%s applied=%s warnings=%s",
		wf, nodeID, alias, model, applied, strings.Join(ack.ParamWarnings, "; ")), true
}
```

`stream.go:67`, right after `timing.Model = ack.InvokeID`:

```go
	if line, ok := policyLine(g.wfID, g.nodeID, g.cfg.Alias, ack.InvokeID, ack); ok && g.logger != nil {
		g.logger.Print(line)
	}
```

- [ ] **Step 4: Run the package tests**

Run: `cd LIB && go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -v 2>&1 | tail -20`
Expected: PASS, all tests.

- [ ] **Step 5: Commit**

```bash
cd LIB && git add pkg/workflow/builtin/inferencegateway && git commit -m "feat(inferencegateway): log gateway-policy line from the ack's appliedParams/paramWarnings

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 5: Docs + CHANGELOG, full lib verification

**Files:**
- Modify: `docs/EXTENDING.md:115-120`, `docs/DEPLOYMENT.md:54-59`, `CHANGELOG.md:5-10`

- [ ] **Step 1: EXTENDING.md** — replace the "LLM (gateway)" bullet with:

```markdown
- **LLM (gateway):** `ai/inference-gateway` — model calls through the org inferenceGateway
  NATS service (`<INFERENCE_GATEWAY_BASE_PATH>.invokeStream`). Config: `alias` (required,
  a gateway tier such as `POWERLINE_CLAIM_SEARCH_MODEL`, `^[A-Z][A-Z0-9_]{1,63}$`), `streamTimeoutSeconds`
  (600), `idleTimeoutSeconds` (120), `basePath` override. Nothing else: the gateway alias row
  resolves the model and its `paramPolicy` owns every inference parameter (`maxTokens`,
  `temperature`, …), so a model change is an alias edit, never a deploy. Any other config key
  (`maxTokens`, `modelId`, `lab`, …) fails the workflow at load. The body sent is `alias`,
  `system`, `messages`, `tools` and a forced `toolChoice` when the request carries one. When the
  alias has a policy the node logs one `gateway-policy` line per call (see DEPLOYMENT.md).
  Document blocks are not supported by the gateway and are rejected.
```

- [ ] **Step 2: DEPLOYMENT.md** — after the `llm-timing` paragraph add:

```markdown
### `gateway-policy` log line

When the gateway alias an `ai/inference-gateway` node calls carries a `paramPolicy`, every call
also logs `gateway-policy wf=<id> node=<id> alias=<name> model=<invokeId> applied=<json> warnings=<w1; w2>`:
`applied` is the compiled request's top-level parameters (what the model actually received),
`warnings` every change the policy made (e.g. `maxTokens set`). Absent when no policy ran.
```

- [ ] **Step 3: CHANGELOG.md** — replace the first v1.8.0 bullet with:

```markdown
- **feat: `ai/inference-gateway` LLM node (alias-only).** Model calls go through the org
  inferenceGateway NATS service (`<INFERENCE_GATEWAY_BASE_PATH>.invokeStream`). The node config
  is `alias` (a gateway tier) plus timeouts/basePath; the gateway alias row resolves the model
  and its `paramPolicy` owns every inference parameter. The body carries `alias`, `system`,
  `messages`, `tools` and forced tool choice only — never `maxTokens`, `temperature` or stop
  sequences. Unknown config keys fail the workflow at load. A `gateway-policy` log line reports
  the ack's `appliedParams`/`paramWarnings` when a policy ran. Streams text and tool-use events
  with seq checking, idle (120 s) and stream (600 s) timeouts. Document blocks are rejected.
- **refactor(agent): no hardcoded `maxTokens`.** The agent loop no longer sets `MaxTokens: 4096`;
  each provider decides. `ai/bedrock` payload is unchanged (its node default is 4096).
  `ai/bedrock` is otherwise untouched and still available.
```

- [ ] **Step 4: Full verification**

Run: `cd LIB && gofmt -l pkg/ && go vet -mod=vendor ./pkg/workflow/... && go test -mod=vendor ./... 2>&1 | tail -40`
Expected: `gofmt -l` prints nothing for the touched files; vet clean; all packages `ok`.

- [ ] **Step 5: Commit**

```bash
cd LIB && git add docs/EXTENDING.md docs/DEPLOYMENT.md CHANGELOG.md docs/superpowers && git commit -m "docs: alias-only ai/inference-gateway, gateway-policy line, v1.8.0 changelog; spec+plan

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 6: Whole-branch review (LIB)

- [ ] **Step 1:** Dispatch a fresh reviewer on `git diff Development...HEAD` in LIB with the spec path and the Review Focus list above. Fix findings, re-run Task 5 Step 4, commit fixes as `fix(inferencegateway): review — <what>`.

---

## Part B — APP (`opensearchAiChatApi`)

### Task 7: Gateway workflow drops knobs, per-workflow alias env var, docs

**Files:**
- Modify: `workflows/powerlineSearchGateway.json:13-20`
- Modify: `pkg/workflow/rsassistant_manifest_test.go:179-189`
- Modify: `docs/configuration.md:24-25`, `docs/api-reference.md:14`, `.envTemplate:12-14`
- Vendor: `go mod vendor` (LIB `replace` already in `go.mod:57`)

- [ ] **Step 1: Write the failing manifest assertions**

Replace `rsassistant_manifest_test.go:179-189` (the `cfg` checks) with:

```go
			var cfg map[string]any
			_ = json.Unmarshal(n.Config, &cfg)
			if cfg["alias"] != "${POWERLINE_MODEL_ALIAS:POWERLINE_CLAIM_SEARCH_MODEL}" {
				t.Fatalf("gateway config alias = %v", cfg["alias"])
			}
			if cfg["basePath"] != "${INFERENCE_GATEWAY_BASE_PATH:trx.inferenceGateway}" {
				t.Fatalf("gateway config basePath = %v", cfg["basePath"])
			}
			// The gateway alias row owns model and parameters; none may reach the node,
			// including maxTokens inherited from the powerlineSearch base node.
			for _, gone := range []string{"model", "region", "autoUpdate", "maxTokens", "temperature", "modelId", "lab", "family"} {
				if _, present := cfg[gone]; present {
					t.Fatalf("gateway config must not carry %q", gone)
				}
			}
```

- [ ] **Step 2: Vendor the new lib and run to verify failure**

Run: `cd APP && go mod vendor && go test ./pkg/workflow/ -run TestGatewayVariantSwapsOnlyTheLLMNode -v`
Expected: FAIL on `alias` (still `${INFERENCE_GATEWAY_ALIAS:MAX_MODEL}`), and — if the loader instantiates factories — `unknown field "maxTokens"` from the new lib.

- [ ] **Step 3: Edit the workflow**

`workflows/powerlineSearchGateway.json` `bedrock1.config` becomes exactly:

```json
      "config": {
        "alias": "${POWERLINE_MODEL_ALIAS:POWERLINE_CLAIM_SEARCH_MODEL}",
        "basePath": "${INFERENCE_GATEWAY_BASE_PATH:trx.inferenceGateway}",
        "model": null,
        "region": null,
        "autoUpdate": null,
        "maxTokens": null
      }
```

`"maxTokens": null` is mandatory: `powerlineSearch.json:50` sets `maxTokens: 4096` on the base `bedrock1` and deep-merge would inherit it. Update `description` to: `"Pharmacy claim search agent (PowerLine). Gateway variant: same agent, model called through the org inferenceGateway by alias; model and parameters are owned by the gateway alias row (paramPolicy)."`

- [ ] **Step 4: Docs and env template**

`docs/configuration.md:25` row becomes:

```markdown
| `POWERLINE_MODEL_ALIAS` | no | Gateway tier the `powerlineSearchGateway` workflow asks for. Default: `POWERLINE_CLAIM_SEARCH_MODEL`. Which model the tier resolves to and every inference parameter (`maxTokens`, …) live in the gateway alias row (`paramPolicy`), never in this app. |
```

`docs/configuration.md:24` (`INFERENCE_GATEWAY_BASE_PATH` row): keep, but change `workflow \`powerlineSearchGateway\`` text to mention the alias row: append `Model and parameters are owned by the gateway alias (see \`POWERLINE_MODEL_ALIAS\`).`

`docs/api-reference.md:14` becomes:

```markdown
| `<NATS_BASE_PATH>.powerlineSearchGateway` | streaming | Same request contract as `powerlineSearch`; model called through the org inferenceGateway by alias (`POWERLINE_MODEL_ALIAS`, default `POWERLINE_CLAIM_SEARCH_MODEL`); model + parameters owned by the gateway alias row; comparison only, not wired to the WebApp |
```

`.envTemplate` after line 14 add:

```
# Gateway tier for the powerlineSearchGateway workflow (lib >= v1.8.0). Model and
# inference parameters live in the gateway alias row, never here.
POWERLINE_MODEL_ALIAS=POWERLINE_CLAIM_SEARCH_MODEL
```

- [ ] **Step 5: Run app tests**

Run: `cd APP && go vet ./... && go test ./... 2>&1 | tail -20`
Expected: all `ok`.

- [ ] **Step 6: Commit (workflow + test + docs; NOT go.mod/go.sum while the `replace` is present)**

```bash
cd APP && git add workflows/powerlineSearchGateway.json pkg/workflow/rsassistant_manifest_test.go docs/configuration.md docs/api-reference.md .envTemplate && git commit -m "workflows: powerlineSearchGateway is alias-only (POWERLINE_MODEL_ALIAS); model+params owned by the gateway alias paramPolicy

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Part C — Alias row + live validation (local ecosystem, Dev table)

### Task 8: Provision `POWERLINE_CLAIM_SEARCH_MODEL` and validate end to end

Pre-conditions: local NATS `localhost:4222` up; local chat API + webApp ecosystem per memory `feedback-local-ecosystem-testing`; AWS profile `Development` valid (`aws sts get-caller-identity --profile Development`).

- [ ] **Step 1: Build and run the local gateway from the policy branch**

```bash
cd GW && git status --short && git branch --show-current   # expect feat/alias-param-policy, clean
go build -o /tmp/inferenceGateway ./cmd/inferenceGateway
env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
  AWS_PROFILE=Development AWS_REGION=us-east-1 NATS_URL=nats://localhost:4222 \
  NATS_QUEUE_NAME=x MODEL_ALIAS_TABLE=transactrx_model_alias /tmp/inferenceGateway
```

Expected log: catalog loaded, alias store ready. **This gateway writes the Dev global alias table**: the `setAlias` below IS the Dev provisioning (spec §6).

- [ ] **Step 2: Write the row (pinned to the direct baseline model), read it back**

```bash
# Preferred: the user writes this row in the gateway webapp. CLI fallback:
nats req trx.inferenceGateway.setAlias \
  '{"alias":"POWERLINE_CLAIM_SEARCH_MODEL","modelId":"us.anthropic.claude-opus-5-5","paramPolicy":{"set":{"maxTokens":4096}}}'
nats req trx.inferenceGateway.resolveAlias '{"alias":"POWERLINE_CLAIM_SEARCH_MODEL"}'
```

Before sending: re-read spec §6.1 (parity table) and confirm `bedrock/payload.go` still sends only `max_tokens` for the pinned model; if a new hardcoded knob appeared, add it to the policy body first.

Expected: `setAlias` echoes the alias with `"paramPolicy":{"set":{"maxTokens":4096}}`; `resolveAlias` returns `invokeId` `us.anthropic.claude-opus-5-5`. If `setAlias` returns 4002/4004 stop and report the message verbatim.

- [ ] **Step 3: Restart the chat API on the new lib and ask one question**

Ask the user to restart the chat API (GoLand run config; remember it ignores `compose.local.env`, so `INFERENCE_GATEWAY_BASE_PATH=trx.inferenceGateway` must be in the run config env). Then send one streaming request to `<NATS_BASE_PATH>.powerlineSearchGateway` with the NATS test contract identity headers from memory `reference-aws-dev-access` (scratchpad client from the 2026-09-25 bench, or the webApp pointed at the gateway workflow).

Expected in the chat API log, in this order:
```
ai/inference-gateway wf=powerlineSearchGateway node=bedrock1 ready (alias="POWERLINE_CLAIM_SEARCH_MODEL" subject=trx.inferenceGateway.invokeStream)
gateway-policy wf=powerlineSearchGateway node=bedrock1 alias=POWERLINE_CLAIM_SEARCH_MODEL model=us.anthropic.claude-opus-5-5 applied={"alias":"POWERLINE_CLAIM_SEARCH_MODEL","maxTokens":4096,...} warnings=maxTokens set
llm-timing wf=powerlineSearchGateway node=bedrock1 provider=ai/inference-gateway model=us.anthropic.claude-opus-5-5 ... stop=end_turn err=-
```
and a coherent streamed answer.

- [ ] **Step 4: Negative check (stale knob fails loudly)**

Temporarily add `"maxTokens": 4096` back to `powerlineSearchGateway.json`, restart the chat API. Expected: workflow load error containing `ai/inference-gateway: parse config: json: unknown field "maxTokens"`. Revert the JSON (`git checkout -- workflows/powerlineSearchGateway.json`), restart, confirm the workflow loads again.

- [ ] **Step 5: Record evidence**

Save the three log lines and the `setAlias`/`resolveAlias` replies into the scratchpad and quote them in the completion report. No commit.

---

## Part D — Ship (every step user-gated; do not execute without an explicit go)

### Task 9: Release sequence

- [ ] **Step 1 (GW, user):** merge `feat/alias-param-policy` → Development, deploy East/West, confirm `nats-discover --service trx.inferenceGateway` documents `paramPolicy` on `setAlias`.
- [ ] **Step 2 (Dev row):** already written in Task 8 Step 2 (local gateway wrote the Dev table). Verify against the deployed Dev gateway: `nats req trx.inferenceGateway.resolveAlias '{"alias":"POWERLINE_CLAIM_SEARCH_MODEL"}'` over Dev NATS.
- [ ] **Step 3 (LIB):** push `feature/inference-gateway-llm`, PR → Development, then Dev→Production PR with label `release:minor` → v1.8.0 tag.
- [ ] **Step 4 (APP):**
```bash
cd APP && go get github.com/transactrx/ai-agent-go-service@v1.8.0 && go mod edit -dropreplace github.com/transactrx/ai-agent-go-service && go mod tidy && go mod vendor && go test ./... && git add go.mod go.sum && git commit -m "build: ai-agent-go-service v1.7.3 -> v1.8.0 (alias-only ai/inference-gateway)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```
push, PR → Development, deploy, check CloudWatch for the `ready (alias="POWERLINE_CLAIM_SEARCH_MODEL" …)` and `gateway-policy` lines.
- [ ] **Step 5 (Prod):** gateway promoted → `setAlias POWERLINE_CLAIM_SEARCH_MODEL` on Prod (same body, Prod NATS/account) → app promoted. Delete merged feature branches local + remote (memory `feedback-branch-hygiene`).

---

## Self-review notes

- Spec §4.1 → Task 1; §4.2 → Task 2; §4.3 → Task 3; §4.4 → Task 4; §4.5 → Task 5; §5 → Task 7; §6 + §8 live → Task 8; §9 → Task 9. §7 error table: rows 1–2 tested in Task 1, row 5 in Task 4; rows 3–4 are existing ack-error paths (`parseAck` non-2xx test already in `stream_test.go`).
- `go.mod`/`go.sum` in APP stay uncommitted until Task 9 Step 4 (local `replace` must not ship).
- Task 1 Step 4 makes a temporary edit to `payload.go` that Task 2 finalizes; both tasks end green on their own.
