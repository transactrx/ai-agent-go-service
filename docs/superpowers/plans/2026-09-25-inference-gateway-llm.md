# ai/inference-gateway LLM Provider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `ai/inference-gateway` LLM node that streams model calls through the org inferenceGateway NATS service by alias, add a shared `llm-timing` log line to both providers, wire a parallel gateway workflow in opensearchAiChatApi, and bench direct vs gateway.

**Architecture:** New package `pkg/workflow/builtin/inferencegateway` implements `node.LLMProvider` over `nc.RequestWithContext` (ack) + `SubscribeSync` inbox (events), translating the gateway's `StreamEvent` JSON into `node.LLMEvent`. `ai/bedrock` is untouched except for usage parsing and the timing line. The app pins the direct path to `us.anthropic.claude-opus-5-5` and adds a derived `powerlineSearchGateway` workflow on alias `MAX_MODEL`.

**Tech Stack:** Go 1.27, `github.com/nats-io/nats.go`, embedded `nats-server/v2` for tests, `nats-service` STATUS header convention, workflow JSON `extends` loader.

**Spec:** `docs/superpowers/specs/2026-09-25-inference-gateway-llm-design.md`

## Global Constraints

- Library builds with `-mod=vendor`: `go build -mod=vendor ./...`, `go test -mod=vendor ./...`. Run `go mod vendor` only if `go.mod` changes (it must not in this plan).
- Node type name: `ai/inference-gateway`. Package: `pkg/workflow/builtin/inferencegateway`.
- Base path env: `INFERENCE_GATEWAY_BASE_PATH`, default `example.inferenceGateway`, subject suffix `.invokeStream`.
- Config defaults: `maxTokens` 4096, `streamTimeoutSeconds` 600, `idleTimeoutSeconds` 120, request (ack) timeout 10 s.
- Timing line, exact format: `llm-timing wf=<id> node=<id> provider=<p> model=<m> ttfb_ms=<n> total_ms=<n> in_tok=<n> out_tok=<n> stop=<s> err=<q>` — unknown values print `-`; `err` is `%q` of the message or `-`.
- `ai/bedrock` behaviour must not change beyond the added log line and usage capture; all existing tests keep passing.
- Library target version v1.8.0. Commit per task. **Never push** without the user's explicit approval (push triggers deploy).
- App: direct model `us.anthropic.claude-opus-5-5`, `autoUpdate: false`; derived workflow id `powerlineSearchGateway`, node id `bedrock1`, alias `${INFERENCE_GATEWAY_ALIAS:MAX_MODEL}`, `rsassistant: null`.
- Bench program lives in the session scratchpad only; never committed.

## Review Focus

1. Gateway publishes `done` before the caller's `SubscribeSync` is active — impossible here because the subscription is created before the request is sent; Task 6 test "events published immediately after ack" pins it.
2. A `delta` event carrying neither `text` nor `toolInputDelta` (reasoning deltas are filtered by the gateway, but an empty delta is legal) must produce no `LLMEvent` — Task 6 test "empty delta is ignored".
3. `toolInputDelta` for an index that never had a `toolUseStart` must be ignored, not panic — Task 6 test "orphan tool delta".
4. `temperature` in workflow JSON is float64 while the gateway takes float32 — Task 4 test "temperature converted".
5. The derived gateway workflow must still resolve all base connections to `bedrock1` after the type swap — Task 9 test asserts the merged file has exactly one `ai/inference-gateway` node with id `bedrock1` and a connection on port `ai_languageModel`.

---

## Part A — Library (repo `ai-agent-go-service`, branch `feature/inference-gateway-llm`)

### Task 1: Shared LLM timing line

**Files:**
- Create: `pkg/workflow/node/llmtiming.go`
- Test: `pkg/workflow/node/llmtiming_test.go`

**Interfaces:**
- Produces:
  ```go
  type LLMTiming struct {
      Provider, Model string
      Start, FirstEvent, End time.Time
      HasUsage bool
      InputTokens, OutputTokens int
      Stop string
      Err error
  }
  func (t LLMTiming) String() string
  func LogLLMTiming(logger *log.Logger, wfID, nodeID string, t LLMTiming) // nil logger = no-op
  ```

- [ ] **Step 1: Write the failing test**

```go
package node

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

func TestLLMTimingString(t *testing.T) {
	start := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	full := LLMTiming{
		Provider: "ai/inference-gateway", Model: "us.anthropic.claude-opus-5-5",
		Start: start, FirstEvent: start.Add(350 * time.Millisecond), End: start.Add(2 * time.Second),
		HasUsage: true, InputTokens: 1200, OutputTokens: 80, Stop: "end_turn",
	}
	want := "provider=ai/inference-gateway model=us.anthropic.claude-opus-5-5 ttfb_ms=350 total_ms=2000 in_tok=1200 out_tok=80 stop=end_turn err=-"
	if got := full.String(); got != want {
		t.Fatalf("String() =\n%s\nwant\n%s", got, want)
	}
	failed := LLMTiming{Provider: "ai/bedrock", Start: start, End: start.Add(time.Second), Err: errors.New("boom \"x\"")}
	want = `provider=ai/bedrock model=- ttfb_ms=- total_ms=1000 in_tok=- out_tok=- stop=- err="boom \"x\""`
	if got := failed.String(); got != want {
		t.Fatalf("String() =\n%s\nwant\n%s", got, want)
	}
}

func TestLogLLMTiming(t *testing.T) {
	var buf bytes.Buffer
	lg := log.New(&buf, "", 0)
	LogLLMTiming(lg, "wf1", "n1", LLMTiming{Provider: "p", Start: time.Now(), End: time.Now()})
	if !strings.HasPrefix(buf.String(), "llm-timing wf=wf1 node=n1 provider=p ") {
		t.Fatalf("unexpected line: %q", buf.String())
	}
	LogLLMTiming(nil, "wf1", "n1", LLMTiming{}) // must not panic
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ai-agent-go-service && go test -mod=vendor ./pkg/workflow/node/ -run 'TestLLMTiming|TestLogLLMTiming' -v`
Expected: FAIL, `undefined: LLMTiming`

- [ ] **Step 3: Write minimal implementation**

```go
package node

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

// LLMTiming is the per-call measurement every LLMProvider logs at the end of
// Stream (spec 2026-09-25-inference-gateway-llm §3.4). It exists so the direct
// Bedrock path and the inferenceGateway path can be compared line for line.
type LLMTiming struct {
	Provider string
	Model    string // resolved id actually invoked; "" when unknown

	Start      time.Time
	FirstEvent time.Time // zero when nothing was received
	End        time.Time

	HasUsage     bool
	InputTokens  int
	OutputTokens int

	Stop string
	Err  error
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// String renders the key=value tail of the log line (everything after node=).
func (t LLMTiming) String() string {
	ttfb := "-"
	if !t.FirstEvent.IsZero() && !t.Start.IsZero() {
		ttfb = strconv.FormatInt(t.FirstEvent.Sub(t.Start).Milliseconds(), 10)
	}
	total := "-"
	if !t.End.IsZero() && !t.Start.IsZero() {
		total = strconv.FormatInt(t.End.Sub(t.Start).Milliseconds(), 10)
	}
	in, out := "-", "-"
	if t.HasUsage {
		in = strconv.Itoa(t.InputTokens)
		out = strconv.Itoa(t.OutputTokens)
	}
	errs := "-"
	if t.Err != nil {
		errs = strconv.Quote(t.Err.Error())
	}
	return fmt.Sprintf("provider=%s model=%s ttfb_ms=%s total_ms=%s in_tok=%s out_tok=%s stop=%s err=%s",
		dash(t.Provider), dash(t.Model), ttfb, total, in, out, dash(t.Stop), errs)
}

// LogLLMTiming writes the llm-timing line. A nil logger is a no-op so
// providers can call it unconditionally.
func LogLLMTiming(logger *log.Logger, wfID, nodeID string, t LLMTiming) {
	if logger == nil {
		return
	}
	logger.Printf("llm-timing wf=%s node=%s %s", wfID, nodeID, t.String())
}
```

- [ ] **Step 4: Run the tests**

Run: `go test -mod=vendor ./pkg/workflow/node/ -v -run 'TestLLMTiming|TestLogLLMTiming'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/node/llmtiming.go pkg/workflow/node/llmtiming_test.go
git commit -m "feat(node): shared llm-timing measurement + log line

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: ai/bedrock emits the timing line with usage

**Files:**
- Modify: `pkg/workflow/builtin/bedrock/stream.go` (`Stream`, `handleAnthropicChunk`)
- Test: `pkg/workflow/builtin/bedrock/timing_test.go`

**Interfaces:**
- Consumes: `node.LLMTiming`, `node.LogLLMTiming` (Task 1).
- Produces: `type anthropicStats struct{ hasUsage bool; inputTokens, outputTokens int; stop string }`; `handleAnthropicChunkStats(ctx, raw, accum, out, st *anthropicStats) error`. `handleAnthropicChunk` keeps its signature and delegates with `nil`.

- [ ] **Step 1: Write the failing test**

```go
package bedrock

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// message_start carries input tokens, message_delta carries output tokens
// and the stop reason; both must land in anthropicStats.
func TestHandleAnthropicChunkStats(t *testing.T) {
	chunks := []string{
		`{"type":"message_start","message":{"id":"m1","usage":{"input_tokens":1200,"output_tokens":1}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":80}}`,
	}
	out := make(chan node.LLMEvent, 8)
	st := &anthropicStats{}
	for _, c := range chunks {
		if err := handleAnthropicChunkStats(context.Background(), []byte(c), map[int]*node.LLMToolUse{}, out, st); err != nil {
			t.Fatal(err)
		}
	}
	if !st.hasUsage || st.inputTokens != 1200 || st.outputTokens != 80 || st.stop != "end_turn" {
		t.Fatalf("stats = %+v", *st)
	}
	// nil stats must still be accepted (existing callers).
	if err := handleAnthropicChunk(context.Background(), []byte(chunks[0]), map[int]*node.LLMToolUse{}, out); err != nil {
		t.Fatal(err)
	}
}

// A failed invoke still logs one llm-timing line naming the model and error.
func TestStreamLogsTimingOnInvokeError(t *testing.T) {
	var buf bytes.Buffer
	fake := &fakeInvoker{errs: []error{validationErr()}}
	b := &bedrockLLM{
		cfg:    Config{Model: "x", MaxTokens: 16, AnthropicVersion: defaultAnthropicVersion},
		model:  "us.anthropic.claude-opus-5-5",
		client: fake,
		logger: log.New(&buf, "", 0),
		wfID:   "wf1", nodeID: "n1",
	}
	out := make(chan node.LLMEvent, 8)
	if err := b.Stream(context.Background(), probeReq(), out); err == nil {
		t.Fatal("expected error")
	}
	line := ""
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(l, "llm-timing ") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no llm-timing line in:\n%s", buf.String())
	}
	for _, want := range []string{"wf=wf1", "node=n1", "provider=ai/bedrock", "model=us.anthropic.claude-opus-5-5", "ttfb_ms=-", "in_tok=-", `err="`} {
		if !strings.Contains(line, want) {
			t.Fatalf("line %q lacks %q", line, want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -mod=vendor ./pkg/workflow/builtin/bedrock/ -run 'TestHandleAnthropicChunkStats|TestStreamLogsTimingOnInvokeError' -v`
Expected: FAIL, `undefined: anthropicStats`

- [ ] **Step 3: Implement**

In `stream.go`:

1. Add near the top:

```go
// anthropicStats collects what the timing line needs from the Bedrock
// Anthropic chunk stream: usage (message_start input, message_delta output)
// and the final stop reason.
type anthropicStats struct {
	hasUsage     bool
	inputTokens  int
	outputTokens int
	stop         string
}
```

2. Replace the `Stream` body head and tail so timing is recorded. Full new `Stream`:

```go
func (b *bedrockLLM) Stream(ctx context.Context, req node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	model := b.currentModel()
	timing := node.LLMTiming{Provider: "ai/bedrock", Model: model, Start: time.Now()}
	stats := &anthropicStats{}
	defer func() {
		timing.End = time.Now()
		timing.HasUsage, timing.InputTokens, timing.OutputTokens = stats.hasUsage, stats.inputTokens, stats.outputTokens
		timing.Stop = stats.stop
		node.LogLLMTiming(b.logger, b.wfID, b.nodeID, timing)
	}()

	makeInput := func(m string) (*bedrockruntime.InvokeModelWithResponseStreamInput, error) {
		payload, err := buildAnthropicPayload(req, b.cfg, m)
		if err != nil {
			return nil, err
		}
		return &bedrockruntime.InvokeModelWithResponseStreamInput{
			ModelId:     aws.String(m),
			ContentType: aws.String("application/json"),
			Accept:      aws.String("application/json"),
			Body:        payload,
		}, nil
	}
	in, err := makeInput(model)
	if err != nil {
		timing.Err = err
		return err
	}
	resp, err := b.client.InvokeModelWithResponseStream(ctx, in)
	if err != nil && isModelUnavailable(err) {
		if lkg := b.lastKnownGoodModel(); lkg != "" && lkg != model {
			if b.logger != nil {
				b.logger.Printf("ai/bedrock wf=%s node=%s model %s failed (%v), retrying with last-known-good %s", b.wfID, b.nodeID, model, err, lkg)
			}
			lkgIn, buildErr := makeInput(lkg)
			if buildErr != nil {
				if b.logger != nil {
					b.logger.Printf("ai/bedrock wf=%s node=%s last-known-good %s payload build failed: %v (keeping original error)", b.wfID, b.nodeID, lkg, buildErr)
				}
			} else {
				model = lkg
				timing.Model = model
				resp, err = b.client.InvokeModelWithResponseStream(ctx, lkgIn)
			}
		}
	}
	if err != nil {
		timing.Err = err
		if b.logger != nil {
			b.logger.Printf("ai/bedrock invoke failed: wf=%s node=%s model=%s region=%s err=%v", b.wfID, b.nodeID, model, b.cfg.Region, err)
		}
		emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
		return err
	}

	stream := resp.GetStream()
	defer stream.Close()

	accum := map[int]*node.LLMToolUse{}

	for evt := range stream.Events() {
		if ctx.Err() != nil {
			timing.Err = ctx.Err()
			return ctx.Err()
		}
		if timing.FirstEvent.IsZero() {
			timing.FirstEvent = time.Now()
		}
		switch e := evt.(type) {
		case *types.ResponseStreamMemberChunk:
			if err := handleAnthropicChunkStats(ctx, e.Value.Bytes, accum, out, stats); err != nil {
				timing.Err = err
				if b.logger != nil {
					b.logger.Printf("ai/bedrock chunk decode failed: wf=%s node=%s model=%s err=%v", b.wfID, b.nodeID, model, err)
				}
				emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
				return err
			}
		default:
			err := fmt.Errorf("bedrock stream: %T", e)
			timing.Err = err
			if b.logger != nil {
				b.logger.Printf("ai/bedrock unexpected stream event: wf=%s node=%s model=%s type=%T", b.wfID, b.nodeID, model, e)
			}
			emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: fmt.Errorf("ai/bedrock: stream error: %T", e)})
			return err
		}
	}
	if err := stream.Err(); err != nil {
		timing.Err = err
		if b.logger != nil {
			b.logger.Printf("ai/bedrock stream terminated with error: wf=%s node=%s model=%s err=%v", b.wfID, b.nodeID, model, err)
		}
		emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
		return err
	}
	return nil
}
```

Add `"time"` to the imports.

3. Rename the existing `handleAnthropicChunk` to `handleAnthropicChunkStats` with the extra parameter `st *anthropicStats`, and add the wrapper:

```go
// handleAnthropicChunk keeps the original signature for callers that do not
// need stats (auto-update probe, tests).
func handleAnthropicChunk(ctx context.Context, raw []byte, accum map[int]*node.LLMToolUse, out chan<- node.LLMEvent) error {
	return handleAnthropicChunkStats(ctx, raw, accum, out, nil)
}
```

Inside `handleAnthropicChunkStats`, add two cases:

```go
	case "message_start":
		if st != nil {
			var v struct {
				Message struct {
					Usage struct {
						InputTokens int `json:"input_tokens"`
					} `json:"usage"`
				} `json:"message"`
			}
			if err := json.Unmarshal(raw, &v); err != nil {
				return err
			}
			st.hasUsage = true
			st.inputTokens = v.Message.Usage.InputTokens
		}
```

and extend the `message_delta` case: change `messageDelta` to

```go
	type messageDelta struct {
		Type  string `json:"type"`
		Delta struct {
			StopReason string `json:"stop_reason"`
		} `json:"delta"`
		Usage struct {
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
```

and after decoding:

```go
		if st != nil {
			if v.Delta.StopReason != "" {
				st.stop = v.Delta.StopReason
			}
			if v.Usage.OutputTokens > 0 {
				st.hasUsage = true
				st.outputTokens = v.Usage.OutputTokens
			}
		}
```

- [ ] **Step 4: Run the whole bedrock package**

Run: `go test -mod=vendor ./pkg/workflow/builtin/bedrock/ -v -run 'TestHandleAnthropicChunkStats|TestStreamLogsTimingOnInvokeError|TestStream|TestProbeDecodeRoundTrip'` then `go test -mod=vendor ./pkg/workflow/builtin/bedrock/`
Expected: PASS, no existing test changed.

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/bedrock/stream.go pkg/workflow/builtin/bedrock/timing_test.go
git commit -m "feat(bedrock): llm-timing line with token usage per call

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: inferencegateway config, factory, Spec

**Files:**
- Create: `pkg/workflow/builtin/inferencegateway/factory.go`
- Test: `pkg/workflow/builtin/inferencegateway/factory_test.go`

**Interfaces:**
- Produces:
  ```go
  const nodeType = "ai/inference-gateway"
  type Config struct {
      Alias, ModelID, Lab, Family string
      MaxTokens int; Temperature *float64
      StreamTimeoutSeconds, IdleTimeoutSeconds int
      BasePath string
  }
  func (c Config) validate() error
  var Factory node.Factory
  type gatewayLLM struct { cfg Config; nc *nats.Conn; subject string; streamTimeout, idleTimeout time.Duration; logger *log.Logger; nodeID, wfID string }
  func newLLM(cfg Config) *gatewayLLM   // applies defaults
  ```

- [ ] **Step 1: Write the failing test**

```go
package inferencegateway

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestFactoryConfigValidation(t *testing.T) {
	cases := []struct {
		name, raw, wantErr string
	}{
		{"alias ok", `{"alias":"MAX_MODEL"}`, ""},
		{"modelId ok", `{"modelId":"us.anthropic.claude-opus-5-5"}`, ""},
		{"lab+family ok", `{"lab":"anthropic","family":"claude-opus"}`, ""},
		{"none", `{}`, "exactly one of alias, modelId, lab+family"},
		{"two selectors", `{"alias":"A","modelId":"m"}`, "exactly one of alias, modelId, lab+family"},
		{"lab without family", `{"lab":"anthropic"}`, "lab and family must be set together"},
		{"family without lab", `{"family":"claude-opus"}`, "lab and family must be set together"},
		{"bad json", `{"alias":1}`, "parse config"},
		{"negative maxTokens", `{"alias":"A","maxTokens":-1}`, "maxTokens must be positive"},
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

func TestNewLLMDefaults(t *testing.T) {
	g := newLLM(Config{Alias: "A"})
	if g.cfg.MaxTokens != 4096 || g.streamTimeout != 600*time.Second || g.idleTimeout != 120*time.Second {
		t.Fatalf("defaults = maxTokens %d stream %s idle %s", g.cfg.MaxTokens, g.streamTimeout, g.idleTimeout)
	}
	g = newLLM(Config{Alias: "A", MaxTokens: 10, StreamTimeoutSeconds: 5, IdleTimeoutSeconds: 2})
	if g.cfg.MaxTokens != 10 || g.streamTimeout != 5*time.Second || g.idleTimeout != 2*time.Second {
		t.Fatalf("overrides not applied: %+v", g)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -v`
Expected: FAIL to build, `undefined: Factory`

- [ ] **Step 3: Implement**

```go
// Package inferencegateway implements the ai/inference-gateway LLMProvider:
// model calls go through the org inferenceGateway NATS service
// (<base>.invokeStream) and the model is chosen by alias (tier), model id, or
// lab+family. Spec: docs/superpowers/specs/2026-09-25-inference-gateway-llm-design.md
package inferencegateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const (
	nodeType             = "ai/inference-gateway"
	defaultMaxTokens     = 4096
	defaultStreamTimeout = 600 * time.Second
	defaultIdleTimeout   = 120 * time.Second
)

// Config is the per-instance node config. Exactly one selector: alias,
// modelId, or lab+family. The gateway resolves it on every call.
type Config struct {
	Alias   string `json:"alias,omitempty"`
	ModelID string `json:"modelId,omitempty"`
	Lab     string `json:"lab,omitempty"`
	Family  string `json:"family,omitempty"`

	MaxTokens   int      `json:"maxTokens,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`

	StreamTimeoutSeconds int `json:"streamTimeoutSeconds,omitempty"`
	IdleTimeoutSeconds   int `json:"idleTimeoutSeconds,omitempty"`

	// BasePath overrides INFERENCE_GATEWAY_BASE_PATH for this node.
	BasePath string `json:"basePath,omitempty"`
}

func (c Config) validate() error {
	if (c.Lab == "") != (c.Family == "") {
		return errors.New("ai/inference-gateway: lab and family must be set together")
	}
	selectors := 0
	if c.Alias != "" {
		selectors++
	}
	if c.ModelID != "" {
		selectors++
	}
	if c.Lab != "" {
		selectors++
	}
	if selectors != 1 {
		return errors.New("ai/inference-gateway: exactly one of alias, modelId, lab+family is required")
	}
	if c.MaxTokens < 0 {
		return errors.New("ai/inference-gateway: maxTokens must be positive")
	}
	if c.StreamTimeoutSeconds < 0 || c.IdleTimeoutSeconds < 0 {
		return errors.New("ai/inference-gateway: timeouts must be positive")
	}
	return nil
}

// Factory builds an ai/inference-gateway node from rawConfig.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("ai/inference-gateway: parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newLLM(cfg), nil
})

// gatewayLLM is the node instance.
type gatewayLLM struct {
	cfg Config

	nc            *nats.Conn
	subject       string // <base>.invokeStream, fixed at Init
	streamTimeout time.Duration
	idleTimeout   time.Duration

	logger *log.Logger
	nodeID string
	wfID   string
}

// newLLM applies config defaults.
func newLLM(cfg Config) *gatewayLLM {
	if cfg.MaxTokens == 0 {
		cfg.MaxTokens = defaultMaxTokens
	}
	g := &gatewayLLM{cfg: cfg, streamTimeout: defaultStreamTimeout, idleTimeout: defaultIdleTimeout}
	if cfg.StreamTimeoutSeconds > 0 {
		g.streamTimeout = time.Duration(cfg.StreamTimeoutSeconds) * time.Second
	}
	if cfg.IdleTimeoutSeconds > 0 {
		g.idleTimeout = time.Duration(cfg.IdleTimeoutSeconds) * time.Second
	}
	return g
}

// Spec returns immutable metadata; same output port as ai/bedrock so the
// agent wiring is identical.
func (g *gatewayLLM) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: nodeType,
		Role: node.RoleLLM,
		OutputPorts: []node.PortSpec{
			{Name: node.PortAILanguageModel, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

// Close has nothing to release: the NATS connection is shared and owned by
// the host.
func (g *gatewayLLM) Close(_ context.Context) error { return nil }
```

`Init` and `Stream` arrive in Tasks 6 and 7; until then add temporary stubs so the package compiles and `LLMProvider` is satisfied:

```go
func (g *gatewayLLM) Init(_ context.Context, _ node.NodeEnv) error { return nil }

func (g *gatewayLLM) Stream(_ context.Context, _ node.LLMRequest, out chan<- node.LLMEvent) error {
	close(out)
	return errors.New("ai/inference-gateway: not implemented")
}
```

- [ ] **Step 4: Run the tests**

Run: `go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/inferencegateway/
git commit -m "feat(inferencegateway): node config, factory and spec

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Request translation (LLMRequest → gateway invokeStream body)

**Files:**
- Create: `pkg/workflow/builtin/inferencegateway/payload.go`
- Test: `pkg/workflow/builtin/inferencegateway/payload_test.go`

**Interfaces:**
- Consumes: `Config` (Task 3), `node.LLMRequest`, `node.ContentBlock`, `node.ToolSpec`.
- Produces:
  ```go
  type invokeStreamRequest struct { Alias, Lab, Family, ModelID, System string; Messages []wireMessage; MaxTokens *int32; Temperature *float32; StopSequences []string; Tools []wireTool; ToolChoice, ToolChoiceName, StreamSubject string }
  func buildRequest(req node.LLMRequest, cfg Config) (*invokeStreamRequest, error)
  func imageFormat(mediaType string) (string, error)
  ```

- [ ] **Step 1: Write the failing test**

```go
package inferencegateway

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestBuildRequestFullShape(t *testing.T) {
	temp := 0.2
	req := node.LLMRequest{
		System:    "sys",
		MaxTokens: 512,
		Temperature: &temp,
		Stop:      []string{"END"},
		Tools:     []node.ToolSpec{{Name: "search", Description: "d", InputSchema: json.RawMessage(`{"type":"object"}`)}},
		ToolChoiceName: "search",
		Messages: []node.Message{
			{Role: node.UserMsg, Content: []node.ContentBlock{
				{Type: node.BlockText, Text: "hello"},
				{Type: node.BlockImage, MediaType: "image/png", Data: []byte{1, 2, 3}},
			}},
			{Role: node.AssistantMsg, Content: []node.ContentBlock{
				{Type: node.BlockToolUse, ToolUseID: "tu1", ToolName: "search"},
			}},
			{Role: node.UserMsg, Content: []node.ContentBlock{
				{Type: node.BlockToolResult, ToolUseID: "tu1", ToolResult: json.RawMessage(`{"hits":3}`), IsError: true},
				{Type: node.BlockToolResult, ToolUseID: "tu2"},
			}},
		},
	}
	got, err := buildRequest(req, Config{Alias: "MAX_MODEL", MaxTokens: 4096})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	want := `{"alias":"MAX_MODEL","system":"sys","messages":[` +
		`{"role":"user","content":[{"text":"hello"},{"image":{"format":"png","base64":"AQID"}}]},` +
		`{"role":"assistant","content":[{"toolUse":{"toolUseId":"tu1","name":"search","input":{}}}]},` +
		`{"role":"user","content":[{"toolResult":{"toolUseId":"tu1","content":[{"text":"{\"hits\":3}"}],"status":"error"}},` +
		`{"toolResult":{"toolUseId":"tu2","content":[{"text":"\"\""}]}}]}],` +
		`"maxTokens":512,"temperature":0.2,"stopSequences":["END"],` +
		`"tools":[{"name":"search","description":"d","inputSchema":{"type":"object"}}],` +
		`"toolChoice":"tool","toolChoiceName":"search","streamSubject":""}`
	if string(b) != want {
		t.Fatalf("body =\n%s\nwant\n%s", b, want)
	}
}

func TestBuildRequestSelectorsAndDefaults(t *testing.T) {
	min := node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "x"}}}}}
	got, err := buildRequest(min, Config{Lab: "anthropic", Family: "claude-opus", MaxTokens: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if got.Lab != "anthropic" || got.Family != "claude-opus" || got.Alias != "" || got.ModelID != "" {
		t.Fatalf("selectors = %+v", got)
	}
	if got.MaxTokens == nil || *got.MaxTokens != 4096 {
		t.Fatalf("config maxTokens must apply when request has none: %v", got.MaxTokens)
	}
	if got.Temperature != nil || got.ToolChoice != "" || got.Tools != nil {
		t.Fatalf("optional fields must be omitted: %+v", got)
	}
	got, _ = buildRequest(min, Config{ModelID: "us.x", MaxTokens: 4096})
	if got.ModelID != "us.x" {
		t.Fatalf("modelId selector: %+v", got)
	}
}

// temperature converted: workflow JSON float64 → gateway float32.
func TestBuildRequestTemperatureConverted(t *testing.T) {
	temp := 0.7
	min := node.LLMRequest{Temperature: &temp, Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "x"}}}}}
	got, _ := buildRequest(min, Config{Alias: "A", MaxTokens: 1})
	if got.Temperature == nil || *got.Temperature != float32(0.7) {
		t.Fatalf("temperature = %v", got.Temperature)
	}
}

func TestBuildRequestRejectsUnsupported(t *testing.T) {
	doc := node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockDocument, MediaType: "application/pdf", Data: []byte("x")}}}}}
	if _, err := buildRequest(doc, Config{Alias: "A"}); err == nil || !strings.Contains(err.Error(), "document blocks are not supported") {
		t.Fatalf("document: err = %v", err)
	}
	bmp := node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockImage, MediaType: "image/bmp", Data: []byte("x")}}}}}
	if _, err := buildRequest(bmp, Config{Alias: "A"}); err == nil || !strings.Contains(err.Error(), "image/bmp") {
		t.Fatalf("bmp: err = %v", err)
	}
	unknown := node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: "weird"}}}}}
	if _, err := buildRequest(unknown, Config{Alias: "A"}); err == nil || !strings.Contains(err.Error(), "unknown block type") {
		t.Fatalf("unknown: err = %v", err)
	}
}

func TestImageFormat(t *testing.T) {
	for mt, want := range map[string]string{"image/png": "png", "image/jpeg": "jpeg", "image/jpg": "jpeg", "image/gif": "gif", "image/webp": "webp"} {
		if got, err := imageFormat(mt); err != nil || got != want {
			t.Fatalf("%s → %q, %v", mt, got, err)
		}
	}
	if _, err := imageFormat("image/tiff"); err == nil {
		t.Fatal("tiff must error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -run 'TestBuildRequest|TestImageFormat' -v`
Expected: FAIL to build, `undefined: buildRequest`

- [ ] **Step 3: Implement**

```go
package inferencegateway

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Wire types mirror the gateway's models.InvokeStreamRequest (repo
// transactrx/inferenceGateway, pkg/models). Kept local so this library does
// not import the gateway module.

type wireImage struct {
	Format string `json:"format"`
	Base64 string `json:"base64"`
}

type wireToolUse struct {
	ToolUseID string          `json:"toolUseId"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
}

type wireToolResultContent struct {
	Text string `json:"text,omitempty"`
}

type wireToolResult struct {
	ToolUseID string                  `json:"toolUseId"`
	Content   []wireToolResultContent `json:"content"`
	Status    string                  `json:"status,omitempty"`
}

type wireBlock struct {
	Text       string          `json:"text,omitempty"`
	Image      *wireImage      `json:"image,omitempty"`
	ToolUse    *wireToolUse    `json:"toolUse,omitempty"`
	ToolResult *wireToolResult `json:"toolResult,omitempty"`
}

type wireMessage struct {
	Role    string      `json:"role"`
	Content []wireBlock `json:"content"`
}

type wireTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// invokeStreamRequest is the body sent to <base>.invokeStream.
type invokeStreamRequest struct {
	Alias   string `json:"alias,omitempty"`
	Lab     string `json:"lab,omitempty"`
	Family  string `json:"family,omitempty"`
	ModelID string `json:"modelId,omitempty"`

	System   string        `json:"system,omitempty"`
	Messages []wireMessage `json:"messages"`

	MaxTokens     *int32   `json:"maxTokens,omitempty"`
	Temperature   *float32 `json:"temperature,omitempty"`
	StopSequences []string `json:"stopSequences,omitempty"`

	Tools          []wireTool `json:"tools,omitempty"`
	ToolChoice     string     `json:"toolChoice,omitempty"`
	ToolChoiceName string     `json:"toolChoiceName,omitempty"`

	StreamSubject string `json:"streamSubject"`
}

// imageFormat maps an IANA media type to the gateway's image format enum.
func imageFormat(mediaType string) (string, error) {
	switch mediaType {
	case "image/png":
		return "png", nil
	case "image/jpeg", "image/jpg":
		return "jpeg", nil
	case "image/gif":
		return "gif", nil
	case "image/webp":
		return "webp", nil
	}
	return "", fmt.Errorf("ai/inference-gateway: unsupported image media type %q", mediaType)
}

// buildRequest converts the provider-agnostic request into the gateway body.
// Every parameter the agent sends today is forwarded (spec §3.2); only the
// stream subject is left for Stream to fill.
func buildRequest(req node.LLMRequest, cfg Config) (*invokeStreamRequest, error) {
	out := &invokeStreamRequest{
		Alias:         cfg.Alias,
		Lab:           cfg.Lab,
		Family:        cfg.Family,
		ModelID:       cfg.ModelID,
		System:        req.System,
		StopSequences: req.Stop,
	}
	maxTok := cfg.MaxTokens
	if req.MaxTokens > 0 {
		maxTok = req.MaxTokens
	}
	if maxTok > 0 {
		v := int32(maxTok)
		out.MaxTokens = &v
	}
	if req.Temperature != nil {
		v := float32(*req.Temperature)
		out.Temperature = &v
	}
	for _, m := range req.Messages {
		wm := wireMessage{Role: string(m.Role), Content: make([]wireBlock, 0, len(m.Content))}
		for _, b := range m.Content {
			switch b.Type {
			case node.BlockText:
				wm.Content = append(wm.Content, wireBlock{Text: b.Text})
			case node.BlockToolUse:
				input := b.ToolInput
				if len(input) == 0 {
					input = json.RawMessage(`{}`)
				}
				wm.Content = append(wm.Content, wireBlock{ToolUse: &wireToolUse{ToolUseID: b.ToolUseID, Name: b.ToolName, Input: input}})
			case node.BlockToolResult:
				// Same stringify rule as ai/bedrock: the raw JSON rides inside
				// one text content so object payloads round-trip safely.
				text := string(b.ToolResult)
				if text == "" {
					text = `""`
				}
				tr := &wireToolResult{ToolUseID: b.ToolUseID, Content: []wireToolResultContent{{Text: text}}}
				if b.IsError {
					tr.Status = "error"
				}
				wm.Content = append(wm.Content, wireBlock{ToolResult: tr})
			case node.BlockImage:
				format, err := imageFormat(b.MediaType)
				if err != nil {
					return nil, err
				}
				wm.Content = append(wm.Content, wireBlock{Image: &wireImage{Format: format, Base64: base64.StdEncoding.EncodeToString(b.Data)}})
			case node.BlockDocument:
				return nil, fmt.Errorf("ai/inference-gateway: document blocks are not supported by the gateway (file %q)", b.Filename)
			default:
				return nil, fmt.Errorf("ai/inference-gateway: unknown block type %q", b.Type)
			}
		}
		out.Messages = append(out.Messages, wm)
	}
	for _, ts := range req.Tools {
		out.Tools = append(out.Tools, wireTool{Name: ts.Name, Description: ts.Description, InputSchema: ts.InputSchema})
	}
	if req.ToolChoiceName != "" {
		out.ToolChoice = "tool"
		out.ToolChoiceName = req.ToolChoiceName
	}
	return out, nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/inferencegateway/payload.go pkg/workflow/builtin/inferencegateway/payload_test.go
git commit -m "feat(inferencegateway): translate LLMRequest to the gateway invokeStream body

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Subject resolution and ack parsing

**Files:**
- Create: `pkg/workflow/builtin/inferencegateway/client.go`
- Test: `pkg/workflow/builtin/inferencegateway/client_test.go`

**Interfaces:**
- Produces:
  ```go
  const basePathEnv = "INFERENCE_GATEWAY_BASE_PATH"; defaultBasePath = "example.inferenceGateway"; invokeStreamSuffix = ".invokeStream"; requestTimeout = 10 * time.Second
  func invokeSubject(override string) string
  type streamAck struct { Accepted bool; ModelID, InvokeID, ErrorMessage string }
  func parseAck(status string, body []byte) (streamAck, error)
  type streamEvent struct { Seq int; Type string; ContentIndex int; Text, ToolUseID, ToolName, ToolInputDelta, StopReason string; Usage *streamUsage; Error string }
  type streamUsage struct { InputTokens, OutputTokens int32 }
  ```

- [ ] **Step 1: Write the failing test**

```go
package inferencegateway

import (
	"strings"
	"testing"
)

func TestInvokeSubject(t *testing.T) {
	t.Setenv(basePathEnv, "")
	if got := invokeSubject(""); got != "example.inferenceGateway.invokeStream" {
		t.Fatalf("default = %q", got)
	}
	t.Setenv(basePathEnv, " trx.inferenceGateway ")
	if got := invokeSubject(""); got != "trx.inferenceGateway.invokeStream" {
		t.Fatalf("env = %q", got)
	}
	if got := invokeSubject(" trx.other "); got != "trx.other.invokeStream" {
		t.Fatalf("override = %q", got)
	}
}

func TestParseAck(t *testing.T) {
	ok := `{"accepted":true,"modelId":"anthropic.claude-opus-5-5","invokeId":"us.anthropic.claude-opus-5-5","streamSubject":"_INBOX.x"}`
	cases := []struct {
		name, status, body, wantInvoke, wantErr string
	}{
		{"success", "200", ok, "us.anthropic.claude-opus-5-5", ""},
		{"no status header", "", ok, "us.anthropic.claude-opus-5-5", ""},
		{"error status with message", "400", `{"status":400,"errorMessage":"alias NOPE not found"}`, "", "gateway status 400: alias NOPE not found"},
		{"error status raw body", "500", `Server Error`, "", "gateway status 500: Server Error"},
		{"non numeric status", "abc", ok, "", "gateway status abc"},
		{"not accepted", "200", `{"accepted":false}`, "", "gateway did not accept"},
		{"not json", "200", `nope`, "", "not JSON"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ack, err := parseAck(c.status, []byte(c.body))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, c.wantErr)
				}
				return
			}
			if err != nil || ack.InvokeID != c.wantInvoke {
				t.Fatalf("ack = %+v, err = %v", ack, err)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -run 'TestInvokeSubject|TestParseAck' -v`
Expected: FAIL to build, `undefined: invokeSubject`

- [ ] **Step 3: Implement**

```go
package inferencegateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	basePathEnv        = "INFERENCE_GATEWAY_BASE_PATH"
	defaultBasePath    = "example.inferenceGateway"
	invokeStreamSuffix = ".invokeStream"
	// requestTimeout bounds the ack round trip only; the stream has its own
	// timeouts (Config).
	requestTimeout = 10 * time.Second
)

// invokeSubject resolves <base>.invokeStream: node config override, else the
// env var, else the repo-convention placeholder (cf. bedrock.gatewaySubject).
func invokeSubject(override string) string {
	base := strings.TrimSpace(override)
	if base == "" {
		base = strings.TrimSpace(os.Getenv(basePathEnv))
	}
	if base == "" {
		base = defaultBasePath
	}
	return base + invokeStreamSuffix
}

// streamAck is the invokeStream reply (gateway models.InvokeStreamAck) plus
// the NatsServiceError field a failure carries.
type streamAck struct {
	Accepted     bool   `json:"accepted"`
	ModelID      string `json:"modelId"`
	InvokeID     string `json:"invokeId"`
	ErrorMessage string `json:"errorMessage"`
}

// parseAck applies the nats-service STATUS rule: empty or strict numeric
// 200-299 is success; anything else is an error carrying the gateway's
// errorMessage (or the truncated raw body).
func parseAck(status string, body []byte) (streamAck, error) {
	var a streamAck
	jsonErr := json.Unmarshal(body, &a)
	if status != "" {
		if n, err := strconv.Atoi(status); err != nil || n < 200 || n > 299 {
			return streamAck{}, fmt.Errorf("gateway status %s: %s", status, errText(a.ErrorMessage, body))
		}
	}
	if jsonErr != nil {
		return streamAck{}, fmt.Errorf("gateway ack not JSON: %w", jsonErr)
	}
	if !a.Accepted {
		return streamAck{}, errors.New("gateway did not accept the stream request: " + errText(a.ErrorMessage, body))
	}
	return a, nil
}

func errText(msg string, body []byte) string {
	if msg != "" {
		return msg
	}
	const max = 200
	if len(body) > max {
		return string(body[:max]) + "..."
	}
	return string(body)
}

// streamUsage mirrors the gateway's models.Usage.
type streamUsage struct {
	InputTokens  int32 `json:"inputTokens"`
	OutputTokens int32 `json:"outputTokens"`
}

// streamEvent mirrors the gateway's models.StreamEvent published on the
// stream subject.
type streamEvent struct {
	Seq  int    `json:"seq"`
	Type string `json:"type"` // messageStart, delta, toolUseStart, contentBlockStop, messageStop, metadata, done, error

	ContentIndex int    `json:"contentIndex,omitempty"`
	Text         string `json:"text,omitempty"`

	ToolUseID      string `json:"toolUseId,omitempty"`
	ToolName       string `json:"toolName,omitempty"`
	ToolInputDelta string `json:"toolInputDelta,omitempty"`

	StopReason string       `json:"stopReason,omitempty"`
	Usage      *streamUsage `json:"usage,omitempty"`
	Error      string       `json:"error,omitempty"`
}
```

- [ ] **Step 4: Run the tests**

Run: `go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/inferencegateway/client.go pkg/workflow/builtin/inferencegateway/client_test.go
git commit -m "feat(inferencegateway): invokeStream subject, ack parsing, event wire types

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Stream over NATS (request, subscribe, consume, timing)

**Files:**
- Create: `pkg/workflow/builtin/inferencegateway/stream.go`
- Modify: `pkg/workflow/builtin/inferencegateway/factory.go` (delete the `Stream` stub)
- Test: `pkg/workflow/builtin/inferencegateway/stream_test.go`

**Interfaces:**
- Consumes: `buildRequest` (Task 4), `parseAck`, `streamEvent`, `requestTimeout` (Task 5), `node.LLMTiming` (Task 1), `nats_service_common.STATUS`.
- Produces: `func (g *gatewayLLM) Stream(ctx, req, out) error`; `func consumeStream(ctx context.Context, sub *nats.Subscription, idle time.Duration, out chan<- node.LLMEvent, timing *node.LLMTiming) error`.

- [ ] **Step 1: Write the failing test**

```go
package inferencegateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const testSubject = "trx.test.invokeStream"

func runEmbeddedNATS(t *testing.T) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return srv, nc
}

// fakeGateway answers testSubject: replies with ack (status header) and then
// publishes the scripted events to the request's streamSubject. It records
// the request body it saw.
type fakeGateway struct {
	status string
	ack    string
	events []string // raw JSON, published in order after the ack
	delay  time.Duration
	seen   chan invokeStreamRequest
}

func (f *fakeGateway) serve(t *testing.T, nc *nats.Conn) {
	t.Helper()
	f.seen = make(chan invokeStreamRequest, 1)
	_, err := nc.Subscribe(testSubject, func(m *nats.Msg) {
		var req invokeStreamRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Errorf("fake gateway: bad body: %v", err)
			return
		}
		f.seen <- req
		reply := &nats.Msg{Subject: m.Reply, Data: []byte(f.ack), Header: nats.Header{}}
		if f.status != "" {
			reply.Header.Set(nats_service_common.STATUS, f.status)
		}
		_ = m.RespondMsg(reply)
		for _, ev := range f.events {
			if f.delay > 0 {
				time.Sleep(f.delay)
			}
			_ = nc.Publish(req.StreamSubject, []byte(ev))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()
}

func newTestLLM(nc *nats.Conn, logs *bytes.Buffer) *gatewayLLM {
	g := newLLM(Config{Alias: "MAX_MODEL", MaxTokens: 64, StreamTimeoutSeconds: 5, IdleTimeoutSeconds: 1})
	g.nc = nc
	g.subject = testSubject
	g.wfID, g.nodeID = "wf1", "n1"
	if logs != nil {
		g.logger = log.New(logs, "", 0)
	} else {
		g.logger = log.New(io.Discard, "", 0)
	}
	return g
}

func collect(t *testing.T, g *gatewayLLM, req node.LLMRequest) ([]node.LLMEvent, error) {
	t.Helper()
	out := make(chan node.LLMEvent, 64)
	errc := make(chan error, 1)
	go func() { errc <- g.Stream(context.Background(), req, out) }()
	var evs []node.LLMEvent
	for ev := range out {
		evs = append(evs, ev)
	}
	return evs, <-errc
}

func textReq() node.LLMRequest {
	return node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: "hi"}}}}}
}

const okAck = `{"accepted":true,"modelId":"anthropic.claude-opus-5-5","invokeId":"us.anthropic.claude-opus-5-5"}`

func TestStreamTextHappyPath(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: okAck, events: []string{
		`{"seq":0,"type":"messageStart"}`,
		`{"seq":1,"type":"delta","contentIndex":0,"text":"Hel"}`,
		`{"seq":2,"type":"delta","contentIndex":0}`, // empty delta is ignored
		`{"seq":3,"type":"delta","contentIndex":0,"text":"lo"}`,
		`{"seq":4,"type":"contentBlockStop","contentIndex":0}`,
		`{"seq":5,"type":"messageStop","stopReason":"end_turn"}`,
		`{"seq":6,"type":"metadata","usage":{"inputTokens":12,"outputTokens":3,"totalTokens":15}}`,
		`{"seq":7,"type":"done"}`,
	}}
	fg.serve(t, nc)
	var logs bytes.Buffer
	evs, err := collect(t, newTestLLM(nc, &logs), textReq())
	if err != nil {
		t.Fatal(err)
	}
	seen := <-fg.seen
	if seen.Alias != "MAX_MODEL" || seen.StreamSubject == "" || seen.MaxTokens == nil || *seen.MaxTokens != 64 {
		t.Fatalf("request = %+v", seen)
	}
	var text string
	var stop string
	for _, ev := range evs {
		switch ev.Kind {
		case node.LLMTextDelta:
			text += ev.Delta
		case node.LLMMessageStop:
			stop = ev.Stop
		case node.LLMError:
			t.Fatalf("unexpected error event: %v", ev.Error)
		}
	}
	if text != "Hello" || stop != "end_turn" || len(evs) != 3 {
		t.Fatalf("text=%q stop=%q events=%d", text, stop, len(evs))
	}
	line := logs.String()
	for _, want := range []string{"llm-timing wf=wf1 node=n1", "provider=ai/inference-gateway", "model=us.anthropic.claude-opus-5-5", "in_tok=12", "out_tok=3", "stop=end_turn", "err=-"} {
		if !strings.Contains(line, want) {
			t.Fatalf("timing line %q lacks %q", line, want)
		}
	}
	if strings.Contains(line, "ttfb_ms=-") {
		t.Fatalf("ttfb must be measured: %q", line)
	}
}

func TestStreamToolUseRoundTrip(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: okAck, events: []string{
		`{"seq":0,"type":"messageStart"}`,
		`{"seq":1,"type":"toolUseStart","contentIndex":1,"toolUseId":"tu1","toolName":"search"}`,
		`{"seq":2,"type":"delta","contentIndex":1,"toolInputDelta":"{\"q\":"}`,
		`{"seq":3,"type":"delta","contentIndex":1,"toolInputDelta":"\"x\"}"}`,
		`{"seq":4,"type":"delta","contentIndex":9,"toolInputDelta":"orphan"}`, // orphan tool delta ignored
		`{"seq":5,"type":"contentBlockStop","contentIndex":1}`,
		`{"seq":6,"type":"contentBlockStop","contentIndex":9}`, // no accumulator → nothing
		`{"seq":7,"type":"messageStop","stopReason":"tool_use"}`,
		`{"seq":8,"type":"done"}`,
	}}
	fg.serve(t, nc)
	evs, err := collect(t, newTestLLM(nc, nil), textReq())
	if err != nil {
		t.Fatal(err)
	}
	kinds := []node.LLMEventKind{}
	for _, ev := range evs {
		kinds = append(kinds, ev.Kind)
	}
	want := []node.LLMEventKind{node.LLMToolUseStart, node.LLMToolUseDelta, node.LLMToolUseDelta, node.LLMToolUseStop, node.LLMMessageStop}
	if len(kinds) != len(want) {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("kinds = %v, want %v", kinds, want)
		}
	}
	tu := evs[3].ToolUse
	if tu.ID != "tu1" || tu.Name != "search" || string(tu.InputJSON) != `{"q":"x"}` {
		t.Fatalf("tool use = %+v input=%s", tu, tu.InputJSON)
	}
	if evs[4].Stop != "tool_use" {
		t.Fatalf("stop = %q", evs[4].Stop)
	}
}

func TestStreamErrorEvent(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: okAck, events: []string{
		`{"seq":0,"type":"messageStart"}`,
		`{"seq":1,"type":"error","error":"bedrock invocation failed"}`,
	}}
	fg.serve(t, nc)
	var logs bytes.Buffer
	evs, err := collect(t, newTestLLM(nc, &logs), textReq())
	if err == nil || !strings.Contains(err.Error(), "bedrock invocation failed") {
		t.Fatalf("err = %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != node.LLMError {
		t.Fatalf("events = %+v", evs)
	}
	if !strings.Contains(logs.String(), `err="`) {
		t.Fatalf("timing line must carry the error: %s", logs.String())
	}
}

func TestStreamBadAck(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "400", ack: `{"status":400,"errorMessage":"alias MAX_MODEL not found"}`}
	fg.serve(t, nc)
	evs, err := collect(t, newTestLLM(nc, nil), textReq())
	if err == nil || !strings.Contains(err.Error(), "alias MAX_MODEL not found") {
		t.Fatalf("err = %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != node.LLMError {
		t.Fatalf("events = %+v", evs)
	}
}

func TestStreamNoResponder(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	g := newTestLLM(nc, nil)
	g.subject = "trx.nobody.invokeStream"
	_, err := collect(t, g, textReq())
	if err == nil || !strings.Contains(err.Error(), "gateway request") {
		t.Fatalf("err = %v", err)
	}
}

func TestStreamSeqGap(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: okAck, events: []string{
		`{"seq":0,"type":"messageStart"}`,
		`{"seq":2,"type":"delta","text":"x"}`,
	}}
	fg.serve(t, nc)
	_, err := collect(t, newTestLLM(nc, nil), textReq())
	if err == nil || !strings.Contains(err.Error(), "seq gap: want 1 got 2") {
		t.Fatalf("err = %v", err)
	}
}

func TestStreamIdleTimeout(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: okAck, events: []string{`{"seq":0,"type":"messageStart"}`}} // never sends done
	fg.serve(t, nc)
	start := time.Now()
	_, err := collect(t, newTestLLM(nc, nil), textReq()) // idle = 1s
	if err == nil || !strings.Contains(err.Error(), "idle") {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 4*time.Second {
		t.Fatal("idle timeout did not fire in time")
	}
}

func TestStreamTimeout(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	events := []string{}
	for i := 0; i < 40; i++ {
		events = append(events, `{"seq":`+strconv.Itoa(i)+`,"type":"delta","text":"x"}`)
	}
	fg := &fakeGateway{status: "200", ack: okAck, events: events, delay: 100 * time.Millisecond}
	fg.serve(t, nc)
	g := newTestLLM(nc, nil)
	g.streamTimeout = 1 * time.Second // events keep the idle timer alive; the stream bound must still fire
	_, err := collect(t, g, textReq())
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("err = %v", err)
	}
}

func TestStreamCallerCancel(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: okAck, events: []string{`{"seq":0,"type":"messageStart"}`}}
	fg.serve(t, nc)
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan node.LLMEvent, 8)
	errc := make(chan error, 1)
	go func() { errc <- newTestLLM(nc, nil).Stream(ctx, textReq(), out) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	err := <-errc
	if err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("err = %v", err)
	}
}

func TestStreamRejectsDocumentBeforeRequest(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	fg := &fakeGateway{status: "200", ack: okAck}
	fg.serve(t, nc)
	doc := node.LLMRequest{Messages: []node.Message{{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockDocument, Data: []byte("x")}}}}}
	evs, err := collect(t, newTestLLM(nc, nil), doc)
	if err == nil || !strings.Contains(err.Error(), "document blocks") {
		t.Fatalf("err = %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != node.LLMError {
		t.Fatalf("events = %+v", evs)
	}
	select {
	case r := <-fg.seen:
		t.Fatalf("gateway must not be called, saw %+v", r)
	case <-time.After(200 * time.Millisecond):
	}
}

```

Add `"strconv"` to the test file imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -run 'TestStream' -v`
Expected: FAIL — every test errors with `not implemented` (stub from Task 3).

- [ ] **Step 3: Implement**

Delete the `Stream` stub from `factory.go`. Create `stream.go`:

```go
package inferencegateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Stream sends one invokeStream request to the gateway and translates the
// events it publishes on a private inbox into node.LLMEvent values on out.
// Closes out before returning. Every call logs one llm-timing line.
func (g *gatewayLLM) Stream(ctx context.Context, req node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	timing := node.LLMTiming{Provider: nodeType, Start: time.Now()}
	defer func() {
		timing.End = time.Now()
		node.LogLLMTiming(g.logger, g.wfID, g.nodeID, timing)
	}()
	fail := func(err error) error {
		timing.Err = err
		if g.logger != nil {
			g.logger.Printf("ai/inference-gateway wf=%s node=%s subject=%s failed: %v", g.wfID, g.nodeID, g.subject, err)
		}
		emit(ctx, out, node.LLMEvent{Kind: node.LLMError, Error: err})
		return err
	}

	wire, err := buildRequest(req, g.cfg)
	if err != nil {
		return fail(err)
	}

	sctx, cancel := context.WithTimeout(ctx, g.streamTimeout)
	defer cancel()

	// Subscribe BEFORE the request so no event can be published into the void.
	inbox := g.nc.NewInbox()
	sub, err := g.nc.SubscribeSync(inbox)
	if err != nil {
		return fail(fmt.Errorf("ai/inference-gateway: subscribe %s: %w", inbox, err))
	}
	defer func() { _ = sub.Unsubscribe() }()
	wire.StreamSubject = inbox

	body, err := json.Marshal(wire)
	if err != nil {
		return fail(err)
	}
	rctx, rcancel := context.WithTimeout(sctx, requestTimeout)
	msg, err := g.nc.RequestWithContext(rctx, g.subject, body)
	rcancel()
	if err != nil {
		return fail(fmt.Errorf("ai/inference-gateway: gateway request %s: %w", g.subject, err))
	}
	ack, err := parseAck(msg.Header.Get(nats_service_common.STATUS), msg.Data)
	if err != nil {
		return fail(fmt.Errorf("ai/inference-gateway: %w", err))
	}
	timing.Model = ack.InvokeID

	if err := consumeStream(sctx, sub, g.idleTimeout, out, &timing); err != nil {
		return fail(err)
	}
	return nil
}

// consumeStream reads gateway StreamEvents from sub until done/error,
// emitting LLMEvents. seq must be contiguous from 0. idle bounds the silence
// between two events; ctx bounds the whole stream.
func consumeStream(ctx context.Context, sub *nats.Subscription, idle time.Duration, out chan<- node.LLMEvent, timing *node.LLMTiming) error {
	accum := map[int]*node.LLMToolUse{}
	want := 0
	for {
		ictx, icancel := context.WithTimeout(ctx, idle)
		m, err := sub.NextMsgWithContext(ictx)
		icancel()
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("ai/inference-gateway: stream: %w", ctx.Err())
			}
			return fmt.Errorf("ai/inference-gateway: stream idle for %s without events: %w", idle, err)
		}
		var ev streamEvent
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			return fmt.Errorf("ai/inference-gateway: bad stream event: %w", err)
		}
		if ev.Seq != want {
			return fmt.Errorf("ai/inference-gateway: stream seq gap: want %d got %d", want, ev.Seq)
		}
		want++
		if timing.FirstEvent.IsZero() {
			timing.FirstEvent = time.Now()
		}

		switch ev.Type {
		case "delta":
			if ev.Text != "" {
				emit(ctx, out, node.LLMEvent{Kind: node.LLMTextDelta, Delta: ev.Text})
			} else if ev.ToolInputDelta != "" {
				if tu := accum[ev.ContentIndex]; tu != nil {
					tu.InputJSON = append(tu.InputJSON, []byte(ev.ToolInputDelta)...)
					emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseDelta, ToolUse: tu})
				}
			}
		case "toolUseStart":
			tu := &node.LLMToolUse{ID: ev.ToolUseID, Name: ev.ToolName, InputJSON: []byte("")}
			accum[ev.ContentIndex] = tu
			emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseStart, ToolUse: tu})
		case "contentBlockStop":
			if tu := accum[ev.ContentIndex]; tu != nil {
				emit(ctx, out, node.LLMEvent{Kind: node.LLMToolUseStop, ToolUse: tu})
				delete(accum, ev.ContentIndex)
			}
		case "messageStop":
			timing.Stop = ev.StopReason
			if ev.StopReason != "" {
				emit(ctx, out, node.LLMEvent{Kind: node.LLMMessageStop, Stop: ev.StopReason})
			}
		case "metadata":
			if ev.Usage != nil {
				timing.HasUsage = true
				timing.InputTokens = int(ev.Usage.InputTokens)
				timing.OutputTokens = int(ev.Usage.OutputTokens)
			}
		case "done":
			return nil
		case "error":
			return errors.New("ai/inference-gateway: gateway stream error: " + ev.Error)
		// messageStart and unknown types: nothing to emit.
		}
	}
}

func emit(ctx context.Context, out chan<- node.LLMEvent, evt node.LLMEvent) {
	select {
	case out <- evt:
	case <-ctx.Done():
	}
}
```

- [ ] **Step 4: Run the tests (with race detector)**

Run: `go test -mod=vendor -race ./pkg/workflow/builtin/inferencegateway/ -v`
Expected: PASS. `TestStreamTimeout` error text contains `context deadline exceeded`; `TestStreamCallerCancel` contains `context canceled`.

- [ ] **Step 5: Commit**

```bash
git add pkg/workflow/builtin/inferencegateway/
git commit -m "feat(inferencegateway): stream model calls through the gateway over NATS

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Init from the NATS host, registration, docs, changelog

**Files:**
- Modify: `pkg/workflow/builtin/inferencegateway/factory.go` (replace `Init` stub)
- Create: `pkg/workflow/builtin/inferencegateway/init_test.go`
- Modify: `pkg/workflow/builtin/register.go` (add import + `reg.Register("ai/inference-gateway", inferencegateway.Factory)`)
- Modify: `pkg/workflow/builtin/register_test.go:15` (add `"ai/inference-gateway"` to `want`)
- Modify: `README.md:56`, `docs/EXTENDING.md:114`, `docs/DEPLOYMENT.md:42`, `CHANGELOG.md`

**Interfaces:**
- Consumes: `invokeSubject` (Task 5), `*nats_service.NatService.GetNatsService() *nats.Conn`.
- Produces: `func natsConn(env hostLookup) *nats.Conn`; `type hostLookup interface{ Host(kind string) (any, bool) }`.

- [ ] **Step 1: Write the failing test**

```go
package inferencegateway

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
)

// fakeEnv is the minimal node.NodeEnv for Init: only Host, Logger, NodeID
// and WorkflowID matter here.
type fakeEnv struct{ hosts map[string]any }

func (f fakeEnv) Logger() *log.Logger                          { return log.New(io.Discard, "", 0) }
func (f fakeEnv) NodeID() string                               { return "n1" }
func (f fakeEnv) WorkflowID() string                           { return "wf1" }
func (f fakeEnv) Secret(string) (secret.String, error)         { return secret.String{}, nil }
func (f fakeEnv) Render(s string, _ node.RenderCtx) (string, error) { return s, nil }
func (f fakeEnv) Host(kind string) (any, bool)                 { v, ok := f.hosts[kind]; return v, ok }
func (f fakeEnv) Peer(string) ([]node.Node, error)             { return nil, nil }
func (f fakeEnv) AwaitClientToolResult(context.Context, string, time.Duration) ([]byte, error) {
	return nil, nil
}
func (f fakeEnv) RetryPolicy() node.RetryPolicy { return nil }

func TestInitRequiresNatsHost(t *testing.T) {
	g := newLLM(Config{Alias: "A"})
	err := g.Init(context.Background(), fakeEnv{hosts: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "nats host") {
		t.Fatalf("missing host: err = %v", err)
	}
	err = g.Init(context.Background(), fakeEnv{hosts: map[string]any{"nats": "wrong type"}})
	if err == nil || !strings.Contains(err.Error(), "nats host") {
		t.Fatalf("wrong type: err = %v", err)
	}
}

func TestInitWiresConnectionAndSubject(t *testing.T) {
	srv, _ := runEmbeddedNATS(t)
	ns, err := nats_service.NewLowLevel("trx.test.agent", "q", srv.ClientURL(), "", "", 2048, 300*1024)
	if err != nil {
		t.Fatalf("NewLowLevel: %v", err)
	}
	// Not closing ns: nats-service's ClosedHandler exits the process on any
	// connection close (same caveat as bedrock's TestNatsConn).
	t.Setenv(basePathEnv, "trx.gw")
	g := newLLM(Config{Alias: "A"})
	if err := g.Init(context.Background(), fakeEnv{hosts: map[string]any{"nats": ns}}); err != nil {
		t.Fatal(err)
	}
	if g.nc == nil || g.subject != "trx.gw.invokeStream" || g.wfID != "wf1" || g.nodeID != "n1" || g.logger == nil {
		t.Fatalf("init state = %+v", g)
	}
	g2 := newLLM(Config{Alias: "A", BasePath: "trx.override"})
	_ = g2.Init(context.Background(), fakeEnv{hosts: map[string]any{"nats": ns}})
	if g2.subject != "trx.override.invokeStream" {
		t.Fatalf("basePath override: %q", g2.subject)
	}
}
```

Check the exact `secret.String` zero value compiles: `grep -n "type String" pkg/secret/*.go`. If `String` is a struct with unexported fields, `secret.String{}` is valid.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -mod=vendor ./pkg/workflow/builtin/inferencegateway/ -run 'TestInit' -v`
Expected: FAIL — the stub `Init` returns nil, so `TestInitRequiresNatsHost` fails with `missing host: err = <nil>`.

- [ ] **Step 3: Implement Init and registration**

Replace the `Init` stub in `factory.go`:

```go
// hostLookup is the slice of node.NodeEnv natsConn needs (narrowed for tests).
type hostLookup interface {
	Host(kind string) (any, bool)
}

// natsConn returns the shared NATS connection from the hosts map, or nil
// when the host is missing, mistyped, or not connected.
func natsConn(env hostLookup) *nats.Conn {
	host, ok := env.Host("nats")
	if !ok {
		return nil
	}
	ns, ok := host.(*nats_service.NatService)
	if !ok {
		return nil
	}
	return ns.GetNatsService()
}

// Init binds the node to the shared NATS connection and fixes the
// invokeStream subject. No connection means the workflow cannot call any
// model, so it fails loudly at load time.
func (g *gatewayLLM) Init(_ context.Context, env node.NodeEnv) error {
	g.logger = env.Logger()
	g.nodeID = env.NodeID()
	g.wfID = env.WorkflowID()
	g.nc = natsConn(env)
	if g.nc == nil {
		return errors.New("ai/inference-gateway: nats host not registered or not connected")
	}
	g.subject = invokeSubject(g.cfg.BasePath)
	g.logger.Printf("ai/inference-gateway wf=%s node=%s ready (alias=%q modelId=%q lab=%q family=%q subject=%s)",
		g.wfID, g.nodeID, g.cfg.Alias, g.cfg.ModelID, g.cfg.Lab, g.cfg.Family, g.subject)
	return nil
}
```

Add `nats_service "github.com/transactrx/nats-service/pkg/nats-service"` to the imports.

In `register.go` add the import `"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/inferencegateway"` and, right after the `ai/bedrock` line:

```go
		reg.Register("ai/inference-gateway", inferencegateway.Factory),
```

In `register_test.go` change the first `want` line to:

```go
		"trigger/nats-chat", "ai/agent", "ai/bedrock", "ai/inference-gateway",
```

- [ ] **Step 4: Docs and changelog**

`README.md` line 56: `LLM: \`ai/bedrock\`` → `LLM: \`ai/bedrock\`, \`ai/inference-gateway\``.

`docs/EXTENDING.md` line 114, add after the `ai/bedrock` bullet:

```markdown
- **LLM (gateway):** `ai/inference-gateway` — model calls through the org inferenceGateway
  NATS service (`<INFERENCE_GATEWAY_BASE_PATH>.invokeStream`), model chosen by `alias`
  (tier such as `MAX_MODEL`), `modelId`, or `lab`+`family`. No auto-update: re-pointing a
  tier in the gateway moves every caller. Config: `maxTokens` (4096), `temperature`,
  `streamTimeoutSeconds` (600), `idleTimeoutSeconds` (120), `basePath` override.
  Document blocks are not supported by the gateway and are rejected.
```

`docs/DEPLOYMENT.md` line 42, replace the `INFERENCE_GATEWAY_BASE_PATH` row's description with:

```
Org inferenceGateway NATS base path (org value: `trx.inferenceGateway`). Used by `ai/inference-gateway` for every model call (`<base>.invokeStream`; unreachable → the call fails) and by the `ai/bedrock` auto-update (`<base>.resolveModel`; unreachable → Bedrock catalog-scan fallback). Set it in the consuming service's deployment env.
```

Add after the env table in `docs/DEPLOYMENT.md`:

```markdown
### `llm-timing` log line

Every LLM call (`ai/bedrock` and `ai/inference-gateway`) ends with one line:
`llm-timing wf=<id> node=<id> provider=<type> model=<invoked id> ttfb_ms=<n> total_ms=<n> in_tok=<n> out_tok=<n> stop=<reason> err=<quoted|->`.
`ttfb_ms` is request send → first streamed event; unknown values print `-`.
```

`CHANGELOG.md`, insert at the top under `# Changelog`:

```markdown
## v1.8.0

- **feat: `ai/inference-gateway` LLM node.** Model calls go through the org inferenceGateway
  NATS service (`<INFERENCE_GATEWAY_BASE_PATH>.invokeStream`) and the model is selected by
  `alias` (tier), `modelId`, or `lab`+`family`. Forwards system, messages, tools, forced tool
  choice, maxTokens, temperature and stop sequences; streams text and tool-use events with
  seq checking, idle (120 s) and stream (600 s) timeouts. Document blocks are rejected (the
  gateway has no document content block). `ai/bedrock` is unchanged and still available.
- **feat: `llm-timing` log line** from both LLM providers, one per call:
  `llm-timing wf= node= provider= model= ttfb_ms= total_ms= in_tok= out_tok= stop= err=`.
  `ai/bedrock` now parses token usage from `message_start` / `message_delta` for it.
```

- [ ] **Step 5: Run everything**

Run: `go build -mod=vendor ./... && go vet -mod=vendor ./pkg/workflow/... && go test -mod=vendor -race ./pkg/workflow/... ./pkg/rsassistant/...`
Expected: PASS. `TestRegisterDefaults_RegistersGenericTypes` passes with the new type.

- [ ] **Step 6: Commit**

```bash
git add pkg/workflow/builtin/inferencegateway/ pkg/workflow/builtin/register.go pkg/workflow/builtin/register_test.go README.md docs/EXTENDING.md docs/DEPLOYMENT.md CHANGELOG.md
git commit -m "feat(inferencegateway): Init from the NATS host, register ai/inference-gateway, docs, v1.8.0 changelog

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

Library done. **Stop here and ask the user before any push.** The release path is: push the branch, PR to Development, then the lib's release process (`release:minor` label on the Development→Production PR) mints v1.8.0. Until v1.8.0 exists, Part B uses a local `replace`.

---

## Part B — App (repo `opensearchAiChatApi`, new branch `feature/inference-gateway-llm` from `Development`)

### Task 8: Point the direct path at opus 5.5 and pick up the library

**Files:**
- Modify: `go.mod` (lib version / temporary replace), `go.sum`, `vendor/` (the app vendors its dependencies)
- Modify: `workflows/powerlineSearch.json:47-48`, `workflows/eprescribeSearch.json` (the `ai/bedrock` node config)

- [ ] **Step 1: Create the branch and wire the library**

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/opensearchAiChatApi
git checkout Development && git pull --ff-only
git checkout -b feature/inference-gateway-llm
# Until v1.8.0 is released, build against the local checkout:
go mod edit -replace github.com/transactrx/ai-agent-go-service=../ai-agent-go-service
go mod tidy && go mod vendor
go build ./...
```

After v1.8.0 exists, a final commit swaps the replace for the tag:
`go get github.com/transactrx/ai-agent-go-service@v1.8.0 && go mod edit -dropreplace github.com/transactrx/ai-agent-go-service && go mod tidy && go mod vendor`, then `git add go.mod go.sum vendor && git commit -m "build: ai-agent-go-service v1.7.3 -> v1.8.0 (ai/inference-gateway)"`. Until then keep `go.mod`, `go.sum` and `vendor/` **uncommitted** (same practice as the RSAssistant rollout); every commit in Tasks 8–9 stages only the listed files.

- [ ] **Step 2: Write the failing test** (extend `pkg/workflow/rsassistant_manifest_test.go`)

```go
// The direct path is pinned: every ai/bedrock node in the resolved workflows
// runs opus 5.5 with auto-update off (comparison baseline for the gateway).
func TestDirectBedrockNodesArePinnedToOpus55(t *testing.T) {
	for id, raw := range loadResolved(t) {
		var wf struct {
			Nodes []struct {
				ID     string `json:"id"`
				Type   string `json:"type"`
				Config struct {
					Model      string `json:"model"`
					AutoUpdate *bool  `json:"autoUpdate"`
				} `json:"config"`
			} `json:"nodes"`
		}
		if err := json.Unmarshal(raw, &wf); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		for _, n := range wf.Nodes {
			if n.Type != "ai/bedrock" {
				continue
			}
			if n.Config.Model != "us.anthropic.claude-opus-5-5" {
				t.Fatalf("%s node %s: model = %q, want us.anthropic.claude-opus-5-5", id, n.ID, n.Config.Model)
			}
			if n.Config.AutoUpdate == nil || *n.Config.AutoUpdate {
				t.Fatalf("%s node %s: autoUpdate must be false", id, n.ID)
			}
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./pkg/workflow/ -run TestDirectBedrockNodesArePinnedToOpus55 -v`
Expected: FAIL, `model = "us.anthropic.claude-opus-4-7"`

- [ ] **Step 4: Edit both base workflows**

In `workflows/powerlineSearch.json` and `workflows/eprescribeSearch.json`, the `ai/bedrock` node config becomes:

```json
      "config": {
        "model": "us.anthropic.claude-opus-5-5",
        "autoUpdate": false,
        "region": "${AWS_REGION_BEDROCK:us-east-1}",
        "maxTokens": 4096
      }
```

(keep any other keys already present in the eprescribe file).

- [ ] **Step 5: Run the app tests**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 6: Commit (workflows only)**

```bash
git add workflows/powerlineSearch.json workflows/eprescribeSearch.json pkg/workflow/rsassistant_manifest_test.go
git commit -m "workflows: pin direct ai/bedrock path to opus 5.5, auto-update off (gateway comparison baseline)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Derived `powerlineSearchGateway` workflow, env, docs

**Files:**
- Create: `workflows/powerlineSearchGateway.json` (generated from the base with the script below, then reviewed)
- Modify: `compose.local.env` (add `INFERENCE_GATEWAY_BASE_PATH=trx.inferenceGateway`), `docs/configuration.md:24`
- Test: `pkg/workflow/rsassistant_manifest_test.go` (new test)

- [ ] **Step 1: Write the failing test**

```go
// The gateway variant swaps only the LLM node: same id, new type, alias
// config, base connections intact, never published as an RSAssistant agent.
func TestGatewayVariantSwapsOnlyTheLLMNode(t *testing.T) {
	resolved := loadResolved(t)
	raw, ok := resolved["powerlineSearchGateway"]
	if !ok {
		t.Fatal("powerlineSearchGateway must load")
	}
	var wf struct {
		Extends     string          `json:"extends"`
		RSAssistant json.RawMessage `json:"rsassistant"`
		Nodes       []struct {
			ID     string          `json:"id"`
			Type   string          `json:"type"`
			Config json.RawMessage `json:"config"`
		} `json:"nodes"`
		Connections []struct {
			From struct {
				Node string `json:"node"`
				Port string `json:"port"`
			} `json:"from"`
			To struct {
				Node string `json:"node"`
				Port string `json:"port"`
			} `json:"to"`
		} `json:"connections"`
	}
	if err := json.Unmarshal(raw, &wf); err != nil {
		t.Fatal(err)
	}
	if len(wf.RSAssistant) != 0 && string(wf.RSAssistant) != "null" {
		t.Fatalf("rsassistant must be removed, got %s", wf.RSAssistant)
	}
	gw, direct := 0, 0
	for _, n := range wf.Nodes {
		switch n.Type {
		case "ai/inference-gateway":
			gw++
			if n.ID != "bedrock1" {
				t.Fatalf("gateway node id = %q, want bedrock1", n.ID)
			}
			var cfg map[string]any
			_ = json.Unmarshal(n.Config, &cfg)
			if cfg["alias"] != "${INFERENCE_GATEWAY_ALIAS:MAX_MODEL}" || cfg["maxTokens"] != float64(4096) {
				t.Fatalf("gateway config = %v", cfg)
			}
			for _, gone := range []string{"model", "region", "autoUpdate"} {
				if _, present := cfg[gone]; present {
					t.Fatalf("gateway config must not carry %q", gone)
				}
			}
		case "ai/bedrock":
			direct++
		}
	}
	if gw != 1 || direct != 0 {
		t.Fatalf("gateway nodes = %d, direct nodes = %d", gw, direct)
	}
	wired := false
	for _, c := range wf.Connections {
		if (c.From.Node == "bedrock1" || c.To.Node == "bedrock1") && (c.From.Port == "ai_languageModel" || c.To.Port == "ai_languageModel") {
			wired = true
		}
	}
	if !wired {
		t.Fatal("bedrock1 must stay connected on port ai_languageModel")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/workflow/ -run TestGatewayVariantSwapsOnlyTheLLMNode -v`
Expected: FAIL, `powerlineSearchGateway must load`

- [ ] **Step 3: Generate the derived file**

The prompts are large and must be copied verbatim (loader rule), so generate the file with `jq` instead of hand-editing:

```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/opensearchAiChatApi
jq '
  {
    "$schema": ."$schema",
    "id": "powerlineSearchGateway",
    "extends": "powerlineSearch",
    "version": 1,
    "description": "Pharmacy claim search agent (PowerLine). Gateway variant: same agent, model called through the org inferenceGateway by alias, for the direct-vs-gateway comparison.",
    "rsassistant": null,
    "nodes": [
      { "id": "bedrock1", "type": "ai/inference-gateway",
        "config": { "alias": "${INFERENCE_GATEWAY_ALIAS:MAX_MODEL}", "maxTokens": 4096,
                    "model": null, "region": null, "autoUpdate": null } },
      ( .nodes[] | select(.type == "ai/agent")
        | { "id": .id, "config": { "systemMessageFixed": .config.systemMessageFixed,
                                   "systemMessageFlexible": .config.systemMessageFlexible } } )
    ]
  }' workflows/powerlineSearch.json > workflows/powerlineSearchGateway.json
```

Open the result and check: the `$schema` line survived, the agent node id matches the base (`agent1`), and both prompt strings are present. The description is a literal on purpose: with `rsassistant` removed, `BuildCardSpec` falls back to the workflow description and the manifest test rejects any `${VAR}` in it. If the base `ai/bedrock` config has a `name` key, it is inherited by deep-merge; leave it.

- [ ] **Step 4: Env and docs**

Append to `compose.local.env`:

```
INFERENCE_GATEWAY_BASE_PATH=trx.inferenceGateway
```

`docs/configuration.md` line 24: replace the `INFERENCE_GATEWAY_BASE_PATH` row with:

```markdown
| `INFERENCE_GATEWAY_BASE_PATH` | for gateway workflows | NATS base path of the org inferenceGateway (`trx.inferenceGateway`). `ai/inference-gateway` nodes (workflow `powerlineSearchGateway`) call `<base>.invokeStream` for every model request and fail when it is unreachable. The ai/bedrock auto-updater also asks `<base>.resolveModel` when enabled. Default: `example.inferenceGateway`. |
| `INFERENCE_GATEWAY_ALIAS` | no | Tier the gateway workflow asks for. Default: `MAX_MODEL`. |
```

- [ ] **Step 5: Run the app tests**

Run: `go test ./...`
Expected: PASS, including `TestRSAssistantManifestsResolveToExpectedAgents` (the variant has UI tools, so it is allowed to carry the default card name) and `TestGatewayVariantSwapsOnlyTheLLMNode`.

- [ ] **Step 6: Local load check**

Restart the chat API in the user's local ecosystem (ask the user to restart it; do not restart components yourself). Then:

```bash
grep -E "ai/inference-gateway wf=powerlineSearchGateway .* ready|powerlineSearchGateway" <api log>
nats --context localhost request --timeout 90s trx.local.powerlineSearchGateway \
  -H X-Account-Id:<test account> -H X-User-Id:<test user> -H X-User-Name:bench \
  '{"message":"How many claims were processed today?","responseMode":"single"}'
```

Expected: the ready line names subject `trx.inferenceGateway.invokeStream`; the request returns an answer; the API log shows `llm-timing … provider=ai/inference-gateway model=us.anthropic.claude-opus-5-5`. Repeat against `trx.local.powerlineSearch` and confirm `provider=ai/bedrock model=us.anthropic.claude-opus-5-5`.

- [ ] **Step 7: Commit**

```bash
git add workflows/powerlineSearchGateway.json compose.local.env docs/configuration.md pkg/workflow/rsassistant_manifest_test.go
git commit -m "workflows: powerlineSearchGateway variant on ai/inference-gateway (MAX_MODEL) for the direct-vs-gateway comparison

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: Bench (throwaway, scratchpad only)

**Files:**
- Create (scratchpad, never committed): `<scratchpad>/llmbench/main.go`, `<scratchpad>/llmbench/go.mod`

- [ ] **Step 1: Write the bench program**

`go.mod` (module `llmbench`, `go 1.27`, `require github.com/transactrx/ai-agent-go-service v0.0.0` with `replace github.com/transactrx/ai-agent-go-service => /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service`, plus `github.com/nats-io/nats.go`).

`main.go`:

```go
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

type sample struct {
	path            string
	ttfb, total     time.Duration
	chars           int
	err             error
}

func main() {
	url := flag.String("nats", os.Getenv("NATS_URL"), "NATS url")
	base := flag.String("base", "trx.local", "NATS base path")
	n := flag.Int("n", 10, "runs per path")
	account := flag.String("account", "", "X-Account-Id")
	user := flag.String("user", "bench", "X-User-Id")
	flag.Parse()
	if *account == "" {
		log.Fatal("-account is required")
	}
	prompts := []string{
		"How many claims were processed today? One sentence.",
		"Which bin has the highest reject rate today? Short answer.",
		"Show total transactions per region for today as a short table.",
	}
	opts := []nats.Option{}
	if jwt, key := os.Getenv("NATS_JWT"), os.Getenv("NATS_KEY"); jwt != "" && key != "" {
		opts = append(opts, nats.UserJWTAndSeed(jwt, key))
	}
	nc, err := nats.Connect(*url, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	logger := log.New(os.Stderr, "", 0)
	headers := map[string]string{"X-Account-Id": *account, "X-User-Id": *user, "X-User-Name": "bench", "X-Time-Zone": "America/New_York"}
	paths := []string{"powerlineSearch", "powerlineSearchGateway"}

	var samples []sample
	for i := 0; i < *n; i++ {
		prompt := prompts[i%len(prompts)]
		for _, p := range paths { // alternate so load conditions are shared
			body, _ := json.Marshal(map[string]string{"message": prompt, "responseMode": "streaming"})
			s := sample{path: p}
			start := time.Now()
			sub, err := natsstream.DoStreamingRequest(nc, *base+"."+p, headers, body, 180*time.Second, logger)
			if err != nil {
				s.err = err
				samples = append(samples, s)
				continue
			}
			first := time.Time{}
			for ev := range sub.Events() {
				switch ev.Type {
				case natsstream.StreamDelta:
					if first.IsZero() {
						first = time.Now()
					}
					s.chars += len(ev.Data)
				case natsstream.StreamError:
					s.err = fmt.Errorf("stream error: %s", ev.Data)
				}
			}
			sub.Close()
			s.total = time.Since(start)
			if !first.IsZero() {
				s.ttfb = first.Sub(start)
			}
			samples = append(samples, s)
			fmt.Printf("%-24s run=%d ttfb=%v total=%v chars=%d err=%v\n", p, i, s.ttfb.Round(time.Millisecond), s.total.Round(time.Millisecond), s.chars, s.err)
		}
	}
	for _, p := range paths {
		var ttfb, total []time.Duration
		fails := 0
		for _, s := range samples {
			if s.path != p {
				continue
			}
			if s.err != nil {
				fails++
				continue
			}
			ttfb = append(ttfb, s.ttfb)
			total = append(total, s.total)
		}
		fmt.Printf("\n%s: ok=%d fail=%d ttfb median=%v p95=%v | total median=%v p95=%v\n", p, len(total), fails, pct(ttfb, 50), pct(ttfb, 95), pct(total, 50), pct(total, 95))
	}
}

func pct(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	idx := (len(d)-1) * p / 100
	return d[idx].Round(time.Millisecond)
}
```

Check `natsstream.StreamEvent.Data` is the delta payload (`json.RawMessage`); if delta text is wrapped in an object (`{"text":"..."}`), count `len(ev.Data)` anyway — it is only a size proxy.

- [ ] **Step 2: Run against the local ecosystem**

```bash
cd <scratchpad>/llmbench && go mod tidy
set -a; source /Users/yceleiro/Documents/TransactRx/GitHub/opensearchAiChatApi/compose.local.env; set +a
go run . -n 10 -account <test account id> -user bench 2>/dev/null | tee ../bench-results.txt
```

Then collect the model-only numbers from the API log:

```bash
grep "llm-timing" <api log> | grep -E "wf=powerlineSearch(Gateway)? " > <scratchpad>/llm-timing-lines.txt
```

- [ ] **Step 3: Report**

Produce a table per path: end-to-end ttfb and total (median, p95), pure model ttfb/total from `llm-timing` (median), in/out tokens, failures. State the thinking caveat: the gateway path runs Converse with no `thinking` field while the direct path sends `thinking: disabled`; if gateway `out_tok` is markedly higher at similar answer length, that is the cause and the fix belongs in inferenceGateway. Save the report to `<scratchpad>/bench-report.md` and paste it in chat. Nothing from this task is committed.

---

## Self-review notes

- Spec §3.1–3.5 → Tasks 3–7; §3.4 timing → Tasks 1, 2, 6; §4 app → Tasks 8, 9; §5 bench → Task 10; §6 error table → Task 6 tests (bad ack, no responder, error event, seq gap, idle, stream timeout, cancel, document) and Task 7 (no NATS host); §7 testing → each task's tests plus Task 9 step 6 live check; §8 ship order → end of Task 7 and Task 8 step 1.
- Names used consistently: `newLLM`, `gatewayLLM{nc, subject, streamTimeout, idleTimeout, logger, nodeID, wfID}`, `buildRequest`, `invokeStreamRequest`, `parseAck`, `streamAck`, `streamEvent`, `consumeStream`, `invokeSubject`, `natsConn`, `anthropicStats`, `handleAnthropicChunkStats`, `node.LLMTiming`, `node.LogLLMTiming`.
- Review Focus items 1–5 are pinned by tests in Tasks 6 (1–3), 4 (4) and 9 (5).
