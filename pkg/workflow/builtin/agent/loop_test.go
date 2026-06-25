package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/retry"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeLLM emits a scripted sequence of LLMEvents per call.
type fakeLLM struct {
	scripts [][]node.LLMEvent
	calls   int
}

func (fakeLLM) Spec() node.NodeSpec                          { return node.NodeSpec{Role: node.RoleLLM} }
func (fakeLLM) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (fakeLLM) Close(_ context.Context) error                { return nil }
func (l *fakeLLM) Stream(_ context.Context, _ node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	if l.calls >= len(l.scripts) {
		return fmt.Errorf("fakeLLM: no more scripts")
	}
	for _, ev := range l.scripts[l.calls] {
		out <- ev
	}
	l.calls++
	return nil
}

// recordingSink captures stream events.
type recordingSink struct {
	mu     sync.Mutex
	events []node.StreamEvent
	term   node.StreamEvent
	closed bool
}

func (s *recordingSink) Send(_ context.Context, e node.StreamEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}
func (s *recordingSink) Close(_ context.Context, e node.StreamEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.term = e
	return nil
}

// fakeMem is an in-memory Memory.
type fakeMem struct {
	mu    sync.Mutex
	turns []node.Turn
}

func (fakeMem) Spec() node.NodeSpec                          { return node.NodeSpec{Role: node.RoleMemory} }
func (fakeMem) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (fakeMem) Close(_ context.Context) error                { return nil }
func (m *fakeMem) Load(_ context.Context, _ node.MemoryKey) ([]node.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []node.Message
	for _, t := range m.turns {
		out = append(out, t.User, t.Assistant)
	}
	return out, nil
}
func (m *fakeMem) Append(_ context.Context, _ node.MemoryKey, t node.Turn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.turns = append(m.turns, t)
	return nil
}

// fakeTool returns a configured payload (or error). Implements node.Tool.
type fakeTool struct {
	name    string
	out     json.RawMessage
	err     error
	policy  node.FailurePolicy
	invoked int
}

func (fakeTool) Spec() node.NodeSpec                          { return node.NodeSpec{Role: node.RoleTool} }
func (fakeTool) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (fakeTool) Close(_ context.Context) error                { return nil }
func (f *fakeTool) ToolSpec() node.ToolSpec {
	return node.ToolSpec{Name: f.name, Description: "test", InputSchema: json.RawMessage(`{}`)}
}
func (f *fakeTool) FailurePolicy() node.FailurePolicy {
	if f.policy == "" {
		return node.FailureSurfaceToLLM
	}
	return f.policy
}
func (fakeTool) RetryPolicy() node.RetryPolicy { return nil }
func (f *fakeTool) Invoke(_ context.Context, _ json.RawMessage) (json.RawMessage, error) {
	f.invoked++
	return f.out, f.err
}

func TestAgentTextOnlyHappyPath(t *testing.T) {
	a := &agentNode{
		cfg:        Config{MaxIterations: 5, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMTextDelta, Delta: "hello "},
				{Kind: node.LLMTextDelta, Delta: "world"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	mem := &fakeMem{}
	a.mem = mem

	sink := &recordingSink{}
	err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if !sink.closed || sink.term.Type != node.StreamComplete {
		t.Fatalf("term: %+v", sink.term)
	}
	if !strings.Contains(string(sink.term.Data), "hello world") {
		t.Fatalf("final text missing: %s", sink.term.Data)
	}
	if len(mem.turns) != 1 {
		t.Fatalf("memory turns: %d", len(mem.turns))
	}
}

func TestAgentSingleToolCall(t *testing.T) {
	tool := &fakeTool{name: "T", out: json.RawMessage(`{"x":1}`)}
	a := &agentNode{
		cfg:        Config{MaxIterations: 5, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		tools:      []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "T", InputJSON: []byte(`{"q":1}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "done"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if tool.invoked != 1 {
		t.Fatalf("tool invoked %d", tool.invoked)
	}
	if !sink.closed || sink.term.Type != node.StreamComplete {
		t.Fatalf("term: %+v", sink.term)
	}
}

func TestAgentMaxIterationsExceeded(t *testing.T) {
	tool := &fakeTool{name: "T", out: json.RawMessage(`{}`)}
	scripts := [][]node.LLMEvent{}
	for i := 0; i < 3; i++ {
		scripts = append(scripts, []node.LLMEvent{
			{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: fmt.Sprintf("tu%d", i), Name: "T", InputJSON: []byte(`{}`)}},
			{Kind: node.LLMMessageStop, Stop: "tool_use"},
		})
	}
	a := &agentNode{
		cfg:        Config{MaxIterations: 2, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		tools:      []node.Tool{tool},
		llm:        &fakeLLM{scripts: scripts},
	}
	sink := &recordingSink{}
	_ = a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink)
	if sink.term.Type != node.StreamError {
		t.Fatalf("term: %+v", sink.term)
	}
	if !strings.Contains(string(sink.term.Data), "max-iterations") {
		t.Fatalf("data: %s", sink.term.Data)
	}
}

func TestAgentToolErrorSurfaceToLLM(t *testing.T) {
	tool := &fakeTool{name: "T", err: errors.New("boom"), policy: node.FailureSurfaceToLLM}
	a := &agentNode{
		cfg:        Config{MaxIterations: 3, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		tools:      []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "T", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "ok"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("expected complete, got %v: %s", sink.term.Type, sink.term.Data)
	}
	var sawErr bool
	for _, ev := range sink.events {
		if ev.Type == node.StreamToolResult && strings.Contains(string(ev.Data), `"isError":true`) {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatal("never saw tool_result with isError=true")
	}
}

func TestAgentToolErrorTerminate(t *testing.T) {
	tool := &fakeTool{name: "T", err: errors.New("boom"), policy: node.FailureTerminate}
	a := &agentNode{
		cfg:        Config{MaxIterations: 3, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		tools:      []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "T", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
		}},
	}
	sink := &recordingSink{}
	_ = a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink)
	if sink.term.Type != node.StreamError || !strings.Contains(string(sink.term.Data), "tool-error") {
		t.Fatalf("term: %+v", sink.term)
	}
}

func TestProcessEmitsAttachmentFromTool(t *testing.T) {
	tool := &attachmentEmittingTool{
		name:   "ChartTool",
		desc:   "render chart",
		out:    json.RawMessage(`{"status":"attached","chartId":"abc","kind":"quickchart"}`),
		attach: node.ToolAttachment{Kind: "quickchart", Payload: json.RawMessage(`{"view":"chart"}`)},
	}
	a := &agentNode{
		cfg:        Config{MaxIterations: 5, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		tools:      []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "ChartTool", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "done"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}

	if err := a.Process(context.Background(), node.AgentInput{Message: "draw chart"}, sink); err != nil {
		t.Fatalf("Process: %v", err)
	}

	var attachSeen bool
	for _, e := range sink.events {
		if e.Type == node.StreamAttachment {
			attachSeen = true
			if !strings.Contains(string(e.Data), `"kind":"quickchart"`) {
				t.Fatalf("attachment data missing kind: %s", string(e.Data))
			}
			if !strings.Contains(string(e.Data), `"toolUseId":`) {
				t.Fatalf("attachment must carry toolUseId: %s", string(e.Data))
			}
		}
	}
	if !attachSeen {
		t.Fatalf("no StreamAttachment event emitted")
	}
}

// attachmentEmittingTool is a stub Tool that calls Attach via the ctx-bound
// emitter once per Invoke, then returns its configured output.
type attachmentEmittingTool struct {
	name   string
	desc   string
	out    json.RawMessage
	attach node.ToolAttachment
}

func (s *attachmentEmittingTool) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "tool/stub", Role: node.RoleTool}
}
func (s *attachmentEmittingTool) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (s *attachmentEmittingTool) Close(_ context.Context) error                { return nil }
func (s *attachmentEmittingTool) FailurePolicy() node.FailurePolicy            { return node.FailureSurfaceToLLM }
func (s *attachmentEmittingTool) RetryPolicy() node.RetryPolicy                { return nil }
func (s *attachmentEmittingTool) ToolSpec() node.ToolSpec {
	return node.ToolSpec{Name: s.name, Description: s.desc, InputSchema: json.RawMessage(`{"type":"object"}`)}
}
func (s *attachmentEmittingTool) Invoke(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
	if e, ok := node.EmitterFromContext(ctx); ok {
		_ = e.Attach(ctx, s.attach)
	}
	return s.out, nil
}

// fakeClientTool implements node.ClientUITool. Invoke panics to verify the
// agent loop never calls it for a ClientOnly tool.
type fakeClientTool struct {
	name string
	spec node.ToolSpec
}

func (f *fakeClientTool) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "tool/test-client", Role: node.RoleTool}
}
func (f *fakeClientTool) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (f *fakeClientTool) Close(_ context.Context) error                { return nil }
func (f *fakeClientTool) FailurePolicy() node.FailurePolicy            { return "" }
func (f *fakeClientTool) RetryPolicy() node.RetryPolicy                { return nil }
func (f *fakeClientTool) ToolSpec() node.ToolSpec                      { return f.spec }
func (f *fakeClientTool) Invoke(_ context.Context, _ json.RawMessage) (json.RawMessage, error) {
	return nil, errors.New("Invoke must not be called for ClientUITool")
}
func (f *fakeClientTool) ClientOnly() bool { return true }

// clientResultEnv embeds testNodeEnv and overrides AwaitClientToolResult to
// return canned payloads keyed by toolCallID.
type clientResultEnv struct {
	testNodeEnv
	resp map[string][]byte
}

func (e *clientResultEnv) AwaitClientToolResult(_ context.Context, toolCallID string, _ time.Duration) ([]byte, error) {
	if b, ok := e.resp[toolCallID]; ok {
		return b, nil
	}
	return nil, errors.New("no canned response")
}

func TestAgentSuspendsOnClientOnlyToolAndResumesWithResult(t *testing.T) {
	clientTool := &fakeClientTool{
		name: "Confirm",
		spec: node.ToolSpec{Name: "Confirm", Description: "yes/no", InputSchema: json.RawMessage(`{}`)},
	}
	a := &agentNode{
		cfg:        Config{MaxIterations: 5, SystemMessage: "sys", HumanResponseTimeoutSeconds: 1},
		env:        &clientResultEnv{resp: map[string][]byte{"tu1": []byte(`{"approved":true}`)}},
		workflowID: "wf",
		tools:      []node.Tool{clientTool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "Confirm", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "done"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("expected complete, got %+v", sink.term)
	}
}

func TestAgentClientToolSkipBecomesSurfacedError(t *testing.T) {
	clientTool := &fakeClientTool{
		name: "Confirm",
		spec: node.ToolSpec{Name: "Confirm", Description: "yes/no", InputSchema: json.RawMessage(`{}`)},
	}
	a := &agentNode{
		cfg:        Config{MaxIterations: 5, SystemMessage: "sys", HumanResponseTimeoutSeconds: 1},
		env:        &clientResultEnv{resp: map[string][]byte{"tu1": []byte(`{"_skipped":true}`)}},
		workflowID: "wf",
		tools:      []node.Tool{clientTool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "Confirm", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "ok"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
		t.Fatalf("Process: %v", err)
	}
	var sawSkipErr bool
	for _, ev := range sink.events {
		if ev.Type == node.StreamToolResult &&
			strings.Contains(string(ev.Data), `"isError":true`) &&
			strings.Contains(string(ev.Data), "user declined to answer") {
			sawSkipErr = true
		}
	}
	if !sawSkipErr {
		t.Fatalf("expected tool_result with isError:true and 'user declined to answer'; events: %+v", sink.events)
	}
}

// retryingFakeTool fails for the first N invokes then succeeds.
type retryingFakeTool struct {
	name      string
	failsLeft int
	transient error
	out       json.RawMessage
	policy    *retry.Policy
	invoked   int
}

func (retryingFakeTool) Spec() node.NodeSpec                          { return node.NodeSpec{Role: node.RoleTool} }
func (retryingFakeTool) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (retryingFakeTool) Close(_ context.Context) error                { return nil }
func (f *retryingFakeTool) ToolSpec() node.ToolSpec {
	return node.ToolSpec{Name: f.name, Description: "x", InputSchema: json.RawMessage(`{}`)}
}
func (f *retryingFakeTool) FailurePolicy() node.FailurePolicy { return node.FailureSurfaceToLLM }
func (f *retryingFakeTool) RetryPolicy() node.RetryPolicy {
	if f.policy == nil {
		return nil
	}
	return f.policy
}
func (f *retryingFakeTool) Invoke(_ context.Context, _ json.RawMessage) (json.RawMessage, error) {
	f.invoked++
	if f.failsLeft > 0 {
		f.failsLeft--
		return nil, f.transient
	}
	return f.out, nil
}

func TestAgent_RetriesTransientToolError_ThenSucceeds(t *testing.T) {
	tool := &retryingFakeTool{
		name:      "T",
		failsLeft: 2,
		transient: errors.New("503: down"),
		out:       json.RawMessage(`{"ok":1}`),
		policy: &retry.Policy{
			MaxAttempts:  3,
			Backoff:      retry.BackoffFixed,
			InitialDelay: 0,
			RetryOn:      []retry.Class{retry.ClassTransient},
		},
	}
	a := &agentNode{
		cfg:        Config{MaxIterations: 3, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		tools:      []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "T", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "done"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "go", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if tool.invoked != 3 {
		t.Errorf("invoked: got %d want 3", tool.invoked)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("term: %+v", sink.term)
	}
}

// flakeyLLM returns preErr on the first call, then follows scripts.
type flakeyLLM struct {
	scripts [][]node.LLMEvent
	calls   int
	preErr  error
}

func (flakeyLLM) Spec() node.NodeSpec                          { return node.NodeSpec{Role: node.RoleLLM} }
func (flakeyLLM) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (flakeyLLM) Close(_ context.Context) error                { return nil }
func (l *flakeyLLM) Stream(_ context.Context, _ node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	if l.calls == 0 && l.preErr != nil {
		l.calls++
		// Match Bedrock's pattern: emit LLMError AND return err.
		out <- node.LLMEvent{Kind: node.LLMError, Error: l.preErr}
		return l.preErr
	}
	if l.calls >= len(l.scripts) {
		l.calls++
		return fmt.Errorf("flakeyLLM: no more scripts")
	}
	for _, ev := range l.scripts[l.calls] {
		out <- ev
	}
	l.calls++
	return nil
}

// policyEnv extends testNodeEnv to expose a retry policy from RetryPolicy().
type policyEnv struct {
	testNodeEnv
	policy *retry.Policy
}

func (e *policyEnv) RetryPolicy() node.RetryPolicy {
	if e.policy == nil {
		return nil
	}
	return e.policy
}

func TestAgent_LLM_RetriesPreStreamError(t *testing.T) {
	envWithPolicy := &policyEnv{
		policy: &retry.Policy{
			MaxAttempts:  2,
			Backoff:      retry.BackoffFixed,
			InitialDelay: 0,
			RetryOn:      []retry.Class{retry.ClassTransient},
		},
	}
	llm := &flakeyLLM{
		preErr: errors.New("500: bedrock error"),
		scripts: [][]node.LLMEvent{
			nil, // attempt 0 uses preErr
			{
				{Kind: node.LLMTextDelta, Delta: "hi"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		},
	}
	a := &agentNode{
		cfg:        Config{MaxIterations: 3, SystemMessage: "sys"},
		env:        envWithPolicy,
		workflowID: "wf",
		llm:        llm,
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "x", SessionID: "s"}, sink); err != nil {
		t.Fatal(err)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("term: %+v", sink.term)
	}
	if llm.calls < 2 {
		t.Errorf("calls: got %d want >=2", llm.calls)
	}
}

func TestAgent_LLM_NoPolicy_AbortsAsBefore(t *testing.T) {
	// Sanity: when no retry policy is set, behavior matches today — LLM error
	// aborts the turn immediately.
	llm := &flakeyLLM{
		preErr: errors.New("500: bedrock error"),
	}
	a := &agentNode{
		cfg:        Config{MaxIterations: 3, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		llm:        llm,
	}
	sink := &recordingSink{}
	_ = a.Process(context.Background(), node.AgentInput{Message: "x", SessionID: "s"}, sink)
	if sink.term.Type != node.StreamError {
		t.Fatalf("expected StreamError, got %+v", sink.term)
	}
	if !strings.Contains(string(sink.term.Data), "llm-error") {
		t.Errorf("expected llm-error code; got %s", sink.term.Data)
	}
	if llm.calls != 1 {
		t.Errorf("calls: got %d want 1 (no retry policy)", llm.calls)
	}
}

func TestAgent_RetryExhausted_SurfacesToLLMWithAttemptsExhausted(t *testing.T) {
	tool := &retryingFakeTool{
		name:      "T",
		failsLeft: 99,
		transient: errors.New("503: down"),
		policy: &retry.Policy{
			MaxAttempts:  3,
			Backoff:      retry.BackoffFixed,
			InitialDelay: 0,
			RetryOn:      []retry.Class{retry.ClassTransient},
		},
	}
	a := &agentNode{
		cfg:        Config{MaxIterations: 3, SystemMessage: "sys"},
		env:        testNodeEnv{},
		workflowID: "wf",
		tools:      []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "T", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "ok"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "go", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if tool.invoked != 3 {
		t.Errorf("invoked: got %d want 3", tool.invoked)
	}
	var sawExhausted bool
	for _, ev := range sink.events {
		if ev.Type == node.StreamToolResult &&
			strings.Contains(string(ev.Data), `"attemptsExhausted":3`) &&
			strings.Contains(string(ev.Data), `"code":"transient"`) {
			sawExhausted = true
		}
	}
	if !sawExhausted {
		t.Fatalf("expected payload with attemptsExhausted:3 and code:transient; events: %+v", sink.events)
	}
}

func TestAgent_EmptyEndTurn_RepromptsForFinalAnswer(t *testing.T) {
	// The model ends its first turn with no text (as it does after a tool error
	// it gives up on). The agent must re-prompt instead of completing blank,
	// which the client would render as a truncated, empty conversation.
	a := &agentNode{
		cfg:                   Config{MaxIterations: 5, SystemMessage: "sys"},
		env:                   testNodeEnv{},
		workflowID:            "wf",
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{ // 1st turn: empty end_turn (no text block)
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
			{ // 2nd turn: real answer after the nudge
				{Kind: node.LLMTextDelta, Delta: "Here is the recovered answer"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("expected complete, got %+v", sink.term)
	}
	if !strings.Contains(string(sink.term.Data), "Here is the recovered answer") {
		t.Fatalf("final text should be the recovered answer, got: %s", sink.term.Data)
	}
	if llm := a.llm.(*fakeLLM); llm.calls != 2 {
		t.Errorf("expected 2 LLM calls (initial + 1 reprompt), got %d", llm.calls)
	}
}

func TestAgent_EmptyEndTurn_GivesUpAfterMaxRetries(t *testing.T) {
	// The model NEVER produces text. The agent must stop re-prompting after
	// the configured budget and complete gracefully — not loop forever, not
	// surface an error.
	retries := defaultMaxEmptyAnswerRetries
	scripts := [][]node.LLMEvent{}
	for i := 0; i < retries+2; i++ {
		scripts = append(scripts, []node.LLMEvent{{Kind: node.LLMMessageStop, Stop: "end_turn"}})
	}
	a := &agentNode{
		cfg:                   Config{MaxIterations: retries + 5, SystemMessage: "sys"},
		env:                   testNodeEnv{},
		workflowID:            "wf",
		maxEmptyAnswerRetries: retries,
		llm:                   &fakeLLM{scripts: scripts},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("expected complete (graceful give-up), got %+v", sink.term)
	}
	// initial attempt + `retries` reprompts, then it gives up.
	if llm := a.llm.(*fakeLLM); llm.calls != retries+1 {
		t.Errorf("expected %d LLM calls, got %d", retries+1, llm.calls)
	}
}

func TestAgent_EmptyEndTurn_WhitespaceOnlyCountsAsEmpty(t *testing.T) {
	// A turn whose only "answer" is whitespace is just as useless as an empty
	// one — it must trigger a re-prompt, not complete with blank-looking text.
	a := &agentNode{
		cfg:                   Config{MaxIterations: 5, SystemMessage: "sys"},
		env:                   testNodeEnv{},
		workflowID:            "wf",
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMTextDelta, Delta: "  \n\t "},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "real answer"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sink.term.Data), "real answer") {
		t.Fatalf("expected re-prompt to recover a real answer, got: %s", sink.term.Data)
	}
	if llm := a.llm.(*fakeLLM); llm.calls != 2 {
		t.Errorf("expected 2 LLM calls, got %d", llm.calls)
	}
}

func TestAgent_RecoversByRetryingToolAfterEmptyAnswer(t *testing.T) {
	// The realistic recovery path: a tool fails, the model ends empty, gets
	// nudged, RE-CALLS the (now-succeeding) tool, and answers. End to end.
	tool := &retryingFakeTool{
		name:      "QuickChart",
		failsLeft: 1, // first invoke fails, second succeeds
		transient: errors.New("tool/quickchart: chart render failed (status 400): invalid config"),
		out:       json.RawMessage(`{"imageUrl":"https://signed/chart.png","type":"line"}`),
	}
	a := &agentNode{
		cfg:                   Config{MaxIterations: 8, SystemMessage: "sys"},
		env:                   testNodeEnv{},
		workflowID:            "wf",
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		maxToolRetries:        defaultMaxToolRetries,
		tools:                 []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{ // turn0: call chart tool -> fails (surfaced)
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "QuickChart", InputJSON: []byte(`{"type":"line","data":{}}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{ // turn1: model gives up with empty text -> nudge
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
			{ // turn2: model fixes config and re-calls -> succeeds
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu2", Name: "QuickChart", InputJSON: []byte(`{"type":"line","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{ // turn3: final answer
				{Kind: node.LLMTextDelta, Delta: "Here is the chart and analysis"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "chart traffic", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("expected complete, got %+v", sink.term)
	}
	if !strings.Contains(string(sink.term.Data), "Here is the chart and analysis") {
		t.Fatalf("expected recovered final answer, got: %s", sink.term.Data)
	}
	if tool.invoked != 2 {
		t.Errorf("expected tool invoked twice (fail then retry-success), got %d", tool.invoked)
	}
	// The first invoke must have surfaced an error to the model.
	var sawToolErr bool
	for _, ev := range sink.events {
		if ev.Type == node.StreamToolResult && strings.Contains(string(ev.Data), `"isError":true`) {
			sawToolErr = true
		}
	}
	if !sawToolErr {
		t.Error("expected a surfaced tool error before recovery")
	}
}

func TestAgent_EmptyAnswerNudge_NotPersistedToMemory(t *testing.T) {
	// The recovery nudge is an internal control message — it must never leak
	// into persisted conversation memory.
	mem := &fakeMem{}
	a := &agentNode{
		cfg:                   Config{MaxIterations: 5, SystemMessage: "sys"},
		env:                   testNodeEnv{},
		workflowID:            "wf",
		mem:                   mem,
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{{Kind: node.LLMMessageStop, Stop: "end_turn"}},
			{
				{Kind: node.LLMTextDelta, Delta: "recovered answer"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi there", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if len(mem.turns) != 1 {
		t.Fatalf("expected 1 memory turn, got %d", len(mem.turns))
	}
	uText := lastText(mem.turns[0].User)
	aText := lastText(mem.turns[0].Assistant)
	if !strings.Contains(uText, "hi there") {
		t.Errorf("memory user msg should be the original, got %q", uText)
	}
	if strings.Contains(uText, "ended your turn") || strings.Contains(aText, "ended your turn") ||
		strings.Contains(aText, "(no answer)") {
		t.Errorf("nudge/placeholder leaked into memory: user=%q assistant=%q", uText, aText)
	}
	if !strings.Contains(aText, "recovered answer") {
		t.Errorf("memory assistant should be the recovered answer, got %q", aText)
	}
}

func TestResolveMaxEmptyAnswerRetries(t *testing.T) {
	cases := []struct {
		val  string
		want int
	}{
		{"", defaultMaxEmptyAnswerRetries},    // empty/unset -> default
		{"0", 0},                              // floor: disables the guard
		{"2", 2},                              // in range
		{"5", 5},                              // at the cap
		{"7", maxEmptyAnswerRetriesCap},       // above cap -> clamped to 5
		{"999", maxEmptyAnswerRetriesCap},     // far above cap -> 5
		{"-3", 0},                             // negative -> 0
		{"abc", defaultMaxEmptyAnswerRetries}, // unparseable -> default
		{" 4 ", 4},                            // whitespace trimmed
	}
	for _, tc := range cases {
		t.Run(tc.val, func(t *testing.T) {
			t.Setenv(envMaxEmptyAnswerRetries, tc.val)
			if got := resolveMaxEmptyAnswerRetries(); got != tc.want {
				t.Errorf("resolveMaxEmptyAnswerRetries() with %q = %d, want %d", tc.val, got, tc.want)
			}
		})
	}
}

func TestAgent_ToolError_SurfacesRecoveryHintWithoutPrompt(t *testing.T) {
	// SystemMessage has NO recovery guidance — the recovery instructions must
	// come from CODE (embedded in the tool_result), so recovery still works if
	// the DB-managed system prompt is edited to remove its guidance.
	tool := &fakeTool{
		name:   "T",
		err:    errors.New("tool/quickchart: chart render failed (status 400)"),
		policy: node.FailureSurfaceToLLM,
	}
	a := &agentNode{
		cfg:                   Config{MaxIterations: 3, SystemMessage: "sys"},
		env:                   testNodeEnv{},
		workflowID:            "wf",
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		tools:                 []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "T", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{
				{Kind: node.LLMTextDelta, Delta: "ok"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	var sawHint bool
	for _, ev := range sink.events {
		if ev.Type == node.StreamToolResult &&
			strings.Contains(string(ev.Data), `"recovery"`) &&
			strings.Contains(string(ev.Data), "Never end your turn without a written answer") {
			sawHint = true
		}
	}
	if !sawHint {
		t.Fatalf("tool_result must embed a code-level recovery hint; events: %+v", sink.events)
	}
}

func TestExtractChartKeys(t *testing.T) {
	cases := []struct {
		name, in string
		want     []string
	}{
		{"tool imageUrl", `{"imageUrl":"https://x.s3.amazonaws.com/chart/2eaa8d72-f33c-4b62-aafe-3d2a17141eda.png?X-Amz-Algorithm=AWS4","type":"bar"}`, []string{"2eaa8d72-f33c-4b62-aafe-3d2a17141eda"}},
		{"model markdown", `Here it is ![chart](https://x/chart/abc12345-1111-2222-3333-444455556666.png?sig) done`, []string{"abc12345-1111-2222-3333-444455556666"}},
		{"none", "no chart here, just text", nil},
		{"dedup", "chart/11111111-1111-1111-1111-111111111111.png chart/11111111-1111-1111-1111-111111111111.png", []string{"11111111-1111-1111-1111-111111111111"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractChartKeys(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("got[%d]=%q want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestExtractChartRefs_JSONEscaped guards the bug where a presigned URL scraped
// from a tool result kept its "&" query separators as Go's HTML-escaped &,
// so the canonical URL substituted into the answer 404'd in the browser.
// extractChartRefs must hand back a decoded URL. The input is built via the
// same json.Marshal the quickchart tool uses, so it carries the real escaping.
func TestExtractChartRefs_JSONEscaped(t *testing.T) {
	key := "d035c7ae-bf49-45ba-a2fa-a1891ad600c3"
	url := "https://b.s3.amazonaws.com/chart/" + key +
		".png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=604800&X-Amz-Signature=abc123"
	b, err := json.Marshal(map[string]string{"imageUrl": url})
	if err != nil {
		t.Fatal(err)
	}
	got := extractChartRefs(string(b))
	if len(got) != 1 {
		t.Fatalf("got %d refs, want 1: %v", len(got), got)
	}
	if got[0].key != key {
		t.Errorf("key = %q, want %q", got[0].key, key)
	}
	if got[0].url != url {
		t.Errorf("url not decoded:\n got  %q\n want %q", got[0].url, url)
	}
}

// extractChartRefs must match the new relative short URL form the redirect
// produces (aichatviewer/chart/<uuid>.png), not just the legacy https:// form.
func TestExtractChartRefs_ShortURL(t *testing.T) {
	key := "eeeeeeee-5555-5555-5555-555555555555"
	b, _ := json.Marshal(map[string]string{
		"imageUrl": "aichatviewer/chart/" + key + ".png",
		"type":     "bar",
	})
	// URL has no escapable chars so unescapeJSONURL is a no-op; the JSON
	// wrapping mirrors how tool results actually arrive (key "imageUrl").
	got := extractChartRefs(string(b))
	if len(got) != 1 {
		t.Fatalf("want 1 ref, got %d (%v)", len(got), got)
	}
	if got[0].key != key {
		t.Errorf("key=%q want %q", got[0].key, key)
	}
	if got[0].url != "aichatviewer/chart/"+key+".png" {
		t.Errorf("url=%q want relative short url", got[0].url)
	}
}

// TestUnescapeJSONURL proves the decode is general, not ampersand-specific:
// Go's HTML-safe json.Marshal escapes ampersand, less-than AND greater-than, and
// unescapeJSONURL must resolve all of them (plus pass clean URLs through and fall
// back on undecodable input). Inputs are built via json.Marshal so the escaped
// forms are exactly what the tool emits — never hand-typed.
func TestUnescapeJSONURL(t *testing.T) {
	// A URL exercising all three HTML-escaped bytes: & < >
	raw := "https://b/chart/aaaaaaaa-1111-2222-3333-444444444444.png?a=1&b=2&lt=<&gt=>"
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	escaped := string(b[1 : len(b)-1]) // strip the surrounding quotes -> the scraped fragment
	if escaped == raw {
		t.Fatalf("test setup: expected json.Marshal to escape &/</> in %q", raw)
	}
	if got := unescapeJSONURL(escaped); got != raw {
		t.Errorf("did not fully decode:\n got  %q\n want %q", got, raw)
	}

	// Clean URL (no backslash) is returned untouched.
	clean := "https://b/chart/bbbbbbbb-2222-2222-2222-222222222222.png?x=1"
	if got := unescapeJSONURL(clean); got != clean {
		t.Errorf("clean url changed: got %q", got)
	}

	// Undecodable fragment (lone trailing backslash) falls back to the raw input.
	bad := `https://b/chart/cccccccc.png?x=1\`
	if got := unescapeJSONURL(bad); got != bad {
		t.Errorf("undecodable fragment should fall back: got %q", got)
	}
}

// TestAppendMissingCharts guards the multi-chart bug: when the model embeds a
// chart inline and then calls another tool, that chart lands in a reasoning
// segment, not the final answer — so a "two charts" request rendered only the
// last one. Every produced chart absent from the final answer must be appended.
func TestAppendMissingCharts(t *testing.T) {
	k1 := "aaaaaaaa-1111-1111-1111-111111111111"
	u1 := "https://s3/chart/" + k1 + ".png?a"
	k2 := "bbbbbbbb-2222-2222-2222-222222222222"
	u2 := "https://s3/chart/" + k2 + ".png?b"
	produced := []chartRef{{key: k1, url: u1}, {key: k2, url: u2}}

	t.Run("first chart stranded in reasoning is appended", func(t *testing.T) {
		// final answer references only chart 2 (chart 1 ended up in reasoning)
		final := "Here is the hourly chart:\n![chart](" + u2 + ")\n\nSummary: top vs bottom."
		got, n := appendMissingCharts(final, produced)
		if n != 1 {
			t.Fatalf("appended %d, want 1", n)
		}
		if !strings.Contains(got, u1) || !strings.Contains(got, u2) {
			t.Errorf("both charts must be present: %q", got)
		}
	})
	t.Run("all charts already present -> unchanged", func(t *testing.T) {
		final := "![chart](" + u1 + ") and ![chart](" + u2 + ")"
		got, n := appendMissingCharts(final, produced)
		if n != 0 || got != final {
			t.Errorf("expected no-op, got n=%d text=%q", n, got)
		}
	})
	t.Run("no charts produced -> unchanged", func(t *testing.T) {
		if got, n := appendMissingCharts("plain answer", nil); n != 0 || got != "plain answer" {
			t.Errorf("expected no-op, got n=%d text=%q", n, got)
		}
	})
	t.Run("empty answer -> charts without leading separator", func(t *testing.T) {
		got, n := appendMissingCharts("", []chartRef{{key: k1, url: u1}})
		if n != 1 || got != "![chart]("+u1+")" {
			t.Errorf("got n=%d text=%q", n, got)
		}
	})
}

func TestCorrectChartRefs(t *testing.T) {
	k1 := "aaaaaaaa-1111-1111-1111-111111111111"
	u1 := "https://s3/chart/" + k1 + ".png?sigA"
	k2 := "bbbbbbbb-2222-2222-2222-222222222222"
	u2 := "https://s3/chart/" + k2 + ".png?sigB"
	fakeURL := "https://s3/chart/cccccccc-3333-3333-3333-333333333333.png?fake"

	t.Run("key matches -> canonical url", func(t *testing.T) {
		in := "see ![chart](https://s3/chart/" + k1 + ".png?STALE) done"
		got := correctChartRefs(in, []chartRef{{key: k1, url: u1}})
		if !strings.Contains(got, u1) || strings.Contains(got, "STALE") {
			t.Errorf("expected canonical url, got %q", got)
		}
	})
	t.Run("short-form key matches -> canonical short url", func(t *testing.T) {
		sk := "dddddddd-4444-4444-4444-444444444444"
		su := "aichatviewer/chart/" + sk + ".png"
		in := "here ![chart](aichatviewer/chart/" + sk + ".png) ok"
		got := correctChartRefs(in, []chartRef{{key: sk, url: su}})
		if !strings.Contains(got, "![chart]("+su+")") {
			t.Errorf("short-form chart should be kept/canonicalized, got %q", got)
		}
	})
	t.Run("single produced, fabricated ref -> substitute", func(t *testing.T) {
		in := "here ![chart](" + fakeURL + ")"
		got := correctChartRefs(in, []chartRef{{key: k1, url: u1}})
		if !strings.Contains(got, u1) || strings.Contains(got, "cccccccc") {
			t.Errorf("expected fabricated url replaced with the single produced chart, got %q", got)
		}
	})
	t.Run("multi produced, fabricated ref -> left for gray box", func(t *testing.T) {
		in := "a ![a](https://s3/chart/" + k1 + ".png?x) b ![b](" + fakeURL + ")"
		got := correctChartRefs(in, []chartRef{{key: k1, url: u1}, {key: k2, url: u2}})
		if !strings.Contains(got, u1) {
			t.Errorf("valid ref should be kept (canonical): %q", got)
		}
		if !strings.Contains(got, fakeURL) || strings.Contains(got, "chart unavailable") {
			t.Errorf("fabricated ref in multi-chart must be LEFT unchanged (client gray box), got %q", got)
		}
	})
	t.Run("no charts produced -> left for gray box", func(t *testing.T) {
		in := "look ![chart](" + fakeURL + ")"
		got := correctChartRefs(in, nil)
		if got != in {
			t.Errorf("fabricated ref with zero produced must be left unchanged, got %q", got)
		}
	})
	t.Run("short-form, no charts produced -> left for gray box", func(t *testing.T) {
		shortFake := "aichatviewer/chart/ffffffff-9999-9999-9999-999999999999.png"
		in := "look ![chart](" + shortFake + ")"
		got := correctChartRefs(in, nil)
		if got != in {
			t.Errorf("short-form fabricated ref with zero produced must be left unchanged, got %q", got)
		}
	})
	t.Run("no chart image -> unchanged", func(t *testing.T) {
		in := "just text, no chart"
		if got := correctChartRefs(in, []chartRef{{key: k1, url: u1}}); got != in {
			t.Errorf("text without a chart image must be unchanged, got %q", got)
		}
	})
}

func TestAgent_ChartTrace_FlagsFabricatedKey(t *testing.T) {
	// Tool produces keyA; the model's final answer references keyB (a key never
	// produced). The chart-trace must log keyA as produced and WARN that keyB
	// was referenced-but-not-produced — exactly the fabrication signal.
	const keyA = "11111111-1111-1111-1111-111111111111"
	const keyB = "22222222-2222-2222-2222-222222222222"
	tool := &fakeTool{
		name: "QuickChart",
		out:  json.RawMessage(`{"imageUrl":"https://x.s3.amazonaws.com/chart/` + keyA + `.png?sig","type":"bar"}`),
	}
	buf := &bytes.Buffer{}
	a := &agentNode{
		cfg:                   Config{MaxIterations: 5, SystemMessage: "sys"},
		env:                   &capturingEnv{buf: buf},
		workflowID:            "wf",
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		maxToolRetries:        defaultMaxToolRetries,
		tools:                 []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{ // turn0: actually render a chart (produces keyA)
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "QuickChart", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{ // turn1: final answer references a DIFFERENT (fabricated) key -> WARNING + forced re-call
				{Kind: node.LLMTextDelta, Delta: "Here it is ![chart](https://x.s3.amazonaws.com/chart/" + keyB + ".png?sig)"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
			{ // turn2: forced recall round — model produces nothing new; prose discarded, turn1 answer restored
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "chart it", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "produced chart key="+keyA) {
		t.Errorf("expected produced-key trace for %s; logs:\n%s", keyA, logs)
	}
	if !strings.Contains(logs, "WARNING model referenced chart key "+keyB) {
		t.Errorf("expected fabrication WARNING for %s; logs:\n%s", keyB, logs)
	}
}

func TestResolveMaxToolRetries(t *testing.T) {
	cases := []struct {
		val  string
		want int
	}{
		{"", defaultMaxToolRetries}, // empty/unset -> default
		{"0", 0},                    // floor: one attempt, no retry
		{"2", 2},                    // in range
		{"10", 10},                  // at the cap
		{"15", maxToolRetriesCap},   // above cap -> clamped to 10
		{"-1", 0},                   // negative -> 0
		{"abc", defaultMaxToolRetries},
		{" 4 ", 4}, // whitespace trimmed
	}
	for _, tc := range cases {
		t.Run(tc.val, func(t *testing.T) {
			t.Setenv(envMaxToolRetries, tc.val)
			if got := resolveMaxToolRetries(); got != tc.want {
				t.Errorf("resolveMaxToolRetries() with %q = %d, want %d", tc.val, got, tc.want)
			}
		})
	}
}

func TestAgent_ToolRetryCap_BlocksAfterCap(t *testing.T) {
	// A tool that ALWAYS errors. With maxToolRetries=1 the loop allows 2 real
	// invocations (1 + 1 retry), then refuses to invoke it again and steers the
	// model to answer in text. The conversation still completes.
	tool := &fakeTool{
		name:   "QuickChart",
		err:    errors.New("tool/quickchart: chart render failed (status 400)"),
		policy: node.FailureSurfaceToLLM,
	}
	a := &agentNode{
		cfg:                   Config{MaxIterations: 10, SystemMessage: "sys"},
		env:                   testNodeEnv{},
		workflowID:            "wf",
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		maxToolRetries:        1,
		tools:                 []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{ // attempt 1
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "QuickChart", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{ // attempt 2 (the 1 allowed retry)
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu2", Name: "QuickChart", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{ // attempt 3 -> blocked by the cap, not invoked
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu3", Name: "QuickChart", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{ // model gives the text fallback
				{Kind: node.LLMTextDelta, Delta: "I couldn't render the chart; here are the values in text."},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "chart it", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("expected complete, got %+v", sink.term)
	}
	if tool.invoked != 2 {
		t.Errorf("tool should be invoked exactly 2 times (1 + 1 retry) then capped, got %d", tool.invoked)
	}
	var sawCap bool
	for _, ev := range sink.events {
		if ev.Type == node.StreamToolResult &&
			strings.Contains(string(ev.Data), `"retry-cap-exceeded"`) &&
			strings.Contains(string(ev.Data), `"isError":true`) {
			sawCap = true
		}
	}
	if !sawCap {
		t.Fatalf("expected a retry-cap-exceeded tool_result once the cap was hit; events: %+v", sink.events)
	}
}

func TestMissingChartKeys(t *testing.T) {
	if got := missingChartKeys([]string{"a", "b"}, []string{"a"}); len(got) != 1 || got[0] != "b" {
		t.Errorf("want [b], got %v", got)
	}
	if got := missingChartKeys([]string{"a"}, []string{"a"}); len(got) != 0 {
		t.Errorf("want none, got %v", got)
	}
	if got := missingChartKeys(nil, []string{"a"}); len(got) != 0 {
		t.Errorf("want none for no references, got %v", got)
	}
	if got := missingChartKeys([]string{"x", "y"}, nil); len(got) != 2 {
		t.Errorf("want both missing when none produced, got %v", got)
	}
}

func TestAgent_ForcesRecallForUnproducedChart(t *testing.T) {
	const fabricated = "11111111-1111-1111-1111-111111111111" // model invented this; never produced
	const real = "22222222-2222-2222-2222-222222222222"       // the chart QuickChart actually makes
	const prose = "Sales are up. Here is the chart "
	tool := &fakeTool{
		name: "QuickChart",
		out:  json.RawMessage(`{"imageUrl":"aichatviewer/chart/` + real + `.png","type":"bar"}`),
	}
	buf := &bytes.Buffer{}
	a := &agentNode{
		cfg:                   Config{MaxIterations: 6, SystemMessage: "sys"},
		env:                   &capturingEnv{buf: buf},
		workflowID:            "wf",
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		maxToolRetries:        defaultMaxToolRetries,
		tools:                 []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{ // turn0: stream the answer the user reads, with a FABRICATED chart link, no tool call
				{Kind: node.LLMTextDelta, Delta: prose + "![chart](aichatviewer/chart/" + fabricated + ".png)"},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
			{ // turn1: respond to the nudge by actually calling QuickChart (events suppressed from client)
				{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "QuickChart", InputJSON: []byte(`{}`)}},
				{Kind: node.LLMMessageStop, Stop: "tool_use"},
			},
			{ // turn2: throwaway closing prose — must be DISCARDED; we keep the turn0 answer
				{Kind: node.LLMTextDelta, Delta: "Done."},
				{Kind: node.LLMMessageStop, Stop: "end_turn"},
			},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "chart it", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "forcing one QuickChart re-call for unproduced chart key "+fabricated) {
		t.Errorf("expected force-recall trace for %s; logs:\n%s", fabricated, logs)
	}
	// The recall round's QuickChart tool_call must be SUPPRESSED from the client.
	for _, e := range sink.events {
		if e.Type == node.StreamToolCall {
			t.Errorf("forced-recall tool_call must be suppressed from the client, but a StreamToolCall was sent")
		}
	}
	// completeStream emits the terminal frame via sink.Close with {"finalText": lastText(finalMsg)}.
	final := sink.term
	if final.Type == "" {
		t.Fatal("no terminal complete frame")
	}
	var done struct {
		FinalText string `json:"finalText"`
	}
	if err := json.Unmarshal(final.Data, &done); err != nil {
		t.Fatalf("decode complete: %v", err)
	}
	if !strings.Contains(done.FinalText, prose) {
		t.Errorf("final answer must preserve the prose the user already read; got %q", done.FinalText)
	}
	if !strings.Contains(done.FinalText, real) {
		t.Errorf("final answer should reference the produced chart %s, got %q", real, done.FinalText)
	}
	if strings.Contains(done.FinalText, fabricated) {
		t.Errorf("final answer must not reference the fabricated chart %s, got %q", fabricated, done.FinalText)
	}
	if strings.Contains(done.FinalText, "Done.") {
		t.Errorf("discarded recall-round prose must not appear in the final answer; got %q", done.FinalText)
	}
}

func TestAgent_ToolRetryCap_AllowsRecoveryWithinCap(t *testing.T) {
	// The cap must NOT block a legitimate fix-then-succeed recovery: with the
	// default cap (3) a tool may fail twice and still be retried into success.
	tool := &retryingFakeTool{
		name:      "QuickChart",
		failsLeft: 2, // fail invoke 1 & 2, succeed on invoke 3
		transient: errors.New("tool/quickchart: chart render failed (status 400)"),
		out:       json.RawMessage(`{"imageUrl":"https://signed/chart.png","type":"bar"}`),
	}
	a := &agentNode{
		cfg:                   Config{MaxIterations: 10, SystemMessage: "sys"},
		env:                   testNodeEnv{},
		workflowID:            "wf",
		maxEmptyAnswerRetries: defaultMaxEmptyAnswerRetries,
		maxToolRetries:        defaultMaxToolRetries, // 3
		tools:                 []node.Tool{tool},
		llm: &fakeLLM{scripts: [][]node.LLMEvent{
			{{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "t1", Name: "QuickChart", InputJSON: []byte(`{}`)}}, {Kind: node.LLMMessageStop, Stop: "tool_use"}},
			{{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "t2", Name: "QuickChart", InputJSON: []byte(`{}`)}}, {Kind: node.LLMMessageStop, Stop: "tool_use"}},
			{{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "t3", Name: "QuickChart", InputJSON: []byte(`{}`)}}, {Kind: node.LLMMessageStop, Stop: "tool_use"}},
			{{Kind: node.LLMTextDelta, Delta: "Chart rendered."}, {Kind: node.LLMMessageStop, Stop: "end_turn"}},
		}},
	}
	sink := &recordingSink{}
	if err := a.Process(context.Background(), node.AgentInput{Message: "chart it", SessionID: "s1"}, sink); err != nil {
		t.Fatal(err)
	}
	if sink.term.Type != node.StreamComplete {
		t.Fatalf("expected complete, got %+v", sink.term)
	}
	if tool.invoked != 3 {
		t.Errorf("tool should be invoked 3 times (2 fails + success) within the cap, got %d", tool.invoked)
	}
	if !strings.Contains(string(sink.term.Data), "Chart rendered.") {
		t.Errorf("expected successful recovery answer, got %s", sink.term.Data)
	}
}
