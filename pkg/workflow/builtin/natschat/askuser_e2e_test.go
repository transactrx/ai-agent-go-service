package natschat_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/agent"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/natschat"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// e2eLLM records the tool names of every request. When request_user_input is
// offered it calls it once, then answers with the tool result it got.
type e2eLLM struct {
	mu    sync.Mutex
	tools [][]string
}

func (*e2eLLM) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/llm", Role: node.RoleLLM,
		OutputPorts: []node.PortSpec{{Name: node.PortAILanguageModel, Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (*e2eLLM) Init(context.Context, node.NodeEnv) error { return nil }
func (*e2eLLM) Close(context.Context) error              { return nil }
func (l *e2eLLM) Stream(_ context.Context, req node.LLMRequest, out chan<- node.LLMEvent) error {
	defer close(out)
	names := []string{}
	for _, t := range req.Tools {
		names = append(names, t.Name)
	}
	l.mu.Lock()
	l.tools = append(l.tools, names)
	l.mu.Unlock()
	last := req.Messages[len(req.Messages)-1]
	for _, b := range last.Content {
		if b.Type == node.BlockToolResult {
			out <- node.LLMEvent{Kind: node.LLMTextDelta, Delta: "got " + string(b.ToolResult)}
			out <- node.LLMEvent{Kind: node.LLMMessageStop, Stop: "end_turn"}
			return nil
		}
	}
	for _, n := range names {
		if n == "request_user_input" {
			out <- node.LLMEvent{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: n, InputJSON: []byte(`{"title":"Which?"}`)}}
			out <- node.LLMEvent{Kind: node.LLMMessageStop, Stop: "tool_use"}
			return nil
		}
	}
	out <- node.LLMEvent{Kind: node.LLMTextDelta, Delta: "plain"}
	out <- node.LLMEvent{Kind: node.LLMMessageStop, Stop: "end_turn"}
	return nil
}

func (l *e2eLLM) takeTools() [][]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.tools
	l.tools = nil
	return out
}

// e2eWorkflow takes the trigger's mode config: plain streaming, or the
// production shape (single + allowResponseModeOverride, the bridge asks for
// streaming in the body).
const e2eWorkflow = `{
  "id": "AskWf", "version": 1, "trigger": "trig",
  "nodes": [
    {"id": "trig",  "type": "trigger/nats-chat", "config": {"subject": "AskWf", %s, "requestTimeoutSeconds": 20}},
    {"id": "agent", "type": "ai/agent", "config": {"systemMessage": "sys", "humanResponseTimeoutSeconds": 5}},
    {"id": "llm",   "type": "test/llm", "config": {}}
  ],
  "connections": [
    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}},
    {"from": {"node": "llm", "port": "ai_languageModel"}, "to": {"node": "agent", "port": "ai_languageModel"}}
  ]
}`

// run sends one streaming chat request and answers request_user_input with
// answer. It returns the tool calls seen and the final text.
func run(t *testing.T, nc *nats.Conn, subject, body string, answer string) (calls []string, final string) {
	t.Helper()
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	msg := &nats.Msg{Subject: subject, Reply: inbox, Header: nats.Header{}, Data: []byte(body)}
	msg.Header.Set("X-Account-Id", "a1")
	msg.Header.Set("X-User-Id", "u1")
	msg.Header.Set(natsstream.HdrMessageId, uuid.NewString())
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
	var prefix string
	for {
		m, err := sub.NextMsg(10 * time.Second)
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		if p := m.Header.Get(natsstream.HdrStreamToolResultPrefix); p != "" {
			prefix = p
		}
		switch natsstream.StreamEventType(m.Header.Get(natsstream.HdrStreamEvent)) {
		case natsstream.StreamToolCall:
			var p struct {
				ToolUseID string `json:"toolUseId"`
				Name      string `json:"name"`
			}
			_ = json.Unmarshal(m.Data, &p)
			calls = append(calls, p.Name)
			if err := nc.Publish(prefix+"."+p.ToolUseID, []byte(answer)); err != nil {
				t.Fatal(err)
			}
		case natsstream.StreamComplete:
			var p struct {
				FinalText string `json:"finalText"`
			}
			_ = json.Unmarshal(m.Data, &p)
			return calls, p.FinalText
		case natsstream.StreamError:
			t.Fatalf("error event: %s", m.Data)
		}
	}
}

// Real trigger -> executor -> agent loop: request_user_input reaches the
// loop only on an enabled endpoint for a request with askUser; its call
// takes the client path (tool_call, result on the tool-result subject).
func TestAskUserReachesAgentLoop(t *testing.T) {
	t.Run("streaming", func(t *testing.T) { askUserReachesAgentLoop(t, `"responseMode": "streaming"`, "") })
	t.Run("override", func(t *testing.T) {
		askUserReachesAgentLoop(t, `"responseMode": "single", "allowResponseModeOverride": true`, `"responseMode":"streaming",`)
	})
}

func askUserReachesAgentLoop(t *testing.T, modeCfg, modeBody string) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	defer srv.Shutdown()
	t.Setenv("IDT_VALIDATION", "false")
	ns, err := nats_service.NewLowLevel("test.e2e", "q", srv.ClientURL(), "", "", 2048, 300*1024)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	llm := &e2eLLM{}
	reg := node.NewRegistry()
	_ = reg.Register("trigger/nats-chat", natschat.Factory)
	_ = reg.Register("ai/agent", agent.Factory)
	_ = reg.Register("test/llm", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return llm, nil }))
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wf.json"), []byte(fmt.Sprintf(e2eWorkflow, modeCfg)), 0o644); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(engine.Config{
		Source: engine.NewFilesystemSource(dir), Registry: reg, Logger: log.New(io.Discard, "", 0),
		Hosts: map[string]any{"nats": ns, "nats_basePath": "test.e2e"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := ns.Start(); err != nil {
		t.Fatal(err)
	}
	wf, _ := eng.Workflow("AskWf")
	ep := wf.Trigger.(natschat.ChatEndpoint)
	subject := ep.ChatSubject()

	ask := `{` + modeBody + `"message":"q","askUser":true}`
	plain := `{` + modeBody + `"message":"q"}`
	none := func(name string) {
		t.Helper()
		for _, tools := range llm.takeTools() {
			if len(tools) != 0 {
				t.Fatalf("%s: tools = %v", name, tools)
			}
		}
	}

	// Not enabled: askUser is ignored.
	if calls, final := run(t, nc, subject, ask, ""); len(calls) != 0 || final != "plain" {
		t.Fatalf("not enabled: calls=%v final=%q", calls, final)
	}
	none("not enabled")

	wf.Trigger.(interface{ EnableAskUser() }).EnableAskUser()

	// Enabled, no askUser: unchanged.
	if calls, final := run(t, nc, subject, plain, ""); len(calls) != 0 || final != "plain" {
		t.Fatalf("no askUser: calls=%v final=%q", calls, final)
	}
	none("no askUser")

	// Enabled + askUser: offered, called, answered on the tool-result subject.
	calls, final := run(t, nc, subject, ask, `{"status":"submitted","values":{"p":"2"}}`)
	if len(calls) != 1 || calls[0] != "request_user_input" || final != `got {"status":"submitted","values":{"p":"2"}}` {
		t.Fatalf("askUser: calls=%v final=%q", calls, final)
	}
	tools := llm.takeTools()
	if len(tools) != 2 || len(tools[0]) != 1 || tools[0][0] != "request_user_input" {
		t.Fatalf("askUser tools = %v", tools)
	}
}
