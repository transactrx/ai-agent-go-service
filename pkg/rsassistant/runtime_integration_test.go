package rsassistant_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/rsassistant"
	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// chatTrigger: node.Trigger + natschat.ChatEndpoint (subject fixed for the test).
type chatTrigger struct {
	subject string
}

func (chatTrigger) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/chat-trigger", Role: node.RoleTrigger,
		OutputPorts: []node.PortSpec{{Name: "main", Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (chatTrigger) Init(context.Context, node.NodeEnv) error          { return nil }
func (chatTrigger) Close(context.Context) error                       { return nil }
func (chatTrigger) Subscribe(context.Context, node.TriggerSink) error { return nil }
func (c chatTrigger) ChatSubject() string                             { return c.subject }
func (chatTrigger) ResponseMode() string                              { return "single" }
func (chatTrigger) AllowsResponseModeOverride() bool                  { return true }
func (chatTrigger) RequestTimeout() time.Duration                     { return time.Minute }

// sinkAgent: minimal node.Agent so the workflow validates (copied shape from engine_test.fakeAgent).
type sinkAgent struct{}

func (sinkAgent) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/agent", Role: node.RoleAgent,
		InputPorts:  []node.PortSpec{{Name: "main", Direction: node.PortIn, Cardinality: node.CardOne, Required: true}},
		OutputPorts: []node.PortSpec{{Name: "main", Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (sinkAgent) Init(context.Context, node.NodeEnv) error { return nil }
func (sinkAgent) Close(context.Context) error              { return nil }
func (sinkAgent) Process(_ context.Context, _ node.AgentInput, sink node.StreamSink) error {
	return sink.Close(context.Background(), node.StreamEvent{Type: node.StreamComplete, Data: []byte(`{"finalText":"ok"}`)})
}

// uiTool: client-only marker so one workflow is ineligible.
type uiTool struct{ sinkAgent }

func (uiTool) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "tool/ui-confirm", Role: node.RoleTool,
		InputPorts: []node.PortSpec{{Name: "main", Direction: node.PortIn, Cardinality: node.CardOne, Required: true}}}
}
func (uiTool) ClientOnly() bool { return true }

const wfJSON = `{
  "id": "%s", "version": 1, "trigger": "trig",
  %s
  "nodes": [
    {"id": "trig",  "type": "test/chat-trigger", "config": {}},
    {"id": "agent", "type": "%s", "config": {}}
  ],
  "connections": [{"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}]
}`

func TestRuntimePublishesEligibleWorkflowsAndBridgesVerifiedIdentity(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	t.Setenv("NATS_URL", srv.ClientURL()) // nats-agent opens its own connection from env
	t.Setenv("NATS_JWT", "")
	t.Setenv("NATS_KEY", "")

	// Fake identity: token "good" → granted user u-1 / account acct-9; anything else invalid.
	_, _ = nc.Subscribe("test.identity.validateInternalToken", func(m *nats.Msg) {
		var req struct {
			IDT        string `json:"idt"`
			AgentID    string `json:"agentId"`
			FunctionID string `json:"functionId"`
		}
		_ = json.Unmarshal(m.Data, &req)
		if req.IDT == "good" && req.AgentID == "APPX" && req.FunctionID == "FNX" {
			_ = m.Respond([]byte(`{"valid":true,"functionGranted":true,"userId":"u-1","accountId":"acct-9"}`))
			return
		}
		_ = m.Respond([]byte(`{"valid":false,"reason":"INVALID_IDT"}`))
	})

	// Fake engine on the eligible workflow's chat subject.
	var mu sync.Mutex
	var seen []*nats.Msg
	_, _ = nc.Subscribe("test.base.SingleSearch", func(m *nats.Msg) {
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		script := []struct {
			typ  natsstream.StreamEventType
			data string
		}{
			{natsstream.StreamStart, `{"sessionId":"s"}`},
			{natsstream.StreamDelta, `{"text":"42 claims"}`},
			{natsstream.StreamComplete, `{"finalText":"42 claims","messageStop":"end_turn"}`},
		}
		for i, ev := range script {
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(ev.typ))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(i))
			out.Header.Set(natsstream.HdrStreamId, "st")
			out.Data = []byte(ev.data)
			_ = nc.PublishMsg(out)
		}
	})

	// Engine with two workflows: eligible (manifest name) and ineligible (UI tool).
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("single.json", fmt.Sprintf(wfJSON, "SingleSearch", `"rsassistant": {"name": "claimSearch", "description": "Claims Q&A"},`, "test/agent"))
	write("ui.json", fmt.Sprintf(wfJSON, "UiSearch", "", "test/ui"))
	reg := node.NewRegistry()
	_ = reg.Register("test/chat-trigger", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return chatTrigger{subject: "test.base.SingleSearch"}, nil }))
	_ = reg.Register("test/agent", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return sinkAgent{}, nil }))
	_ = reg.Register("test/ui", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return uiTool{}, nil }))
	eng, err := engine.New(engine.Config{Source: engine.NewFilesystemSource(dir), Registry: reg, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	env := map[string]string{
		"RSASSISTANT_ENABLED":                 "true",
		"APP_ID":                              "APPX",
		"APP_FUNCTION_ID":                     "FNX",
		"NATS_IDENTITY_BASE_PATH":             "test.identity",
		"NATS_IDENTITY_VALIDATE_SUBJECT":      "validateInternalToken",
		"RSASSISTANT_CONSULT_TIMEOUT_SECONDS": "5",
	}
	rt, err := rsassistant.Start(context.Background(), rsassistant.Deps{
		Engine: eng, Logger: log.New(io.Discard, "", 0), RepositoryURL: "https://example/repo",
		LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown()
	if names := rt.Names(); len(names) != 1 || names[0] != "claimSearch" {
		t.Fatalf("published = %v, want [claimSearch]", names)
	}

	cli := agentclient.NewFromConn(nc)

	// Discovery: exactly one card, carrying the shared access pair.
	// nats-agent Start() does not flush its discover subscription, so poll the
	// way RSAssistant does instead of relying on a single scatter-gather.
	var cards []wire.AgentCard
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		cards, err = cli.Discover(context.Background(), wire.DiscoverFilter{}, 300*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if len(cards) > 0 {
			break
		}
	}
	if len(cards) != 1 || cards[0].Name != "claimSearch" || cards[0].Access == nil ||
		cards[0].Access.AppID != "APPX" || cards[0].Access.FunctionID != "FNX" || cards[0].Description != "Claims Q&A" {
		t.Fatalf("cards = %+v", cards)
	}

	// Consult with a good token: identity headers must be the verified ones, not the spoofed userId.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := cli.Chat(agentclient.WithIDT(ctx, "good"), "claimSearch", wire.ChatRequest{
		UserID:  "spoofed-user",
		Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "how many?"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	text, stop := "", ""
	for ev := range run.Events {
		switch ev.Type {
		case wire.EventText:
			text += ev.TextDelta
		case wire.EventDone:
			stop = ev.StopReason
		case wire.EventError:
			t.Fatalf("unexpected error event: %s", ev.Error)
		}
	}
	if text != "42 claims" || stop != wire.StopEndTurn {
		t.Fatalf("text=%q stop=%q", text, stop)
	}
	mu.Lock()
	if len(seen) != 1 || seen[0].Header.Get("X-User-Id") != "u-1" || seen[0].Header.Get("X-Account-Id") != "acct-9" || seen[0].Header.Get("X-TRX-IDT") != "good" {
		t.Fatalf("engine saw headers %v", headersOf(seen))
	}
	mu.Unlock()

	// Consult with a bad token: rejected before the engine is ever called.
	_, err = cli.Chat(agentclient.WithIDT(ctx, "bad"), "claimSearch", wire.ChatRequest{
		Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "how many?"}}},
	})
	if err == nil {
		t.Fatal("bad token must be refused")
	}
	mu.Lock()
	if len(seen) != 1 {
		t.Fatalf("engine must not be called for a denied token; calls=%d", len(seen))
	}
	mu.Unlock()

	rt.Shutdown()
	srv.Shutdown()
}

func headersOf(msgs []*nats.Msg) []nats.Header {
	out := []nats.Header{}
	for _, m := range msgs {
		out = append(out, m.Header)
	}
	return out
}
