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

// idxTool: an index-scoped tool with a required policy input.
type idxTool struct{ pattern string }

func (idxTool) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/idx-tool", Role: node.RoleTool,
		InputPorts: []node.PortSpec{{Name: node.PortPolicy, Direction: node.PortIn, Cardinality: node.CardOne, Required: true, PeerRole: node.RolePolicy}}}
}
func (idxTool) Init(context.Context, node.NodeEnv) error { return nil }
func (idxTool) Close(context.Context) error              { return nil }
func (t idxTool) AllowedIndexPattern() string            { return t.pattern }

// idxPolicy answers FunctionForIndex from a fixed table; a missing pattern is "no row".
type idxPolicy struct{ functions map[string]string }

func (idxPolicy) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/idx-policy", Role: node.RolePolicy,
		OutputPorts: []node.PortSpec{{Name: node.PortPolicy, Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (idxPolicy) Init(context.Context, node.NodeEnv) error { return nil }
func (idxPolicy) Close(context.Context) error              { return nil }
func (idxPolicy) Resolve(context.Context, node.PolicyRequest) (node.PolicyResult, error) {
	return node.PolicyResult{}, nil
}
func (p idxPolicy) FunctionForIndex(_ context.Context, pattern string) (string, error) {
	if fn, ok := p.functions[pattern]; ok {
		return fn, nil
	}
	return "", fmt.Errorf("no powerlineindexconfig row for index %q", pattern)
}

func idxWorkflow(id, agentName, subject, pattern string) string {
	tool, conn := "", ""
	if pattern != "" {
		tool = fmt.Sprintf(`,
    {"id": "tool", "type": "test/idx-tool", "config": {"pattern": %q}},
    {"id": "pol",  "type": "test/idx-policy", "config": {}}`, pattern)
		conn = `,
    {"from": {"node": "pol", "port": "policy"}, "to": {"node": "tool", "port": "policy"}}`
	}
	return fmt.Sprintf(`{
  "id": %q, "version": 1, "trigger": "trig",
  "rsassistant": {"name": %q, "description": "d"},
  "nodes": [
    {"id": "trig",  "type": "test/chat-trigger", "config": {"subject": %q}},
    {"id": "agent", "type": "test/agent", "config": {}}%s
  ],
  "connections": [
    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}%s
  ]
}`, id, agentName, subject, tool, conn)
}

// serveStubEngine answers a workflow chat subject with a start/delta/complete stream.
func serveStubEngine(t *testing.T, nc *nats.Conn, subject string) {
	t.Helper()
	if _, err := nc.Subscribe(subject, func(m *nats.Msg) {
		script := []struct {
			typ  natsstream.StreamEventType
			data string
		}{
			{natsstream.StreamStart, `{"sessionId":"s"}`},
			{natsstream.StreamDelta, `{"text":"ok"}`},
			{natsstream.StreamComplete, `{"finalText":"ok","messageStop":"end_turn"}`},
		}
		for i, ev := range script {
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(ev.typ))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(i))
			out.Header.Set(natsstream.HdrStreamId, "st")
			out.Data = []byte(ev.data)
			_ = nc.PublishMsg(out)
		}
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeUsesTheIndexFunctionPerAgentAndSkipsUnbound(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	defer srv.Shutdown()
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	t.Setenv("NATS_URL", srv.ClientURL())
	t.Setenv("NATS_JWT", "")
	t.Setenv("NATS_KEY", "")

	// Stub identity: token "tok-a" holds only FN-IDX-A; token "tok-app" holds only FN-APP.
	grants := map[string]map[string]bool{"tok-a": {"FN-IDX-A": true}, "tok-app": {"FN-APP": true}}
	_, _ = nc.Subscribe("test.identity.validateInternalToken", func(m *nats.Msg) {
		var req struct {
			IDT        string `json:"idt"`
			FunctionID string `json:"functionId"`
		}
		_ = json.Unmarshal(m.Data, &req)
		held, known := grants[req.IDT]
		switch {
		case !known:
			_ = m.Respond([]byte(`{"valid":false,"reason":"INVALID_IDT"}`))
		case held[req.FunctionID]:
			_ = m.Respond([]byte(`{"valid":true,"functionGranted":true,"userId":"u-1","accountId":"acct-9"}`))
		default:
			_ = m.Respond([]byte(`{"valid":true,"functionGranted":false,"userId":"u-1","accountId":"acct-9","reason":"DENIED_FN"}`))
		}
	})
	serveStubEngine(t, nc, "test.base.idxA")
	serveStubEngine(t, nc, "test.base.plain")

	dir := t.TempDir()
	for name, content := range map[string]string{
		"idxA.json":  idxWorkflow("idxA", "agentA", "test.base.idxA", "a-*"),    // bound to FN-IDX-A
		"idxB.json":  idxWorkflow("idxB", "agentB", "test.base.idxB", "b-*"),    // no row: unbound
		"plain.json": idxWorkflow("plain", "agentPlain", "test.base.plain", ""), // no index: APP_FUNCTION_ID
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reg := node.NewRegistry()
	_ = reg.Register("test/chat-trigger", node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
		var c struct {
			Subject string `json:"subject"`
		}
		_ = json.Unmarshal(raw, &c)
		return chatTrigger{subject: c.Subject}, nil
	}))
	_ = reg.Register("test/agent", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return sinkAgent{}, nil }))
	_ = reg.Register("test/idx-tool", node.FactoryFunc(func(raw json.RawMessage) (node.Node, error) {
		var c struct {
			Pattern string `json:"pattern"`
		}
		_ = json.Unmarshal(raw, &c)
		return idxTool{pattern: c.Pattern}, nil
	}))
	_ = reg.Register("test/idx-policy", node.FactoryFunc(func(json.RawMessage) (node.Node, error) {
		return idxPolicy{functions: map[string]string{"a-*": "FN-IDX-A"}}, nil
	}))
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
		"APP_FUNCTION_ID":                     "FN-APP",
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

	// idxB is unbound, so it is not published.
	if names := rt.Names(); len(names) != 2 || names[0] != "agentA" || names[1] != "agentPlain" {
		t.Fatalf("published = %v, want [agentA agentPlain]", names)
	}

	cli := agentclient.NewFromConn(nc)
	var cards []wire.AgentCard
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		cards, err = cli.Discover(context.Background(), wire.DiscoverFilter{}, 300*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if len(cards) >= 2 {
			break
		}
	}
	byName := map[string]wire.AgentCard{}
	for _, c := range cards {
		byName[c.Name] = c
	}
	if len(byName) != 2 {
		t.Fatalf("cards = %+v, want exactly agentA and agentPlain", cards)
	}
	if a := byName["agentA"]; a.Access == nil || a.Access.AppID != "APPX" || a.Access.FunctionID != "FN-IDX-A" {
		t.Fatalf("agentA access = %+v, want APPX / FN-IDX-A", a.Access)
	}
	if p := byName["agentPlain"]; p.Access == nil || p.Access.FunctionID != "FN-APP" {
		t.Fatalf("agentPlain access = %+v, want FN-APP", p.Access)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	chat := func(token, agent string) error {
		run, err := cli.Chat(agentclient.WithIDT(ctx, token), agent, wire.ChatRequest{
			Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "hi"}}},
		})
		if err != nil {
			return err
		}
		for ev := range run.Events {
			if ev.Type == wire.EventError {
				return fmt.Errorf("%s", ev.Error)
			}
		}
		return nil
	}
	if err := chat("tok-a", "agentA"); err != nil {
		t.Fatalf("holder of the index function must reach agentA: %v", err)
	}
	if err := chat("tok-app", "agentA"); err == nil {
		t.Fatal("a user holding only the old app-wide function must be refused by agentA")
	}
	if err := chat("tok-app", "agentPlain"); err != nil {
		t.Fatalf("agentPlain keeps the app-wide function: %v", err)
	}
	if err := chat("tok-a", "agentPlain"); err == nil {
		t.Fatal("holder of only an index function must be refused by agentPlain")
	}
}
