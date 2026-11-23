package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

var askSpec = node.ToolSpec{Name: "request_user_input", Description: "ask", InputSchema: json.RawMessage(`{"type":"object"}`)}

func runToolAgent(env node.NodeEnv, tools []node.Tool, scripts [][]node.LLMEvent) (*agentNode, *fakeLLM) {
	llm := &fakeLLM{scripts: scripts}
	return &agentNode{
		cfg:        Config{MaxIterations: 5, SystemMessage: "sys", HumanResponseTimeoutSeconds: 1},
		env:        env,
		workflowID: "wf",
		tools:      tools,
		llm:        llm,
	}, llm
}

func toolNames(specs []node.ToolSpec) []string {
	out := make([]string, 0, len(specs))
	for _, s := range specs {
		out = append(out, s.Name)
	}
	return out
}

// Without run tools in ctx the model's tool list is the connected tools only.
func TestRunClientToolsAbsentLeavesToolListUnchanged(t *testing.T) {
	a, llm := runToolAgent(testNodeEnv{}, []node.Tool{&fakeTool{name: "search"}}, [][]node.LLMEvent{{
		{Kind: node.LLMTextDelta, Delta: "ok"}, {Kind: node.LLMMessageStop, Stop: "end_turn"},
	}})
	if err := a.Process(context.Background(), node.AgentInput{Message: "hi", SessionID: "s"}, &recordingSink{}); err != nil {
		t.Fatal(err)
	}
	if got := toolNames(llm.last.Tools); len(got) != 1 || got[0] != "search" {
		t.Fatalf("tools = %v", got)
	}
}

// A run tool is offered after the connected tools and its call takes the
// client path: tool_call, then the client's result.
func TestRunClientToolOfferedAndAwaited(t *testing.T) {
	env := &clientResultEnv{resp: map[string][]byte{"tu1": []byte(`{"status":"submitted","values":{"x":1}}`)}}
	a, llm := runToolAgent(env, []node.Tool{&fakeTool{name: "search"}}, [][]node.LLMEvent{
		{
			{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "request_user_input", InputJSON: []byte(`{"title":"Q"}`)}},
			{Kind: node.LLMMessageStop, Stop: "tool_use"},
		},
		{{Kind: node.LLMTextDelta, Delta: "done"}, {Kind: node.LLMMessageStop, Stop: "end_turn"}},
	})
	ctx := node.WithRunClientTools(context.Background(), askSpec)
	sink := &recordingSink{}
	if err := a.Process(ctx, node.AgentInput{Message: "hi", SessionID: "s"}, sink); err != nil {
		t.Fatal(err)
	}
	if got := toolNames(llm.last.Tools); len(got) != 2 || got[0] != "search" || got[1] != "request_user_input" {
		t.Fatalf("tools = %v", got)
	}
	var call, result bool
	for _, ev := range sink.events {
		s := string(ev.Data)
		if ev.Type == node.StreamToolCall && strings.Contains(s, "request_user_input") {
			call = true
		}
		if ev.Type == node.StreamToolResult && strings.Contains(s, `"submitted"`) && strings.Contains(s, `"isError":false`) {
			result = true
		}
	}
	if !call || !result || sink.term.Type != node.StreamComplete {
		t.Fatalf("call=%t result=%t term=%+v events=%+v", call, result, sink.term, sink.events)
	}
}

// _error from the client is a surfaced tool error for a run tool too.
func TestRunClientToolErrorSurfaced(t *testing.T) {
	env := &clientResultEnv{resp: map[string][]byte{"tu1": []byte(`{"_error":"can't ask the user here"}`)}}
	a, _ := runToolAgent(env, nil, [][]node.LLMEvent{
		{
			{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "request_user_input", InputJSON: []byte(`{}`)}},
			{Kind: node.LLMMessageStop, Stop: "tool_use"},
		},
		{{Kind: node.LLMTextDelta, Delta: "ok"}, {Kind: node.LLMMessageStop, Stop: "end_turn"}},
	})
	sink := &recordingSink{}
	if err := a.Process(node.WithRunClientTools(context.Background(), askSpec), node.AgentInput{Message: "hi", SessionID: "s"}, sink); err != nil {
		t.Fatal(err)
	}
	var saw bool
	for _, ev := range sink.events {
		if ev.Type == node.StreamToolResult && strings.Contains(string(ev.Data), `"isError":true`) &&
			strings.Contains(string(ev.Data), "can't ask the user here") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("events = %+v", sink.events)
	}
}

// A connected tool with the same name wins; the run tool is not added twice.
func TestRunClientToolNameCollisionKeepsConnectedTool(t *testing.T) {
	conn := &fakeTool{name: "request_user_input", out: json.RawMessage(`"server"`)}
	a, llm := runToolAgent(testNodeEnv{}, []node.Tool{conn}, [][]node.LLMEvent{
		{
			{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "request_user_input", InputJSON: []byte(`{}`)}},
			{Kind: node.LLMMessageStop, Stop: "tool_use"},
		},
		{{Kind: node.LLMTextDelta, Delta: "ok"}, {Kind: node.LLMMessageStop, Stop: "end_turn"}},
	})
	sink := &recordingSink{}
	if err := a.Process(node.WithRunClientTools(context.Background(), askSpec), node.AgentInput{Message: "hi", SessionID: "s"}, sink); err != nil {
		t.Fatal(err)
	}
	if got := toolNames(llm.last.Tools); len(got) != 1 {
		t.Fatalf("tools = %v", got)
	}
	if conn.invoked != 1 {
		t.Fatalf("connected tool calls = %d", conn.invoked)
	}
}

func TestRunClientToolsContext(t *testing.T) {
	if got := node.RunClientTools(context.Background()); got != nil {
		t.Fatalf("empty ctx = %v", got)
	}
	ctx := node.WithRunClientTools(context.Background(), askSpec)
	if got := node.RunClientTools(ctx); len(got) != 1 || got[0].Name != "request_user_input" {
		t.Fatalf("got %v", got)
	}
	if node.WithRunClientTools(context.Background()) != context.Background() {
		t.Fatal("no specs must leave ctx unchanged")
	}
}

// _parked on a run tool ends the turn at once: the user's request and a
// "waiting" note are saved to memory (the resume turn needs them), the
// stream completes with awaiting_input, and no later tool call runs.
func TestRunClientToolParkedPersistsTurnAndStops(t *testing.T) {
	env := &clientResultEnv{resp: map[string][]byte{"tu1": []byte(`{"_parked":true}`)}}
	later := &fakeTool{name: "search"}
	mem := &fakeMem{}
	a, llm := runToolAgent(env, []node.Tool{later}, [][]node.LLMEvent{
		{
			{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu1", Name: "request_user_input", InputJSON: []byte(`{"title":"Which pharmacy?"}`)}},
			{Kind: node.LLMToolUseStop, ToolUse: &node.LLMToolUse{ID: "tu2", Name: "search", InputJSON: []byte(`{}`)}},
			{Kind: node.LLMMessageStop, Stop: "tool_use"},
		},
	})
	a.mem = mem
	sink := &recordingSink{}
	if err := a.Process(node.WithRunClientTools(context.Background(), askSpec), node.AgentInput{Message: "claims rejected last month?", SessionID: "s"}, sink); err != nil {
		t.Fatal(err)
	}
	if later.invoked != 0 || llm.calls != 1 {
		t.Fatalf("ran on after park: tool=%d llm=%d", later.invoked, llm.calls)
	}
	if sink.term.Type != node.StreamComplete || !strings.Contains(string(sink.term.Data), `"messageStop":"awaiting_input"`) {
		t.Fatalf("term = %+v", sink.term)
	}
	if len(mem.turns) != 1 {
		t.Fatalf("turns = %+v", mem.turns)
	}
	tu := mem.turns[0]
	if tu.User.Content[0].Text != "claims rejected last month?" || !strings.Contains(tu.Assistant.Content[0].Text, "Which pharmacy?") {
		t.Fatalf("turn = %+v", tu)
	}
}
