package rsassistant

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/ask"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeAsker stands in for *agent.Stream's ask half.
type fakeAsker struct {
	mu     sync.Mutex
	asks   []ask.Ask
	answer func(a ask.Ask) (ask.Answer, error)
	parked []string
}

func (f *fakeAsker) AskUser(_ context.Context, a ask.Ask) (ask.Answer, error) {
	f.mu.Lock()
	f.asks = append(f.asks, a)
	f.mu.Unlock()
	return f.answer(a)
}

func (f *fakeAsker) Park(askID string) {
	f.mu.Lock()
	f.parked = append(f.parked, askID)
	f.mu.Unlock()
}

var _ userAsker = (*agent.Stream)(nil) // the real stream satisfies it

func uiTools() *uiBridge {
	return newUIBridge(map[string]string{"PickOne": "tool/ui-pick-one", "ConfirmAction": "tool/ui-confirm"})
}

func TestRelayWithoutUserInputErrors(t *testing.T) {
	for _, e := range []error{agent.ErrUnsupported, agent.ErrRunEnded} {
		f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{}, e }}
		payload, parked, err := uiTools().answer(context.Background(), f, "PickOne",
			json.RawMessage(`{"prompt":"Which?","options":[{"value":"a","label":"A"},{"value":"b","label":"B"}]}`))
		if err != nil || parked != "" {
			t.Fatalf("%v: err=%v parked=%q", e, err, parked)
		}
		if string(payload) != `{"_error":"can't ask the user here"}` {
			t.Fatalf("%v: payload = %s", e, payload)
		}
	}
}

func TestRelayMalformedAskIsToolErrorWithoutCard(t *testing.T) {
	f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { t.Fatal("no card for a malformed ask"); return ask.Answer{}, nil }}
	payload, _, _ := uiTools().answer(context.Background(), f, "PickOne", json.RawMessage(`{"prompt":"Which?","options":[]}`))
	if !strings.Contains(string(payload), `"_error"`) || !strings.Contains(string(payload), "choice needs options") {
		t.Fatalf("payload = %s", payload)
	}
}

func TestRelaySkipAndParkMapping(t *testing.T) {
	in := json.RawMessage(`{"prompt":"Which?","options":[{"value":"a","label":"A"},{"value":"b","label":"B"}]}`)
	skip := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) {
		return ask.Answer{AskID: "ask_1", Status: ask.StatusSkipped}, agent.ErrSkipped
	}}
	if p, parked, _ := uiTools().answer(context.Background(), skip, "PickOne", in); string(p) != `{"_skipped":true}` || parked != "" {
		t.Fatalf("skip: payload=%s parked=%q", p, parked)
	}
	park := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{AskID: "ask_2"}, agent.ErrParked }}
	if p, parked, _ := uiTools().answer(context.Background(), park, "PickOne", in); string(p) != `{"_skipped":true}` || parked != "ask_2" {
		t.Fatalf("park: payload=%s parked=%q", p, parked)
	}
	// Declining a confirmation is the legacy rejection.
	conf := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) {
		return ask.Answer{AskID: "ask_3", Status: ask.StatusSkipped}, agent.ErrSkipped
	}}
	if p, _, _ := uiTools().answer(context.Background(), conf, "ConfirmAction", json.RawMessage(`{"prompt":"Go?"}`)); string(p) != `{"approved":false}` {
		t.Fatalf("confirm skip: payload=%s", p)
	}
}

// uiEngine is a fake workflow engine that runs one client-UI tool call the
// way the agent loop does: tool_call, wait on the announced tool-result
// subject, then tool_result + complete (or whatever follows).
func startUIEngine(t *testing.T, nc *nats.Conn, subject string, toolCall string, after func(result []byte) []natsstream.StreamEvent) chan []byte {
	t.Helper()
	got := make(chan []byte, 1)
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		prefix := "test.base._.tool_result.stream-1"
		seq := 0
		send := func(e natsstream.StreamEvent) {
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(e.Type))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(seq))
			out.Header.Set(natsstream.HdrStreamId, "stream-1")
			if seq == 0 {
				out.Header.Set(natsstream.HdrStreamCancelSubject, "test.cancel.stream-1")
				out.Header.Set(natsstream.HdrStreamToolResultPrefix, prefix)
			}
			seq++
			out.Data = e.Data
			_ = nc.PublishMsg(out)
		}
		results := make(chan []byte, 1)
		sub, _ := nc.Subscribe(prefix+".tu1", func(r *nats.Msg) { results <- r.Data })
		_ = nc.Flush()
		go func() {
			defer sub.Unsubscribe()
			send(ev(natsstream.StreamStart, 0, `{"sessionId":"s1"}`))
			send(ev(natsstream.StreamToolCall, 0, toolCall))
			select {
			case r := <-results:
				got <- r
				for _, e := range after(r) {
					send(e)
				}
			case <-time.After(5 * time.Second):
				got <- nil
			}
		}()
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestConsultAnswersClientUIToolThroughAskUser(t *testing.T) {
	_, nc := runEmbedded(t)
	got := startUIEngine(t, nc, "test.base.Ui",
		`{"toolUseId":"tu1","name":"PickOne","input":{"prompt":"Which pharmacy?","options":[{"value":"1","label":"One"},{"value":"2","label":"Two"}]}}`,
		func(r []byte) []natsstream.StreamEvent {
			return []natsstream.StreamEvent{
				ev(natsstream.StreamToolResult, 0, `{"toolUseId":"tu1","output":`+string(r)+`,"isError":false}`),
				ev(natsstream.StreamComplete, 0, `{"finalText":"Pharmacy Two.","messageStop":"end_turn"}`),
			}
		})
	f := &fakeAsker{answer: func(a ask.Ask) (ask.Answer, error) {
		return ask.BuildAnswer(a, "ask_1", map[string]any{"value": "2"}), nil
	}}
	out := &recorder{}
	_, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Ui", AccountID: "a", UserID: "u", Text: "find it", Timeout: 5 * time.Second,
		UI: uiTools(), Asker: f,
	}, silent(), out)
	if err != nil {
		t.Fatal(err)
	}
	if r := <-got; string(r) != `{"value":"2"}` {
		t.Fatalf("engine got tool result %s", r)
	}
	if len(f.asks) != 1 || f.asks[0].Title != "Which pharmacy?" || f.asks[0].Fields[0].Type != ask.TypeChoice {
		t.Fatalf("asks = %+v", f.asks)
	}
	last := out.evs[len(out.evs)-1]
	if last.kind != "done" || last.a != wire.StopEndTurn {
		t.Fatalf("last event = %+v", last)
	}
}

func TestConsultParkEndsTurnAwaitingInput(t *testing.T) {
	_, nc := runEmbedded(t)
	got := startUIEngine(t, nc, "test.base.Ui",
		`{"toolUseId":"tu1","name":"PickOne","input":{"prompt":"Which pharmacy?","options":[{"value":"1","label":"One"},{"value":"2","label":"Two"}]}}`,
		func(r []byte) []natsstream.StreamEvent {
			return []natsstream.StreamEvent{
				ev(natsstream.StreamToolResult, 0, `{"toolUseId":"tu1","output":{"error":"user declined to answer"},"isError":true}`),
				ev(natsstream.StreamDelta, 0, `{"text":"OK, skipping that."}`),
				ev(natsstream.StreamComplete, 0, `{"finalText":"OK, skipping that.","messageStop":"end_turn"}`),
			}
		})
	f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{AskID: "ask_9"}, agent.ErrParked }}
	out := &recorder{}
	_, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Ui", AccountID: "a", UserID: "u", Text: "find it", Timeout: 5 * time.Second,
		UI: uiTools(), Asker: f,
	}, silent(), out)
	if err != nil {
		t.Fatal(err)
	}
	if r := <-got; string(r) != `{"_skipped":true}` {
		t.Fatalf("engine got tool result %s", r)
	}
	if len(f.parked) != 1 || f.parked[0] != "ask_9" {
		t.Fatalf("parked = %v", f.parked)
	}
	// After Park the turn is over: nothing the workflow says next is relayed.
	for _, e := range out.evs {
		if e.kind == "text" || e.kind == "done" || e.kind == "error" {
			t.Fatalf("event after park relayed: %+v", e)
		}
	}
}

func TestEligibleWorkflowWithUITools(t *testing.T) {
	pick, err := clientui.PickOneFactory.New(json.RawMessage(`{"toolName":"PickOne","toolDescription":"d"}`))
	if err != nil {
		t.Fatal(err)
	}
	wf := wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"os": plainNode{}, "uiPickOne1": pick})
	ep, reason := Eligibility(wf)
	if reason != "" || ep == nil {
		t.Fatalf("UI-tool workflow must be eligible, got reason=%q", reason)
	}
	tools := ClientUITools(wf)
	if len(tools) != 1 || tools["PickOne"] != "tool/ui-pick-one" {
		t.Fatalf("ClientUITools = %v", tools)
	}
	if got := ClientUITools(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"os": plainNode{}})); len(got) != 0 {
		t.Fatalf("plain workflow has no UI tools, got %v", got)
	}
}

// Answers to asks parked in an earlier turn reach the workflow as context.
func TestResumeInputsBecomeContext(t *testing.T) {
	a := ask.Ask{Title: "Which pharmacy?", Fields: []ask.Field{{Name: "value", Label: "Pharmacy", Type: ask.TypeChoice,
		Options: []ask.Option{{Value: "1", Label: "One"}, {Value: "2", Label: "Two"}}}}}
	raw, _ := json.Marshal(a)
	in := wire.AskInput{AskID: "ask_9", Ask: raw, Action: wire.InputActionSubmit, Values: map[string]any{"value": "2"}}
	got := withInputs("go on", []agent.ValidInput{{AskInput: in, Ask: a}})
	if !strings.HasPrefix(got, "[Earlier you asked the user: Which pharmacy?. They answered: Pharmacy = Two]") || !strings.HasSuffix(got, "go on") {
		t.Fatalf("text = %q", got)
	}
	if withInputs("hi", nil) != "hi" {
		t.Fatal("no inputs must leave the text unchanged")
	}
}

// The bridge parks its asks before the workflow stops listening, so an
// answer at the boundary is never published to a dead subject.
func TestAskWaitBelowEngineWait(t *testing.T) {
	env := func(v string) func(string) (string, bool) {
		return func(k string) (string, bool) {
			if k == "AGENT_ASK_WAIT" && v != "" {
				return v, true
			}
			return "", false
		}
	}
	if got := bridgeAskWait(5*time.Minute, env("")); got != 5*time.Minute-askMargin {
		t.Fatalf("default = %s", got)
	}
	if got := bridgeAskWait(5*time.Minute, env("2m")); got != 2*time.Minute {
		t.Fatalf("env 2m = %s", got)
	}
	if got := bridgeAskWait(time.Minute, env("10m")); got != time.Minute-askMargin {
		t.Fatalf("capped = %s", got)
	}
}

// Time spent waiting for the user does not count against the consult timeout.
func TestEngineStreamPause(t *testing.T) {
	_, nc := runEmbedded(t)
	_, _ = nc.Subscribe("test.silent", func(*nats.Msg) {}) // engine that never answers
	_ = nc.Flush()
	s, err := openEngineStream(nc, "test.silent", nil, []byte(`{}`), 200*time.Millisecond, silent())
	if err != nil {
		t.Fatal(err)
	}
	resume := s.Pause()
	select {
	case <-s.done:
		t.Fatal("timed out while paused")
	case <-time.After(400 * time.Millisecond):
	}
	resume()
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("never timed out after resume")
	}
	if err := s.Close(); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v", err)
	}
}
