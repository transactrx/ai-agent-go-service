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

const askIn = `{"title":"Which pharmacy?","fields":[{"name":"p","label":"Pharmacy","type":"choice","options":[{"value":"1","label":"One"},{"value":"2","label":"Two"}]}]}`

func TestAnswerAskWithoutUserInputErrors(t *testing.T) {
	for _, e := range []error{agent.ErrUnsupported, agent.ErrRunEnded} {
		f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{}, e }}
		payload, parked, err := answerAsk(context.Background(), f, json.RawMessage(askIn))
		if err != nil || parked != "" {
			t.Fatalf("%v: err=%v parked=%q", e, err, parked)
		}
		if string(payload) != `{"_error":"can't ask the user here"}` {
			t.Fatalf("%v: payload = %s", e, payload)
		}
	}
}

func TestAnswerAskMalformedIsToolErrorWithoutCard(t *testing.T) {
	f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { t.Fatal("no card for a malformed ask"); return ask.Answer{}, nil }}
	for _, in := range []string{`{"fields":[]}`, `{"title":"T","bogus":1}`, `not json`, `[1]`} {
		payload, parked, err := answerAsk(context.Background(), f, json.RawMessage(in))
		if err != nil || parked != "" || !strings.Contains(string(payload), `"_error"`) {
			t.Fatalf("%s: payload=%s parked=%q err=%v", in, payload, parked, err)
		}
	}
}

// Results match RSAssistant's own request_user_input (pkg/orchestrator/asktool.go).
func TestAnswerAskOutcomes(t *testing.T) {
	sub := &fakeAsker{answer: func(a ask.Ask) (ask.Answer, error) {
		return ask.BuildAnswer(a, "ask_1", map[string]any{"p": "2"}), nil
	}}
	p, parked, _ := answerAsk(context.Background(), sub, json.RawMessage(askIn))
	if string(p) != `{"status":"submitted","values":{"p":"2"},"display":{"p":"Two"}}` || parked != "" {
		t.Fatalf("submit: payload=%s parked=%q", p, parked)
	}
	if len(sub.asks) != 1 || sub.asks[0].Title != "Which pharmacy?" {
		t.Fatalf("asks = %+v", sub.asks)
	}
	skip := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) {
		return ask.Answer{AskID: "ask_2", Status: ask.StatusSkipped}, agent.ErrSkipped
	}}
	if p, parked, _ := answerAsk(context.Background(), skip, json.RawMessage(askIn)); string(p) != `{"status":"skipped"}` || parked != "" {
		t.Fatalf("skip: payload=%s parked=%q", p, parked)
	}
	sup := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) {
		return ask.Answer{AskID: "ask_3", Status: ask.StatusSuperseded, Values: map[string]any{"text": "never mind"}}, agent.ErrSuperseded
	}}
	if p, parked, _ := answerAsk(context.Background(), sup, json.RawMessage(askIn)); string(p) != `{"status":"not_answered","userWrote":"never mind"}` || parked != "" {
		t.Fatalf("supersede: payload=%s parked=%q", p, parked)
	}
	// Park publishes nothing: the caller cancels the workflow instead.
	park := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{AskID: "ask_4"}, agent.ErrParked }}
	if p, parked, _ := answerAsk(context.Background(), park, json.RawMessage(askIn)); p != nil || parked != "ask_4" {
		t.Fatalf("park: payload=%s parked=%q", p, parked)
	}
	busy := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{}, agent.ErrBusy }}
	if p, _, _ := answerAsk(context.Background(), busy, json.RawMessage(askIn)); !strings.Contains(string(p), `"_error"`) {
		t.Fatalf("busy: payload=%s", p)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gone := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{}, context.Canceled }}
	if p, _, err := answerAsk(ctx, gone, json.RawMessage(askIn)); p != nil || err == nil {
		t.Fatalf("ctx ended: payload=%s err=%v", p, err)
	}
}

// startAskEngine is a fake workflow engine that runs one client tool call
// the way the agent loop does: tool_call, wait on the announced tool-result
// subject, then tool_result + complete (or whatever follows). It records
// the request body.
func startAskEngine(t *testing.T, nc *nats.Conn, subject string, toolCall string, after func(result []byte) []natsstream.StreamEvent) (chan []byte, chan []byte) {
	t.Helper()
	got := make(chan []byte, 1)
	bodies := make(chan []byte, 1)
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		bodies <- m.Data
		// Real ordering: the session's host listens from creation; the loop
		// waits for the result only after tool_call + the rest of the stream.
		host := natsstream.NewToolResultHost(nc, "test.base", "stream-1", silent())
		_ = host.Listen()
		prefix := host.Prefix()
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
		go func() {
			defer host.Stop()
			send(ev(natsstream.StreamStart, 0, `{"sessionId":"s1"}`))
			send(ev(natsstream.StreamToolCall, 0, toolCall))
			time.Sleep(200 * time.Millisecond)
			r, err := host.WaitForToolResult(context.Background(), "tu1", time.Second)
			if err != nil {
				got <- nil
				return
			}
			got <- r
			for _, e := range after(r) {
				send(e)
			}
		}()
	})
	if err != nil {
		t.Fatal(err)
	}
	return got, bodies
}

func completeAfter(r []byte) []natsstream.StreamEvent {
	return []natsstream.StreamEvent{
		ev(natsstream.StreamToolResult, 0, `{"toolUseId":"tu1","output":`+string(r)+`,"isError":false}`),
		ev(natsstream.StreamComplete, 0, `{"finalText":"Pharmacy Two.","messageStop":"end_turn"}`),
	}
}

func TestConsultAnswersRequestUserInput(t *testing.T) {
	_, nc := runEmbedded(t)
	got, bodies := startAskEngine(t, nc, "test.base.Ask", `{"toolUseId":"tu1","name":"request_user_input","input":`+askIn+`}`, completeAfter)
	f := &fakeAsker{answer: func(a ask.Ask) (ask.Answer, error) {
		return ask.BuildAnswer(a, "ask_1", map[string]any{"p": "2"}), nil
	}}
	out := &recorder{}
	_, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Ask", AccountID: "a", UserID: "u", Text: "find it", Timeout: 5 * time.Second,
		AskUser: true, Asker: f,
	}, silent(), out)
	if err != nil {
		t.Fatal(err)
	}
	if b := <-bodies; !strings.Contains(string(b), `"askUser":true`) {
		t.Fatalf("body = %s", b)
	}
	if r := <-got; string(r) != `{"status":"submitted","values":{"p":"2"},"display":{"p":"Two"}}` {
		t.Fatalf("engine got tool result %s", r)
	}
	if len(f.asks) != 1 {
		t.Fatalf("asks = %+v", f.asks)
	}
	last := out.evs[len(out.evs)-1]
	if last.kind != "done" || last.a != wire.StopEndTurn {
		t.Fatalf("last event = %+v", last)
	}
}

// Without AskUser the body is today's (no askUser) and the bridge never
// answers tool calls: a same-named call is left to the workflow.
func TestConsultWithoutAskUserUnchanged(t *testing.T) {
	_, nc := runEmbedded(t)
	got, bodies := startAskEngine(t, nc, "test.base.NoAsk", `{"toolUseId":"tu1","name":"request_user_input","input":`+askIn+`}`, completeAfter)
	f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { t.Fatal("must not ask"); return ask.Answer{}, nil }}
	_, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.NoAsk", AccountID: "a", UserID: "u", Text: "q", Timeout: 5 * time.Second, Asker: f,
	}, silent(), &recorder{})
	if err != nil {
		t.Fatal(err)
	}
	if b := <-bodies; string(b) != `{"message":"q","responseMode":"streaming"}` {
		t.Fatalf("body = %s", b)
	}
	if r := <-got; r != nil {
		t.Fatalf("bridge answered: %s", r)
	}
}

// Other tool calls are never answered by the bridge.
func TestConsultIgnoresOtherToolCalls(t *testing.T) {
	_, nc := runEmbedded(t)
	got, _ := startAskEngine(t, nc, "test.base.Other", `{"toolUseId":"tu1","name":"OpenSearchQuery","input":{}}`, completeAfter)
	f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { t.Fatal("must not ask"); return ask.Answer{}, nil }}
	_, _ = consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Other", AccountID: "a", UserID: "u", Text: "q", Timeout: 5 * time.Second, AskUser: true, Asker: f,
	}, silent(), &recorder{})
	if r := <-got; r != nil {
		t.Fatalf("bridge answered: %s", r)
	}
}

// parkEngine sends one ask tool call, then reports what it gets: a tool
// result (published to results; when endOnResult it then ends the stream as
// the loop does after _parked) or a cancel (cancelled; it then ends the
// stream as cancelled). It ends on its own after 3s otherwise.
func parkEngine(t *testing.T, nc *nats.Conn, subject string, endOnResult bool) (results chan []byte, cancelled chan struct{}) {
	t.Helper()
	results = make(chan []byte, 4)
	cancelled = make(chan struct{}, 1)
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		host := natsstream.NewToolResultHost(nc, "test.base", "stream-1", silent())
		_ = host.Listen()
		seq := 0
		var mu sync.Mutex
		send := func(e natsstream.StreamEvent) {
			mu.Lock()
			defer mu.Unlock()
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(e.Type))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(seq))
			out.Header.Set(natsstream.HdrStreamId, "stream-1")
			if seq == 0 {
				out.Header.Set(natsstream.HdrStreamCancelSubject, "test.cancel."+subject)
				out.Header.Set(natsstream.HdrStreamToolResultPrefix, host.Prefix())
			}
			seq++
			out.Data = e.Data
			_ = nc.PublishMsg(out)
		}
		cancelSub, _ := nc.SubscribeSync("test.cancel." + subject)
		go func() {
			defer host.Stop()
			defer func() { _ = cancelSub.Unsubscribe() }()
			send(ev(natsstream.StreamStart, 0, `{"sessionId":"s1"}`))
			send(ev(natsstream.StreamToolCall, 0, `{"toolUseId":"tu1","name":"request_user_input","input":`+askIn+`}`))
			go func() {
				if r, err := host.WaitForToolResult(context.Background(), "tu1", 3*time.Second); err == nil {
					results <- r
					if endOnResult {
						send(ev(natsstream.StreamComplete, 0, `{"finalText":"I asked the user","messageStop":"awaiting_input"}`))
					}
				}
			}()
			if _, err := cancelSub.NextMsg(3 * time.Second); err != nil {
				return
			}
			cancelled <- struct{}{}
			send(ev(natsstream.StreamError, 0, `{"code":"cancelled","message":"cancelled"}`))
		}()
	})
	if err != nil {
		t.Fatal(err)
	}
	return results, cancelled
}

// Park: the workflow gets {"_parked":true} (its loop saves the turn and
// ends), Park ends the nats-agent turn, nothing after is relayed, and no
// cancel is needed.
func TestConsultParkEndsTurnAwaitingInput(t *testing.T) {
	_, nc := runEmbedded(t)
	results, cancelled := parkEngine(t, nc, "test.base.Park", true)
	f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{AskID: "ask_9"}, agent.ErrParked }}
	out := &recorder{}
	start := time.Now()
	_, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Park", AccountID: "a", UserID: "u", Text: "find it", Timeout: 5 * time.Second,
		AskUser: true, Asker: f,
	}, silent(), out)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("consult returned after %s, want right after the workflow ended", d)
	}
	select {
	case r := <-results:
		if string(r) != `{"_parked":true}` {
			t.Fatalf("engine got %s", r)
		}
	default:
		t.Fatal("the workflow got no _parked result")
	}
	select {
	case <-cancelled:
		t.Fatal("cancel sent although the workflow ended by itself")
	default:
	}
	if len(f.parked) != 1 || f.parked[0] != "ask_9" {
		t.Fatalf("parked = %v", f.parked)
	}
	var closedChip bool
	for _, e := range out.evs {
		if e.kind == "toolResult" && e.a == "tu1" && e.b == ask.ToolName && e.c == waitingForAnswer+"|" {
			closedChip = true
		}
		if e.kind == "text" || e.kind == "done" || e.kind == "error" {
			t.Fatalf("event after park relayed: %+v", e)
		}
	}
	if !closedChip {
		t.Fatalf("no tool result for the parked ask: %+v", out.evs)
	}
}

// If the workflow does not end after _parked, the bridge cancels it.
func TestConsultParkFallsBackToCancel(t *testing.T) {
	old := parkCancelWait
	parkCancelWait = 300 * time.Millisecond
	defer func() { parkCancelWait = old }()
	_, nc := runEmbedded(t)
	results, cancelled := parkEngine(t, nc, "test.base.Stuck", false)
	f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{AskID: "ask_8"}, agent.ErrParked }}
	if _, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Stuck", AccountID: "a", UserID: "u", Text: "q", Timeout: 5 * time.Second,
		AskUser: true, Asker: f,
	}, silent(), &recorder{}); err != nil {
		t.Fatal(err)
	}
	if r := <-results; string(r) != `{"_parked":true}` {
		t.Fatalf("engine got %s", r)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck workflow was not cancelled")
	}
}

// The workflow ends (its request timeout, an error) while the form is open:
// the ask is ended at once (its ctx cancelled: nats-agent closes the card as
// parked), nothing is published to the dead run, and the end is relayed.
func TestConsultEndsAskWhenWorkflowEnds(t *testing.T) {
	_, nc := runEmbedded(t)
	results := make(chan []byte, 1)
	_, _ = nc.Subscribe("test.base.Dies", func(m *nats.Msg) {
		host := natsstream.NewToolResultHost(nc, "test.base", "stream-1", silent())
		_ = host.Listen()
		seq := 0
		send := func(e natsstream.StreamEvent) {
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(e.Type))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(seq))
			out.Header.Set(natsstream.HdrStreamId, "stream-1")
			if seq == 0 {
				out.Header.Set(natsstream.HdrStreamToolResultPrefix, host.Prefix())
			}
			seq++
			out.Data = e.Data
			_ = nc.PublishMsg(out)
		}
		go func() {
			defer host.Stop()
			send(ev(natsstream.StreamStart, 0, `{"sessionId":"s1"}`))
			send(ev(natsstream.StreamToolCall, 0, `{"toolUseId":"tu1","name":"request_user_input","input":`+askIn+`}`))
			time.Sleep(200 * time.Millisecond)
			send(ev(natsstream.StreamError, 0, `{"code":"timeout","message":"request timeout"}`))
			if r, err := host.WaitForToolResult(context.Background(), "tu1", time.Second); err == nil {
				results <- r
			}
		}()
	})
	asked := make(chan struct{})
	f := &fakeAsker{answer: func(ask.Ask) (ask.Answer, error) { return ask.Answer{}, nil }}
	blocking := &ctxAsker{fakeAsker: f, opened: asked}
	out := &recorder{}
	start := time.Now()
	if _, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Dies", AccountID: "a", UserID: "u", Text: "q", Timeout: 5 * time.Second,
		AskUser: true, Asker: blocking,
	}, silent(), out); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("consult took %s: the open ask was not ended", d)
	}
	if !blocking.ended {
		t.Fatal("the ask's ctx was not cancelled")
	}
	select {
	case r := <-results:
		t.Fatalf("published to the ended run: %s", r)
	case <-time.After(1200 * time.Millisecond):
	}
	last := out.evs[len(out.evs)-1]
	if last.kind != "error" {
		t.Fatalf("last event = %+v", last)
	}
	if len(f.parked) != 0 {
		t.Fatalf("Park called for an ended run: %v", f.parked)
	}
}

// ctxAsker blocks until its ctx ends, like AskUser with no answer.
type ctxAsker struct {
	*fakeAsker
	opened chan struct{}
	ended  bool
}

func (c *ctxAsker) AskUser(ctx context.Context, a ask.Ask) (ask.Answer, error) {
	close(c.opened)
	<-ctx.Done()
	c.ended = true
	return ask.Answer{AskID: "ask_7"}, ctx.Err()
}

// The live wait is capped by the agent's client-tool wait and by the
// trigger's request timeout.
func TestAskWaitCap(t *testing.T) {
	wf := wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"os": plainNode{}})
	if got := engineWait(wf, fakeChatTrigger{mode: "streaming"}); got != time.Minute {
		t.Fatalf("cap = %s, want the trigger's 1m", got)
	}
}

// Rule 3 is back: a workflow with a client-only UI tool is not published.
func TestUIToolWorkflowNotEligible(t *testing.T) {
	pick, err := clientui.PickOneFactory.New(json.RawMessage(`{"toolName":"PickOne","toolDescription":"d"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"os": plainNode{}, "uiPickOne1": pick}))
	if !strings.Contains(reason, "uiPickOne1") {
		t.Fatalf("reason = %q", reason)
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

func TestRelayToolCallEmitsProgress(t *testing.T) {
	out := &recorder{}
	newRelay().apply(ev(natsstream.StreamToolCall, 0, `{"toolUseId":"t1","name":"OpenSearchQuery","input":{}}`), out)
	if len(out.progress) != 1 || out.progress[0] != "tool/OpenSearchQuery" {
		t.Fatalf("progress = %v", out.progress)
	}
}

// A new consult on the same session waits for the previous one (a parked
// turn's drain); other sessions don't; the wait is bounded.
func TestSessionLocks(t *testing.T) {
	var l sessionLocks
	unlock, err := l.acquire(context.Background(), lockKey{session: "s1"}, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if u, err := l.acquire(context.Background(), lockKey{session: "s2"}, 50*time.Millisecond, nil); err != nil {
		t.Fatalf("other session must not wait: %v", err)
	} else {
		u()
	}
	if _, err := l.acquire(context.Background(), lockKey{session: "s1"}, 50*time.Millisecond, nil); err == nil {
		t.Fatal("same session must wait and time out while held")
	}
	got := make(chan error, 1)
	go func() {
		u, err := l.acquire(context.Background(), lockKey{session: "s1"}, 2*time.Second, nil)
		if err == nil {
			u()
		}
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)
	unlock()
	if err := <-got; err != nil {
		t.Fatalf("waiter after unlock: %v", err)
	}
	l.mu.Lock()
	n := len(l.m)
	l.mu.Unlock()
	if n != 0 {
		t.Fatalf("locks not released: %d", n)
	}
}

// The drain lock is scoped to the verified identity: another caller using
// the same session id can't hold up someone else's consult.
func TestSessionKeyScopedToIdentity(t *testing.T) {
	alice := agent.Identity{AccountID: "acct-1", UserID: "u-alice", Verified: true}
	mallory := agent.Identity{AccountID: "acct-2", UserID: "u-mallory", Verified: true}
	if sessionKey(alice, "") != (lockKey{}) {
		t.Fatal("no session id: no lock")
	}
	if sessionKey(alice, "s1") == sessionKey(mallory, "s1") {
		t.Fatal("different identities must not share a key")
	}
	var l sessionLocks
	unlock, err := l.acquire(context.Background(), sessionKey(mallory, "s1"), time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if u, err := l.acquire(context.Background(), sessionKey(alice, "s1"), 50*time.Millisecond, nil); err != nil {
		t.Fatalf("other identity, same session id must not wait: %v", err)
	} else {
		u()
	}
	if _, err := l.acquire(context.Background(), sessionKey(mallory, "s1"), 50*time.Millisecond, nil); err == nil {
		t.Fatal("same identity + session must wait")
	}
}

// While waiting, the caller is told the run is alive.
func TestSessionLockWaitReportsProgress(t *testing.T) {
	var l sessionLocks
	unlock, _ := l.acquire(context.Background(), lockKey{session: "k"}, time.Second, nil)
	var waits int
	var mu sync.Mutex
	go func() { time.Sleep(80 * time.Millisecond); unlock() }()
	u, err := l.acquire(context.Background(), lockKey{session: "k"}, time.Second, func() { mu.Lock(); waits++; mu.Unlock() })
	if err != nil {
		t.Fatal(err)
	}
	u()
	mu.Lock()
	defer mu.Unlock()
	if waits < 1 {
		t.Fatal("onWait not called while waiting")
	}
	// No wait, no callback.
	u2, _ := l.acquire(context.Background(), lockKey{session: "k"}, time.Second, func() { t.Fatal("onWait on a free lock") })
	u2()
}

// The wait stays inside the caller's budget: ≤ min(consult timeout, remaining/2).
func TestLockWaitBudget(t *testing.T) {
	if got := lockWait(400*time.Second, nil); got != 400*time.Second {
		t.Fatalf("no budget = %s", got)
	}
	if got := lockWait(400*time.Second, &wire.Budget{RemainingMs: 120000}); got != 60*time.Second {
		t.Fatalf("budget 120s = %s", got)
	}
	if got := lockWait(30*time.Second, &wire.Budget{RemainingMs: 600000}); got != 30*time.Second {
		t.Fatalf("budget larger than timeout = %s", got)
	}
}

// Delimiters inside caller-controlled parts can't make two different
// (identity, session) pairs share a lock.
func TestSessionKeyNoCollisions(t *testing.T) {
	pairs := [][2]lockKey{
		{sessionKey(agent.Identity{AccountID: "a|b", UserID: "c"}, "s"), sessionKey(agent.Identity{AccountID: "a", UserID: "b|c"}, "s")},
		{sessionKey(agent.Identity{AccountID: "a", UserID: "u"}, "x/s"), sessionKey(agent.Identity{AccountID: "a", UserID: "u/x"}, "s")},
	}
	for _, p := range pairs {
		if p[0] == p[1] {
			t.Fatalf("keys collide: %+v", p)
		}
		var l sessionLocks
		unlock, err := l.acquire(context.Background(), p[0], time.Second, nil)
		if err != nil {
			t.Fatal(err)
		}
		if u, err := l.acquire(context.Background(), p[1], 50*time.Millisecond, nil); err != nil {
			t.Fatalf("%+v blocked by %+v: %v", p[1], p[0], err)
		} else {
			u()
		}
		unlock()
	}
}

// askUser is turned off when the workflow already has a request_user_input
// tool: its loop would call that tool, not the bridge.
func TestHasTool(t *testing.T) {
	pick, err := clientui.PickOneFactory.New(json.RawMessage(`{"toolName":"request_user_input","toolDescription":"d"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !hasTool(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"t": pick}), ask.ToolName) {
		t.Fatal("tool not found")
	}
	if hasTool(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"os": plainNode{}}), ask.ToolName) {
		t.Fatal("plain workflow has no such tool")
	}
}
