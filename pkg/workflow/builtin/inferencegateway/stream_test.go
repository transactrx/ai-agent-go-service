package inferencegateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"strconv"
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

func TestConsumeStreamSubscriptionFailed(t *testing.T) {
	_, nc := runEmbeddedNATS(t)
	sub, err := nc.SubscribeSync(nc.NewInbox())
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	out := make(chan node.LLMEvent, 8)
	timing := &node.LLMTiming{}
	go func() {
		for range out {
		}
	}()
	err = consumeStream(context.Background(), sub, time.Second, out, timing)
	close(out)
	if err == nil || !strings.Contains(err.Error(), "subscription failed") {
		t.Fatalf("err = %v, want subscription failed", err)
	}
	if strings.Contains(err.Error(), "idle") {
		t.Fatalf("err = %v, must not mention idle", err)
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
