package rsassistant

import (
	"context"
	"io"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

func runEmbedded(t *testing.T) (*natsserver.Server, *nats.Conn) {
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

func silent() *log.Logger { return log.New(io.Discard, "", 0) }

// fakeEngine answers one streaming request on subject with a scripted stream
// and records the identity headers and body it received.
type fakeEngine struct {
	seen chan *nats.Msg
}

func startFakeEngine(t *testing.T, nc *nats.Conn, subject string, script []natsstream.StreamEvent) *fakeEngine {
	t.Helper()
	fe := &fakeEngine{seen: make(chan *nats.Msg, 4)}
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		fe.seen <- m
		for i, ev := range script {
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(ev.Type))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(i))
			out.Header.Set(natsstream.HdrStreamId, "stream-1")
			if i == 0 {
				out.Header.Set(natsstream.HdrStreamCancelSubject, "test.cancel.stream-1")
			}
			out.Data = ev.Data
			_ = nc.PublishMsg(out)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return fe
}

func TestConsultSendsIdentityHeadersAndRelaysEvents(t *testing.T) {
	_, nc := runEmbedded(t)
	fe := startFakeEngine(t, nc, "test.base.SingleSearch", []natsstream.StreamEvent{
		ev(natsstream.StreamStart, 0, `{"sessionId":"s1"}`),
		ev(natsstream.StreamDelta, 1, `{"text":"hello "}`),
		ev(natsstream.StreamDelta, 2, `{"text":"world"}`),
		ev(natsstream.StreamComplete, 3, `{"finalText":"hello world","messageStop":"end_turn"}`),
	})
	out := &recorder{}
	n, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.SingleSearch", AccountID: "acct-9", UserID: "u-1", IDT: "tok",
		SessionID: "sess-1", Text: "how many?", Timeout: 5 * time.Second,
	}, silent(), out)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("relayed %d events, want 4", n)
	}
	m := <-fe.seen
	if m.Header.Get("X-Account-Id") != "acct-9" || m.Header.Get("X-User-Id") != "u-1" || m.Header.Get("X-TRX-IDT") != "tok" {
		t.Fatalf("headers = %v", m.Header)
	}
	body := string(m.Data)
	for _, want := range []string{`"message":"how many?"`, `"sessionId":"sess-1"`, `"responseMode":"streaming"`} {
		if !contains(body, want) {
			t.Fatalf("body %s missing %s", body, want)
		}
	}
	text := ""
	for _, e := range out.evs {
		if e.kind == "text" {
			text += e.a
		}
	}
	if text != "hello world" || out.evs[len(out.evs)-1].kind != "done" {
		t.Fatalf("events = %+v", out.evs)
	}
}

func TestConsultTimeoutEndsWithUpstreamError(t *testing.T) {
	_, nc := runEmbedded(t)
	startFakeEngine(t, nc, "test.base.Slow", []natsstream.StreamEvent{
		ev(natsstream.StreamStart, 0, `{}`),
		ev(natsstream.StreamDelta, 1, `{"text":"partial"}`),
		// no terminator
	})
	out := &recorder{}
	_, _ = consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Slow", AccountID: "a", UserID: "u", Text: "x", Timeout: 300 * time.Millisecond,
	}, silent(), out)
	last := out.evs[len(out.evs)-1]
	if last.kind != "error" || !contains(last.a, "timed out") {
		t.Fatalf("expected upstream timeout error, got %+v", out.evs)
	}
}

func TestConsultCancelPublishesToCancelSubject(t *testing.T) {
	_, nc := runEmbedded(t)
	cancelled := make(chan struct{}, 1)
	_, _ = nc.Subscribe("test.cancel.stream-1", func(*nats.Msg) { cancelled <- struct{}{} })
	startFakeEngine(t, nc, "test.base.Hang", []natsstream.StreamEvent{
		ev(natsstream.StreamStart, 0, `{}`),
	})
	ctx, cancel := context.WithCancel(context.Background())
	out := &recorder{}
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	_, _ = consult(ctx, nc, consultRequest{Subject: "test.base.Hang", AccountID: "a", UserID: "u", Text: "x", Timeout: 5 * time.Second}, silent(), out)
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel was not propagated to the engine cancel subject")
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func TestConsultServiceErrorReplyFailsFast(t *testing.T) {
	_, nc := runEmbedded(t)
	// Engine rejects before streaming (e.g. missing account header): nats-service
	// error reply with a status header and no _Stream_Event.
	_, _ = nc.Subscribe("test.base.Reject", func(m *nats.Msg) {
		out := nats.NewMsg(m.Reply)
		out.Header.Set(natsstream.HdrStatus, "403")
		out.Data = []byte(`{"status":403,"errorMessage":"IDT validation failed: DENIED_FN","apiStatusCode":1001}`)
		_ = nc.PublishMsg(out)
	})
	out := &recorder{}
	start := time.Now()
	_, err := consult(context.Background(), nc, consultRequest{Subject: "test.base.Reject", AccountID: "a", UserID: "u", Text: "x", Timeout: 10 * time.Second}, silent(), out)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("rejection must fail fast, took %s", time.Since(start))
	}
	last := out.evs[len(out.evs)-1]
	if last.kind != "error" || !contains(last.a, "status_403") || !contains(last.a, "DENIED_FN") {
		t.Fatalf("expected status error, got %+v", out.evs)
	}
}

func TestConsultNoRespondersFailsFast(t *testing.T) {
	_, nc := runEmbedded(t)
	out := &recorder{}
	start := time.Now()
	_, _ = consult(context.Background(), nc, consultRequest{Subject: "test.base.Nobody", AccountID: "a", UserID: "u", Text: "x", Timeout: 10 * time.Second}, silent(), out)
	if time.Since(start) > 2*time.Second {
		t.Fatalf("no responders must fail fast, took %s", time.Since(start))
	}
	if len(out.evs) == 0 || out.evs[len(out.evs)-1].kind != "error" || !contains(out.evs[len(out.evs)-1].a, "no_responders") {
		t.Fatalf("expected no_responders error, got %+v", out.evs)
	}
}

// TestEngineStreamLateEventsAfterCloseDoNotPanic floods events after the
// consumer closed the stream (cancel/timeout race). Run with -race.
func TestEngineStreamLateEventsAfterCloseDoNotPanic(t *testing.T) {
	_, nc := runEmbedded(t)
	_, _ = nc.Subscribe("test.base.Flood", func(m *nats.Msg) {
		for i := 0; i < 500; i++ {
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(natsstream.StreamDelta))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(i))
			out.Data = []byte(`{"text":"x"}`)
			_ = nc.PublishMsg(out)
		}
	})
	for i := 0; i < 20; i++ {
		s, err := openEngineStream(nc, "test.base.Flood", nil, []byte(`{}`), time.Millisecond, silent())
		if err != nil {
			t.Fatal(err)
		}
		<-s.Events()
		_ = s.Close()
	}
	_ = nc.Flush()
	time.Sleep(100 * time.Millisecond)
}
