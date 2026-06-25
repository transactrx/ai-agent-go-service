package natsstream_test

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

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

func newSilentLogger() *log.Logger { return log.New(io.Discard, "", 0) }

func TestServerClientRoundTrip(t *testing.T) {
	_, nc := runEmbedded(t)
	logger := newSilentLogger()

	const subject = "trx.test.echo"
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		ns := &nats_service.NatsMessage{
			MessageId:       "msg-1",
			OriginalMessage: m,
		}
		sess, err := natsstream.NewStreamSession(nc, ns, "trx.test", logger)
		if err != nil {
			t.Errorf("server NewStreamSession: %v", err)
			return
		}
		_ = sess.Start(context.Background(), natsstream.StartPayload{SessionID: "s1", WorkflowID: "wf", RequestID: "r"})
		_ = sess.Send(context.Background(), natsstream.StreamEvent{
			Type: natsstream.StreamDelta, Data: []byte(`{"text":"hi"}`),
		})
		_ = sess.Complete(context.Background(), natsstream.CompletePayload{FinalText: "hi", MessageStop: "end_turn"})
	})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := natsstream.DoStreamingRequest(nc, subject, nil, []byte(`{}`), 5*time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	var got []natsstream.StreamEventType
	timeout := time.After(3 * time.Second)
loop:
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				break loop
			}
			got = append(got, ev.Type)
		case <-timeout:
			t.Fatalf("timeout; got events so far: %v", got)
		}
	}

	want := []natsstream.StreamEventType{natsstream.StreamStart, natsstream.StreamDelta, natsstream.StreamComplete}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestCancelFiresContext(t *testing.T) {
	_, nc := runEmbedded(t)
	logger := newSilentLogger()

	const subject = "trx.test.cancel"
	cancelObserved := make(chan struct{}, 1)
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		ns := &nats_service.NatsMessage{MessageId: "msg-2", OriginalMessage: m}
		sess, err := natsstream.NewStreamSession(nc, ns, "trx.test", logger)
		if err != nil {
			t.Errorf("server NewStreamSession: %v", err)
			return
		}
		_ = sess.Start(context.Background(), natsstream.StartPayload{SessionID: "s1"})

		go func() {
			<-sess.Context().Done()
			cancelObserved <- struct{}{}
			_ = sess.Fail(context.Background(), natsstream.ErrorPayload{Code: "cancelled"})
		}()
	})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := natsstream.DoStreamingRequest(nc, subject, nil, []byte(`{}`), 5*time.Second, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	select {
	case ev := <-sub.Events():
		if ev.Type != natsstream.StreamStart {
			t.Fatalf("expected start first, got %v", ev.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never saw start")
	}

	if err := sub.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	select {
	case <-cancelObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("server context never observed cancel")
	}
}

func TestTerminatorIdempotent(t *testing.T) {
	_, nc := runEmbedded(t)
	logger := newSilentLogger()
	const subject = "trx.test.idem"

	doneCh := make(chan error, 1)
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		ns := &nats_service.NatsMessage{MessageId: "msg-3", OriginalMessage: m}
		sess, _ := natsstream.NewStreamSession(nc, ns, "trx.test", logger)
		_ = sess.Start(context.Background(), natsstream.StartPayload{})
		_ = sess.Complete(context.Background(), natsstream.CompletePayload{})
		// Second termination is a no-op (idempotent).
		err := sess.Complete(context.Background(), natsstream.CompletePayload{})
		doneCh <- err
	})
	if err != nil {
		t.Fatal(err)
	}

	sub, _ := natsstream.DoStreamingRequest(nc, subject, nil, []byte(`{}`), 5*time.Second, logger)
	defer sub.Close()

	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("second Complete returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server didn't run")
	}
}
