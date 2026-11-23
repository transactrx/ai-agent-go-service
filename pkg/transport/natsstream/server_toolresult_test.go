package natsstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

func TestStartEventCarriesToolResultPrefix(t *testing.T) {
	_, nc := runEmbedded(t)
	logger := newSilentLogger()

	subject := "trx.test.tr." + uuid.NewString()
	gotPrefix := make(chan string, 1)

	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		msg := &nats_service.NatsMessage{
			OriginalMessage: m,
			MessageId:       uuid.NewString(),
		}
		sess, err := natsstream.NewStreamSession(nc, msg, "test.base", logger)
		if err != nil {
			t.Errorf("NewStreamSession: %v", err)
			return
		}
		if err := sess.Start(context.Background(), natsstream.StartPayload{
			SessionID: "s", WorkflowID: "wf", RequestID: msg.MessageId,
		}); err != nil {
			t.Errorf("Start: %v", err)
			return
		}
		gotPrefix <- sess.ToolResultPrefix()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	clientInbox := nc.NewInbox()
	clientSub, err := nc.SubscribeSync(clientInbox)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer func() { _ = clientSub.Unsubscribe() }()

	if err := nc.PublishMsg(&nats.Msg{Subject: subject, Reply: clientInbox, Data: []byte(`{}`)}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	first, err := clientSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	got := first.Header.Get(natsstream.HdrStreamToolResultPrefix)
	if got == "" {
		t.Fatalf("expected start frame to carry %q header", natsstream.HdrStreamToolResultPrefix)
	}

	select {
	case server := <-gotPrefix:
		if server != got {
			t.Fatalf("server prefix %q != client-seen %q", server, got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server-side prefix")
	}
}

// The session listens from creation, so a result sent right after the start
// event (before the loop waits) reaches WaitForToolResult.
func TestSessionBuffersEarlyToolResult(t *testing.T) {
	_, nc := runEmbedded(t)
	subject := "trx.test.tr." + uuid.NewString()
	sessCh := make(chan *natsstream.StreamSession, 1)
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		sess, err := natsstream.NewStreamSession(nc, &nats_service.NatsMessage{OriginalMessage: m, MessageId: uuid.NewString()}, "test.base", newSilentLogger())
		if err != nil {
			t.Errorf("NewStreamSession: %v", err)
			return
		}
		_ = sess.Start(context.Background(), natsstream.StartPayload{SessionID: "s"})
		sessCh <- sess
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := nc.NewInbox()
	sub, _ := nc.SubscribeSync(inbox)
	defer func() { _ = sub.Unsubscribe() }()
	_ = nc.PublishMsg(&nats.Msg{Subject: subject, Reply: inbox, Data: []byte(`{}`)})
	first, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	prefix := first.Header.Get(natsstream.HdrStreamToolResultPrefix)
	_ = nc.Publish(prefix+".tu1", []byte(`{"approved":true}`))
	_ = nc.Flush()
	sess := <-sessCh
	time.Sleep(50 * time.Millisecond)
	out, err := sess.ToolResultHost().WaitForToolResult(context.Background(), "tu1", time.Second)
	if err != nil || string(out) != `{"approved":true}` {
		t.Fatalf("out=%s err=%v", out, err)
	}
	_ = sess.Complete(context.Background(), natsstream.CompletePayload{})
}
