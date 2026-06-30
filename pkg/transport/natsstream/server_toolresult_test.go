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
