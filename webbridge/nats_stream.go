package webbridge

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const streamRequestTimeout = 5 * time.Minute

// streamSubscription is the subset of *streamSubscriptionImpl the handler
// uses. Declared as an interface so tests can inject a fake.
type streamSubscription interface {
	Events() <-chan streamEvent
	Cancel() error
	Close() error
	ToolResultPrefix() string
	PublishToolResult(toolCallID string, payload []byte) error
}

// streamerFn is the mockable signature for opening a stream subscription.
// The caller owns the message construction (subject, headers, body); the
// streamer adds Reply inbox and the messageId header on top.
type streamerFn func(msg *nats.Msg) (streamSubscription, error)

var (
	natsConnOnce sync.Once
	natsConn     *nats.Conn
	natsConnErr  error

	// doStreamingRequest is overridden in tests.
	doStreamingRequest streamerFn = realDoStreamingRequest
)

func realDoStreamingRequest(msg *nats.Msg) (streamSubscription, error) {
	nc, err := getNatsConn()
	if err != nil {
		return nil, err
	}
	streamLogger := log.New(os.Stdout, "aichatviewer: ", log.LstdFlags|log.Lshortfile)
	sub, err := doNatsStreamingRequest(nc, msg, streamRequestTimeout, streamLogger)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func getNatsConn() (*nats.Conn, error) {
	natsConnOnce.Do(func() {
		url := os.Getenv("NATS_URL")
		if url == "" {
			natsConnErr = fmt.Errorf("NATS_URL missing")
			return
		}
		jwt := os.Getenv("NATS_JWT")
		key := os.Getenv("NATS_KEY")
		var opts []nats.Option
		if jwt != "" && key != "" {
			opts = append(opts, nats.UserJWTAndSeed(jwt, key))
		}
		opts = append(opts,
			nats.Name("powerlineclaimsearchwebapp-aichatviewer"),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
		nc, err := nats.Connect(url, opts...)
		if err != nil {
			natsConnErr = fmt.Errorf("aichatviewer: nats connect: %w", err)
			return
		}
		natsConn = nc
	})
	return natsConn, natsConnErr
}
