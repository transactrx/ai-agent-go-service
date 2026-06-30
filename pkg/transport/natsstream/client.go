package natsstream

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// ErrCancelNotReady is returned when Cancel is called before the start event
// has been received (cancel subject is announced in start headers).
var ErrCancelNotReady = errors.New("natsstream: cannot cancel before start event arrives")

// StreamSubscription is one client-side streaming consumer.
type StreamSubscription struct {
	nc     *nats.Conn
	inbox  string
	sub    *nats.Subscription
	events chan StreamEvent
	done   chan struct{}
	err    atomic.Pointer[error]

	cancelSubj atomic.Pointer[string]
	streamID   atomic.Pointer[string]

	closeOnce sync.Once
	logger    *log.Logger
}

// DoStreamingRequest sends a streaming request and returns a subscription. The
// caller reads events from sub.Events() until the channel closes (terminator
// received, timeout, or sub.Close()). Out-of-order events are dropped with a
// logged warning.
func DoStreamingRequest(
	nc *nats.Conn,
	subject string,
	headers map[string]string,
	body []byte,
	timeout time.Duration,
	logger *log.Logger,
) (*StreamSubscription, error) {
	inbox := nc.NewInbox()
	sub := &StreamSubscription{
		nc:     nc,
		inbox:  inbox,
		events: make(chan StreamEvent, 32),
		done:   make(chan struct{}),
		logger: logger,
	}

	var nextSeq atomic.Int64
	natsSub, err := nc.Subscribe(inbox, func(m *nats.Msg) {
		evt, parseErr := ParseStreamMessage(m)
		if parseErr != nil {
			logger.Printf("natsstream client: parse error: %v", parseErr)
			return
		}
		want := int(nextSeq.Load())
		if evt.Sequence != want {
			logger.Printf("natsstream client: out-of-order event (got %d, want %d) — dropping", evt.Sequence, want)
			return
		}
		nextSeq.Add(1)

		if evt.Sequence == 0 {
			cs := m.Header.Get(HdrStreamCancelSubject)
			sub.cancelSubj.Store(&cs)
			id := m.Header.Get(HdrStreamId)
			sub.streamID.Store(&id)
		}

		select {
		case sub.events <- evt:
		case <-sub.done:
			return
		}

		if evt.Type == StreamComplete || evt.Type == StreamError {
			sub.closeInternal()
		}
	})
	if err != nil {
		return nil, fmt.Errorf("natsstream: subscribe to inbox: %w", err)
	}
	sub.sub = natsSub

	msg := &nats.Msg{
		Subject: subject,
		Reply:   inbox,
		Header:  nats.Header{},
		Data:    body,
	}
	for k, v := range headers {
		msg.Header.Set(k, v)
	}
	if msg.Header.Get(HdrMessageId) == "" {
		msg.Header.Set(HdrMessageId, uuid.NewString())
	}

	if err := nc.PublishMsg(msg); err != nil {
		_ = natsSub.Unsubscribe()
		sub.closeInternal()
		return nil, fmt.Errorf("natsstream: publish request: %w", err)
	}

	if timeout > 0 {
		time.AfterFunc(timeout, func() {
			sub.setErr(fmt.Errorf("natsstream: timed out after %s", timeout))
			sub.closeInternal()
		})
	}

	return sub, nil
}

// Events returns the channel of received events. Closes when the subscription
// terminates (terminator event, timeout, or explicit Close).
func (s *StreamSubscription) Events() <-chan StreamEvent { return s.events }

// Done is closed when the subscription terminates.
func (s *StreamSubscription) Done() <-chan struct{} { return s.done }

// StreamID returns the server-assigned stream id, or "" if start hasn't arrived.
func (s *StreamSubscription) StreamID() string {
	if p := s.streamID.Load(); p != nil {
		return *p
	}
	return ""
}

// Cancel publishes on the server-allocated cancel subject. Available only
// after the start event has been received.
func (s *StreamSubscription) Cancel() error {
	p := s.cancelSubj.Load()
	if p == nil || *p == "" {
		return ErrCancelNotReady
	}
	return s.nc.Publish(*p, []byte("cancel"))
}

// Close releases the subscription and any goroutines. Idempotent.
func (s *StreamSubscription) Close() error {
	s.closeInternal()
	if e := s.err.Load(); e != nil {
		return *e
	}
	return nil
}

func (s *StreamSubscription) closeInternal() {
	s.closeOnce.Do(func() {
		if s.sub != nil {
			_ = s.sub.Unsubscribe()
		}
		close(s.done)
		close(s.events)
	})
}

func (s *StreamSubscription) setErr(err error) { s.err.Store(&err) }
