package natsstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
)

// StreamSession is one server-side streaming session, scoped to a single
// client request. Owns: cancel subscription, sequence counter, lifecycle.
type StreamSession struct {
	nc         *nats.Conn
	replyInbox string
	streamID   string
	cancelSubj string
	cancelSub  *nats.Subscription

	seq    atomic.Int64
	closed atomic.Bool

	cancelCtx context.Context
	cancelFn  context.CancelFunc

	logger         *log.Logger
	basePath       string
	toolResultHost *ToolResultHost
}

// ErrClosed is returned when Send is called after a terminator.
var ErrClosed = errors.New("natsstream: session already closed")

// NewStreamSession initializes a session from a nats-service NatsMessage.
// basePath is the service base path (used to namespace the cancel subject).
//
// The handler that creates this session MUST return Status:302 so nats-service
// does NOT auto-respond — events are delivered manually on the reply inbox.
func NewStreamSession(nc *nats.Conn, msg *nats_service.NatsMessage, basePath string, logger *log.Logger) (*StreamSession, error) {
	if msg == nil || msg.OriginalMessage == nil {
		return nil, errors.New("natsstream: nil NatsMessage or OriginalMessage")
	}
	if msg.OriginalMessage.Reply == "" {
		return nil, errors.New("natsstream: request has no reply inbox; not a request/reply call")
	}

	streamID := msg.MessageId
	if streamID == "" {
		streamID = uuid.NewString()
	}

	cancelSubj := fmt.Sprintf("%s_.stream_cancel.%s", basePath, uuid.NewString())
	s := &StreamSession{
		nc:         nc,
		replyInbox: msg.OriginalMessage.Reply,
		streamID:   streamID,
		cancelSubj: cancelSubj,
		logger:     logger,
	}
	s.cancelCtx, s.cancelFn = context.WithCancel(context.Background())
	s.basePath = basePath
	s.toolResultHost = NewToolResultHost(nc, basePath, streamID, logger)

	sub, err := nc.Subscribe(cancelSubj, func(_ *nats.Msg) {
		s.cancelFn()
	})
	if err != nil {
		return nil, fmt.Errorf("natsstream: subscribe to cancel subject: %w", err)
	}
	s.cancelSub = sub
	return s, nil
}

// Context returns a context canceled when the client signals cancel.
func (s *StreamSession) Context() context.Context { return s.cancelCtx }

// IsCancelled reports whether the cancel ctx has fired.
func (s *StreamSession) IsCancelled() bool { return s.cancelCtx.Err() != nil }

// StreamID returns the unique id correlating all events of this session.
func (s *StreamSession) StreamID() string { return s.streamID }

// CancelSubject returns the server-allocated subject the client publishes to
// in order to cancel.
func (s *StreamSession) CancelSubject() string { return s.cancelSubj }

// Start sends the first event (sequence 0). Must be called once before any
// Send/Complete/Fail.
func (s *StreamSession) Start(_ context.Context, p StartPayload) error {
	return s.publish(StreamEvent{Type: StreamStart, Data: MustJSON(p)}, false)
}

// Send emits a non-terminator event.
func (s *StreamSession) Send(_ context.Context, evt StreamEvent) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if evt.Type == StreamComplete || evt.Type == StreamError {
		return s.terminate(evt)
	}
	return s.publish(evt, false)
}

// Complete emits the success terminator and tears down.
func (s *StreamSession) Complete(_ context.Context, p CompletePayload) error {
	return s.terminate(StreamEvent{Type: StreamComplete, Data: MustJSON(p)})
}

// Fail emits the error terminator and tears down.
func (s *StreamSession) Fail(_ context.Context, p ErrorPayload) error {
	return s.terminate(StreamEvent{Type: StreamError, Data: MustJSON(p)})
}

func (s *StreamSession) terminate(evt StreamEvent) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil // idempotent
	}
	if s.cancelSub != nil {
		_ = s.cancelSub.Unsubscribe()
	}
	s.cancelFn()
	return s.publish(evt, true)
}

func (s *StreamSession) publish(evt StreamEvent, _ bool) error {
	seq := s.seq.Add(1) - 1
	msg := &nats.Msg{
		Subject: s.replyInbox,
		Header:  nats.Header{},
		Data:    evt.Data,
	}
	msg.Header.Set(HdrStreamEvent, string(evt.Type))
	msg.Header.Set(HdrStreamSequence, strconv.FormatInt(seq, 10))
	msg.Header.Set(HdrStreamId, s.streamID)
	msg.Header.Set(HdrMessageId, s.streamID)
	if seq == 0 {
		msg.Header.Set(HdrStreamCancelSubject, s.cancelSubj)
		msg.Header.Set(HdrStreamToolResultPrefix, s.toolResultHost.Prefix())
	}
	if evt.Type == StreamError {
		msg.Header.Set(HdrStatus, "500")
	} else {
		msg.Header.Set(HdrStatus, "200")
	}
	for k, v := range evt.Header {
		msg.Header.Set(k, v)
	}
	return s.nc.PublishMsg(msg)
}

// ToolResultHost returns the per-session host for awaiting client-side tool
// results. Lives only as long as the session.
func (s *StreamSession) ToolResultHost() *ToolResultHost { return s.toolResultHost }

// ToolResultPrefix is the subject prefix clients use to publish tool results.
// Format: "<basePath>_.tool_result.<streamID>".
func (s *StreamSession) ToolResultPrefix() string { return s.toolResultHost.Prefix() }

// jsonRawForTest is exported for testing the publish path with a raw payload.
// Hidden from public API by alias name.
var _ = json.RawMessage(nil)
