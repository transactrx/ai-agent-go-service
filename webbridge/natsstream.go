package webbridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// This file inlines the small subset of the opensearchAiChatApi
// `pkg/transport/natsstream` package that the bridge needs. Inlined rather
// than imported because that source repo is private; depending on it would
// require authenticated module fetches in CI. The wire protocol is documented
// in docs/superpowers/specs/2026-05-06-ai-chat-viewer-design.md §3.

// NATS header names exchanged with opensearchAiChatApi over the multi-response
// stream. Underscore-prefixed per the nats-service convention.
const (
	hdrStreamEvent           = "_Stream_Event"
	hdrStreamSequence        = "_Stream_Sequence"
	hdrStreamId              = "_Stream_Id"
	hdrStreamCancelSubject   = "_Stream_Cancel_Subject"
	hdrStreamToolResultPrefix = "_Stream_Tool_Result_Prefix"
	hdrMessageId             = "_Message_Id"
)

// streamEventType enumerates wire-level event names.
type streamEventType string

const (
	streamStart      streamEventType = "start"
	streamDelta      streamEventType = "delta"
	streamThought    streamEventType = "thought"
	streamToolCall   streamEventType = "tool_call"
	streamToolResult streamEventType = "tool_result"
	streamComplete   streamEventType = "complete"
	streamError      streamEventType = "error"
)

// streamEvent is one parsed wire-level event passed to the bridge.
type streamEvent struct {
	Type     streamEventType
	Sequence int
	StreamID string
	Data     json.RawMessage
}

// startPayload mirrors the `start` event body shape produced by the agent.
type startPayload struct {
	SessionID  string `json:"sessionId"`
	WorkflowID string `json:"workflowId"`
	RequestID  string `json:"requestId"`
}

// streamSubscriptionImpl is the concrete subscription returned by
// doNatsStreamingRequest. It satisfies the streamSubscription interface
// declared in nats_stream.go.
type streamSubscriptionImpl struct {
	nc     *nats.Conn
	sub    *nats.Subscription
	events chan streamEvent
	done   chan struct{}
	err    atomic.Pointer[error]

	cancelSubj       atomic.Pointer[string]
	toolResultPrefix atomic.Pointer[string]

	closeOnce sync.Once
	logger    *log.Logger

	// cleanups runs in closeInternal after the subscription is shut down.
	// Holds the chunk-serving subscription's Unsubscribe when the outbound
	// request was chunked; otherwise empty.
	cleanups []func()
}

// doNatsStreamingRequest sends a streaming request to a NATS multi-response
// endpoint and returns a subscription. Caller reads sub.Events() until the
// channel closes (terminator received, timeout, or sub.Close()). Out-of-order
// events are dropped with a logged warning.
func doNatsStreamingRequest(
	nc *nats.Conn,
	msg *nats.Msg,
	timeout time.Duration,
	logger *log.Logger,
) (*streamSubscriptionImpl, error) {
	if msg == nil {
		return nil, errors.New("aichatviewer: nil msg")
	}
	inbox := nc.NewInbox()
	sub := &streamSubscriptionImpl{
		nc:     nc,
		events: make(chan streamEvent, 32),
		done:   make(chan struct{}),
		logger: logger,
	}

	var nextSeq atomic.Int64
	natsSub, err := nc.Subscribe(inbox, func(m *nats.Msg) {
		evt, parseErr := parseStreamMessage(m)
		if parseErr != nil {
			logger.Printf("aichatviewer: parse error: %v", parseErr)
			return
		}
		want := int(nextSeq.Load())
		if evt.Sequence != want {
			logger.Printf("aichatviewer: out-of-order event (got %d, want %d) — dropping", evt.Sequence, want)
			return
		}
		nextSeq.Add(1)

		if evt.Sequence == 0 {
			cs := m.Header.Get(hdrStreamCancelSubject)
			sub.cancelSubj.Store(&cs)
			trp := m.Header.Get(hdrStreamToolResultPrefix)
			sub.toolResultPrefix.Store(&trp)
		}

		select {
		case sub.events <- evt:
		case <-sub.done:
			return
		}

		if evt.Type == streamComplete || evt.Type == streamError {
			sub.closeInternal()
		}
	})
	if err != nil {
		return nil, fmt.Errorf("aichatviewer: subscribe inbox: %w", err)
	}
	sub.sub = natsSub

	msg.Reply = inbox
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	if msg.Header.Get(hdrMessageId) == "" {
		msg.Header.Set(hdrMessageId, uuid.NewString())
	}

	// Apply nats-service compress+chunk protocol if the body is large.
	// Avoids hitting NATS server max_payload when user attachments are
	// inlined as base64 in the request envelope.
	chunkCleanup, cerr := prepareLargeRequest(nc, msg, logger)
	if cerr != nil {
		_ = natsSub.Unsubscribe()
		sub.closeInternal()
		return nil, cerr
	}
	sub.cleanups = append(sub.cleanups, chunkCleanup)

	if err := nc.PublishMsg(msg); err != nil {
		_ = natsSub.Unsubscribe()
		sub.closeInternal()
		return nil, fmt.Errorf("aichatviewer: publish request: %w", err)
	}

	if timeout > 0 {
		time.AfterFunc(timeout, func() {
			sub.setErr(fmt.Errorf("aichatviewer: timed out after %s", timeout))
			sub.closeInternal()
		})
	}

	return sub, nil
}

// Events returns the channel of incoming events. Closes when the
// subscription terminates.
func (s *streamSubscriptionImpl) Events() <-chan streamEvent { return s.events }

// Cancel publishes on the server-allocated cancel subject. Returns an error
// if the start event has not yet been received.
func (s *streamSubscriptionImpl) Cancel() error {
	p := s.cancelSubj.Load()
	if p == nil || *p == "" {
		return errors.New("aichatviewer: cannot cancel before start event arrives")
	}
	return s.nc.Publish(*p, []byte("cancel"))
}

// ToolResultPrefix returns the subject prefix announced by the server in the
// start frame. Clients publish to "<prefix>.<toolCallId>" to deliver a tool
// result. Empty string if start hasn't arrived yet OR the server doesn't
// announce one (legacy server).
func (s *streamSubscriptionImpl) ToolResultPrefix() string {
	if p := s.toolResultPrefix.Load(); p != nil {
		return *p
	}
	return ""
}

// PublishToolResult publishes a tool result payload to the per-stream subject
// "<prefix>.<toolCallId>". Returns an error if the prefix isn't known yet
// (start event hasn't arrived) or the publish fails.
func (s *streamSubscriptionImpl) PublishToolResult(toolCallID string, payload []byte) error {
	prefix := s.ToolResultPrefix()
	if prefix == "" {
		return fmt.Errorf("aichatviewer: no tool-result prefix announced; cannot route tool_result_from_client")
	}
	return s.nc.Publish(prefix+"."+toolCallID, payload)
}

// Close releases the subscription. Idempotent.
func (s *streamSubscriptionImpl) Close() error {
	s.closeInternal()
	if e := s.err.Load(); e != nil {
		return *e
	}
	return nil
}

func (s *streamSubscriptionImpl) closeInternal() {
	s.closeOnce.Do(func() {
		if s.sub != nil {
			_ = s.sub.Unsubscribe()
		}
		for _, fn := range s.cleanups {
			if fn != nil {
				fn()
			}
		}
		close(s.done)
		close(s.events)
	})
}

func (s *streamSubscriptionImpl) setErr(err error) { s.err.Store(&err) }

// parseStreamMessage extracts a streamEvent from a *nats.Msg arriving on the
// reply inbox. Returns an error on malformed headers.
//
// nats-service error responses arrive with a numeric "status" header (set by
// nats-service.go on the agent side when a handler returns NatsServiceError).
// These have no _Stream_Event header. We synthesize a streamError event so
// the bridge can surface the failure to the WS client (e.g. agent-side IDT
// validation denial returns 403 here).
func parseStreamMessage(m *nats.Msg) (streamEvent, error) {
	if m == nil {
		return streamEvent{}, errors.New("nil msg")
	}
	if status := m.Header.Get("status"); status != "" && status != "200" {
		return streamEvent{
			Type:     streamError,
			Sequence: 0,
			StreamID: m.Header.Get(hdrStreamId),
			Data:     json.RawMessage(m.Data),
		}, nil
	}
	evtName := m.Header.Get(hdrStreamEvent)
	if evtName == "" {
		return streamEvent{}, errors.New("missing _Stream_Event")
	}
	seqStr := m.Header.Get(hdrStreamSequence)
	seq, err := strconv.Atoi(seqStr)
	if err != nil {
		return streamEvent{}, fmt.Errorf("bad _Stream_Sequence %q: %w", seqStr, err)
	}
	return streamEvent{
		Type:     streamEventType(evtName),
		Sequence: seq,
		StreamID: m.Header.Get(hdrStreamId),
		Data:     json.RawMessage(m.Data),
	}, nil
}
