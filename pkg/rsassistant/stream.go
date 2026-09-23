package rsassistant

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

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

// engineStream is the bridge's own client for the engine's natsstream
// protocol. It is a dedicated sibling of natsstream.DoStreamingRequest with two
// differences the bridge needs:
//
//   - a non-stream reply (nats-service error with a non-200 "status" header,
//     or the NATS server's 503 no-responders notice) ends the stream with an
//     error event instead of being dropped, so a rejected request fails fast
//     rather than waiting for the consult timeout;
//   - delivery and close are serialized, so a late engine event racing a
//     timeout or cancel can never send on a closed channel.
type engineStream struct {
	nc     *nats.Conn
	sub    *nats.Subscription
	events chan natsstream.StreamEvent
	done   chan struct{}

	mu       sync.Mutex // guards closed + sends on events
	closed   bool
	doneOnce sync.Once

	nextSeq    atomic.Int64
	cancelSubj atomic.Pointer[string]
	err        atomic.Pointer[error]
	logger     *log.Logger
}

// errorEvent is what engineStream synthesizes for a non-stream reply. Its
// payload is a natsstream.ErrorPayload so the relay handles both identically.
func errorEvent(code, message string) natsstream.StreamEvent {
	data, _ := json.Marshal(natsstream.ErrorPayload{Code: code, Message: message})
	return natsstream.StreamEvent{Type: natsstream.StreamError, Data: data}
}

// openEngineStream publishes one streaming request on subject and returns the
// live stream. Events() closes after a terminator, a timeout, or Close().
func openEngineStream(nc *nats.Conn, subject string, headers map[string]string, body []byte, timeout time.Duration, logger *log.Logger) (*engineStream, error) {
	s := &engineStream{
		nc:     nc,
		events: make(chan natsstream.StreamEvent, 64),
		done:   make(chan struct{}),
		logger: logger,
	}
	inbox := nc.NewInbox()
	sub, err := nc.Subscribe(inbox, s.onMsg)
	if err != nil {
		return nil, fmt.Errorf("rsassistant: subscribe inbox: %w", err)
	}
	s.sub = sub

	msg := &nats.Msg{Subject: subject, Reply: inbox, Header: nats.Header{}, Data: body}
	for k, v := range headers {
		msg.Header.Set(k, v)
	}
	msg.Header.Set(natsstream.HdrMessageId, uuid.NewString())
	if err := nc.PublishMsg(msg); err != nil {
		s.close()
		return nil, fmt.Errorf("rsassistant: publish request: %w", err)
	}
	if timeout > 0 {
		time.AfterFunc(timeout, func() {
			s.setErr(fmt.Errorf("timed out after %s", timeout))
			s.close()
		})
	}
	return s, nil
}

func (s *engineStream) onMsg(m *nats.Msg) {
	// NATS server no-responders notice (nats.go exposes it as Status 503).
	if m.Header.Get("Status") == "503" {
		s.deliver(errorEvent("no_responders", "no workflow instance is subscribed to the chat subject"))
		s.close()
		return
	}
	// nats-service error reply: status header set, no stream event header.
	if st := m.Header.Get(natsstream.HdrStatus); st != "" && st != "200" && m.Header.Get(natsstream.HdrStreamEvent) == "" {
		s.deliver(errorEvent("status_"+st, serviceErrorText(m.Data)))
		s.close()
		return
	}
	ev, err := natsstream.ParseStreamMessage(m)
	if err != nil {
		s.logger.Printf("RSASSISTANT event=stream.parse_error err=%q", err.Error())
		return
	}
	want := int(s.nextSeq.Load())
	if ev.Sequence != want {
		s.logger.Printf("RSASSISTANT event=stream.out_of_order got=%d want=%d", ev.Sequence, want)
		return
	}
	s.nextSeq.Add(1)
	if ev.Sequence == 0 {
		cs := m.Header.Get(natsstream.HdrStreamCancelSubject)
		s.cancelSubj.Store(&cs)
	}
	s.deliver(ev)
	if ev.Type == natsstream.StreamComplete || ev.Type == natsstream.StreamError {
		s.close()
	}
}

// deliver sends ev unless the stream is closed. Sending under mu guarantees
// close() cannot close events mid-send; the done case unblocks a send when
// the consumer stopped reading.
func (s *engineStream) deliver(ev natsstream.StreamEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.events <- ev:
	case <-s.done:
	}
}

// close is idempotent and safe from any goroutine.
func (s *engineStream) close() {
	s.doneOnce.Do(func() { close(s.done) })
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.sub != nil {
		_ = s.sub.Unsubscribe()
	}
	close(s.events)
}

func (s *engineStream) setErr(err error) { s.err.CompareAndSwap(nil, &err) }

// Events delivers engine events in sequence order; closed on termination.
func (s *engineStream) Events() <-chan natsstream.StreamEvent { return s.events }

// Cancel asks the engine to stop the run. Requires the start event.
func (s *engineStream) Cancel() error {
	p := s.cancelSubj.Load()
	if p == nil || *p == "" {
		return errors.New("cancel subject not known yet (no start event)")
	}
	return s.nc.Publish(*p, []byte("cancel"))
}

// Close releases the stream and returns the timeout error, if any.
func (s *engineStream) Close() error {
	s.close()
	if e := s.err.Load(); e != nil {
		return *e
	}
	return nil
}

// serviceErrorText renders a nats-service error body for the caller.
func serviceErrorText(body []byte) string {
	var e struct {
		ErrorMessage  string `json:"errorMessage"`
		ApiStatusCode int    `json:"apiStatusCode"`
	}
	if json.Unmarshal(body, &e) == nil && e.ErrorMessage != "" {
		if e.ApiStatusCode != 0 {
			return e.ErrorMessage + " (code " + strconv.Itoa(e.ApiStatusCode) + ")"
		}
		return e.ErrorMessage
	}
	if len(body) > 300 {
		body = body[:300]
	}
	return string(body)
}
