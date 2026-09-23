package rsassistant

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// consultRequest is one bridged call into a workflow's chat subject.
type consultRequest struct {
	Subject   string
	AccountID string // from verified identity only
	UserID    string // from verified identity only
	IDT       string // forwarded when non-empty
	SessionID string
	Text      string
	Timeout   time.Duration
}

type engineChatBody struct {
	Message      string `json:"message"`
	SessionID    string `json:"sessionId,omitempty"`
	ResponseMode string `json:"responseMode"`
}

// consult performs the engine self-call and relays its stream onto out.
// It returns the number of engine events relayed. An error is returned only
// when the request could not be sent; stream-level failures are reported to
// out as an upstream error and return nil.
func consult(ctx context.Context, nc *nats.Conn, req consultRequest, logger *log.Logger, out emitter) (int, error) {
	headers := map[string]string{
		"X-Account-Id": req.AccountID,
		"X-User-Id":    req.UserID,
	}
	if req.IDT != "" {
		headers["X-TRX-IDT"] = req.IDT
	}
	body, _ := json.Marshal(engineChatBody{Message: req.Text, SessionID: req.SessionID, ResponseMode: ModeStreaming})

	s, err := openEngineStream(nc, req.Subject, headers, body, req.Timeout, logger)
	if err != nil {
		return 0, err
	}
	r := newRelay()
	events := s.Events()
	for {
		select {
		case <-ctx.Done():
			// RSAssistant cancelled (or agent shutting down): stop the workflow.
			if cerr := s.Cancel(); cerr != nil {
				logger.Printf("RSASSISTANT event=consult.cancel_failed subject=%s err=%q", req.Subject, cerr.Error())
			}
			_ = s.Close()
			return r.count(), nil // nats-agent emits done/cancelled itself
		case ev, ok := <-events:
			if !ok {
				r.finish(s.Close(), out)
				return r.count(), nil
			}
			r.apply(ev, out)
			if r.terminated() {
				_ = s.Close()
				return r.count(), nil
			}
		}
	}
}
