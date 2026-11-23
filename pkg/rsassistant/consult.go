package rsassistant

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

// consultRequest is one bridged call into a workflow's chat subject.
type consultRequest struct {
	Subject   string
	AccountID string // from verified identity only
	UserID    string // from verified identity only
	IDT       string // forwarded when non-empty
	TimeZone  string // display only: caller's normalized context (X-Time-Zone)
	UserName  string // display only: caller's normalized context (X-User-Name)
	SessionID string
	Text      string
	Timeout   time.Duration

	// UI and Asker answer the workflow's client-UI tool calls by asking the
	// user (nil UI: the workflow has none).
	UI    *uiBridge
	Asker userAsker
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
	if req.TimeZone != "" {
		headers["X-Time-Zone"] = req.TimeZone
	}
	if req.UserName != "" {
		headers["X-User-Name"] = req.UserName
	}
	body, _ := json.Marshal(engineChatBody{Message: req.Text, SessionID: req.SessionID, ResponseMode: ModeStreaming})

	s, err := openEngineStream(nc, req.Subject, headers, body, req.Timeout, logger)
	if err != nil {
		return 0, err
	}
	r := newRelay()
	parked := false // the turn ended awaiting input; drain the workflow
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
				if parked {
					_ = s.Close()
					return r.count(), nil
				}
				r.finish(s.Close(), out)
				return r.count(), nil
			}
			r.apply(ev, out)
			if ev.Type == natsstream.StreamToolCall && req.UI != nil && req.Asker != nil {
				if askedPark, call := answerClientUI(ctx, req, s, ev, logger); askedPark != "" && !parked {
					// Close the relayed tool chip, then Park ends the nats-agent
					// turn (askClosed{parked} + done awaiting_input). The
					// workflow got {"_skipped":true}; let it finish so its
					// memory keeps the turn, relaying nothing.
					out.ToolResult(call.ToolUseID, call.Name, waitingForAnswer, "")
					req.Asker.Park(askedPark)
					r.done = true
					parked = true
				}
			}
			if r.terminated() && !parked {
				_ = s.Close()
				return r.count(), nil
			}
		}
	}
}

// waitingForAnswer is the tool result shown for a UI tool whose ask parked.
const waitingForAnswer = "waiting for the user's answer"

// answerClientUI answers one client-UI tool call (if it is one) and returns
// the parked ask id when the live wait ran out, with the call.
func answerClientUI(ctx context.Context, req consultRequest, s *engineStream, ev natsstream.StreamEvent, logger *log.Logger) (string, toolCallPayload) {
	var p toolCallPayload
	if json.Unmarshal(ev.Data, &p) != nil || !req.UI.owns(p.Name) {
		return "", p
	}
	resume := s.Pause() // the user's think time is not workflow time
	payload, parked, err := req.UI.answer(ctx, req.Asker, p.Name, p.Input)
	resume()
	if err != nil {
		return "", p // ctx ended: the select above stops the workflow
	}
	if perr := s.PublishToolResult(p.ToolUseID, payload); perr != nil {
		logger.Printf("RSASSISTANT event=ui_tool.publish_failed tool=%s err=%q", p.Name, perr.Error())
	}
	return parked, p
}

// userTimeZone and userName read the turn's display context (already
// normalized by the nats-agent runtime). Empty when absent.
func userTimeZone(c *wire.UserContext) string {
	if c == nil {
		return ""
	}
	return c.TimeZone
}

func userName(c *wire.UserContext) string {
	if c == nil {
		return ""
	}
	return c.UserName
}
