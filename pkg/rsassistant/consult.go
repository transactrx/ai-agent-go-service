package rsassistant

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/transactrx/nats-agent/pkg/ask"
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

	// AskUser offers request_user_input to the workflow run; Asker answers
	// its calls by asking the user. Both set only for an askUser agent whose
	// caller can show forms.
	AskUser bool
	Asker   userAsker
}

type engineChatBody struct {
	Message      string `json:"message"`
	SessionID    string `json:"sessionId,omitempty"`
	ResponseMode string `json:"responseMode"`
	AskUser      bool   `json:"askUser,omitempty"` // natschat streamRunContext
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
	body, _ := json.Marshal(engineChatBody{Message: req.Text, SessionID: req.SessionID, ResponseMode: ModeStreaming,
		AskUser: req.AskUser && req.Asker != nil})

	s, err := openEngineStream(nc, req.Subject, headers, body, req.Timeout, logger)
	if err != nil {
		return 0, err
	}
	r := newRelay()
	events := s.Events()
	var held []natsstream.StreamEvent // read while an ask was open, not yet relayed
	ended := false                    // the workflow ended while an ask was open
	for {
		var (
			ev natsstream.StreamEvent
			ok bool
		)
		if len(held) > 0 && ctx.Err() == nil {
			ev, ok, held = held[0], true, held[1:]
		} else {
			select {
			case <-ctx.Done():
				// RSAssistant cancelled (or agent shutting down): stop the workflow.
				if cerr := s.Cancel(); cerr != nil {
					logger.Printf("RSASSISTANT event=consult.cancel_failed subject=%s err=%q", req.Subject, cerr.Error())
				}
				_ = s.Close()
				return r.count(), nil // nats-agent emits done/cancelled itself
			case ev, ok = <-events:
			}
		}
		if !ok {
			r.finish(s.Close(), out)
			return r.count(), nil
		}
		r.apply(ev, out)
		if ev.Type == natsstream.StreamToolCall && req.AskUser && req.Asker != nil && !ended {
			res := answerAskCall(ctx, req, s, ev, logger)
			held = append(held, res.held...)
			ended = ended || res.ended
			if res.parked != "" && !res.ended {
				// Close the relayed tool chip, then Park ends the nats-agent
				// turn (askClosed{parked} + done awaiting_input), then the
				// workflow is told the ask parked: its loop saves the turn
				// and ends, running nothing more (Ruling 17). The resume
				// turn gives the model the answer as context (withInputs).
				out.ToolResult(res.call.ToolUseID, res.call.Name, waitingForAnswer, "")
				req.Asker.Park(res.parked)
				r.done = true
				parkWorkflow(ctx, s, res.call, req.Subject, logger)
				return r.count(), nil
			}
		}
		if r.terminated() {
			_ = s.Close()
			return r.count(), nil
		}
	}
}

// waitingForAnswer is the tool result shown for an ask that parked.
const waitingForAnswer = "waiting for the user's answer"

// parkCancelWait bounds how long a parked consult waits for the workflow's
// stream to end before it cancels the run and lets go (freeing the session
// lock).
var parkCancelWait = 30 * time.Second

// parkedResult tells the workflow's loop the ask parked: it saves the turn
// (the user's request and what it asked) to its memory and ends the run.
var parkedResult = []byte(`{"_parked":true}`)

// parkWorkflow ends the workflow after a park and returns once its stream
// ended, ctx ended, or parkCancelWait passed; nothing it sends is relayed.
// The run is cancelled instead when _parked can't be sent or the run does
// not end in time.
func parkWorkflow(ctx context.Context, s *engineStream, call toolCallPayload, subject string, logger *log.Logger) {
	defer func() { _ = s.Close() }()
	cancel := func(why string) {
		logger.Printf("RSASSISTANT event=consult.park_cancel subject=%s reason=%q", subject, why)
		if err := s.Cancel(); err != nil {
			logger.Printf("WARN RSASSISTANT event=consult.park_cancel_failed subject=%s err=%q", subject, err.Error())
		}
	}
	if err := s.PublishToolResult(call.ToolUseID, parkedResult); err != nil {
		cancel("publish _parked: " + err.Error())
	}
	timer := time.NewTimer(parkCancelWait)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-s.Events():
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		case <-timer.C:
			cancel("workflow still running after park")
			return
		}
	}
}

// askCallResult is the outcome of one request_user_input call.
type askCallResult struct {
	call   toolCallPayload
	parked string                   // ask id when the live wait ran out
	held   []natsstream.StreamEvent // workflow events read while the ask was open
	ended  bool                     // the workflow ended while the ask was open
}

// answerAskCall answers one request_user_input call (if it is one). While
// the user thinks, it keeps reading the workflow's stream (held for the
// caller to relay): if the run ends (request timeout, error, cancel) the ask
// is ended at once (its ctx is cancelled, so nats-agent closes the card as
// parked) and nothing is published to the dead run.
func answerAskCall(ctx context.Context, req consultRequest, s *engineStream, ev natsstream.StreamEvent, logger *log.Logger) askCallResult {
	var res askCallResult
	if json.Unmarshal(ev.Data, &res.call) != nil || res.call.Name != ask.ToolName {
		return res
	}
	askCtx, cancelAsk := context.WithCancel(ctx)
	defer cancelAsk()
	type outcome struct {
		payload []byte
		parked  string
		err     error
	}
	done := make(chan outcome, 1)
	resume := s.Pause() // the user's think time is not workflow time
	go func() {
		p, pk, err := answerAsk(askCtx, req.Asker, res.call.Input)
		done <- outcome{p, pk, err}
	}()
	events := s.Events()
	var o outcome
	for waiting := true; waiting; {
		select {
		case o = <-done:
			waiting = false
		case e, ok := <-events:
			if !ok {
				events = nil
				res.ended = true
				cancelAsk()
				continue
			}
			res.held = append(res.held, e)
			if e.Type == natsstream.StreamComplete || e.Type == natsstream.StreamError {
				res.ended = true
				cancelAsk()
			}
		}
	}
	resume()
	switch {
	case res.ended:
		logger.Printf("RSASSISTANT event=ask.workflow_ended tool=%s", res.call.Name)
	case o.err != nil:
		// ctx ended: the consult loop stops the workflow
	case o.parked != "":
		res.parked = o.parked // nothing to publish: the caller parks the workflow
	default:
		if perr := s.PublishToolResult(res.call.ToolUseID, o.payload); perr != nil {
			logger.Printf("RSASSISTANT event=ask.publish_failed tool=%s err=%q", res.call.Name, perr.Error())
		}
	}
	return res
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
