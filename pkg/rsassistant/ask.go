package rsassistant

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/ask"
	"github.com/transactrx/nats-agent/pkg/wire"
)

// request_user_input over nats-agent 1.7 "ask the user" (spec §10).
//
// For an askUser agent whose caller can show forms, the consult asks the
// workflow run to offer request_user_input to its agent loop as a client-only
// tool. The loop emits tool_call and waits on the stream's tool-result
// subject, exactly as for PowerLine's web UI tools; the bridge asks the
// RSAssistant user (Stream.AskUser) and publishes the result. The results
// match RSAssistant's own request_user_input (pkg/orchestrator/asktool.go).

// cantAsk is the tool error when the run cannot ask the user.
const cantAsk = "can't ask the user here"

// askMargin keeps the bridge's live wait under the workflow's own wait for
// the tool result, so an ask always parks (or is answered) while the loop
// still listens: an answer at the boundary is never published to nobody.
const askMargin = 10 * time.Second

// defaultEngineWait is the agent loop's wait when humanResponseTimeoutSeconds
// is unset (builtin/agent/loop.go).
const defaultEngineWait = 5 * time.Minute

// userAsker is the ask half of *agent.Stream.
type userAsker interface {
	AskUser(ctx context.Context, a ask.Ask) (ask.Answer, error)
	Park(askID string)
}

// answerAsk asks the user for one request_user_input call and returns the
// payload to publish as the tool result. parked is the ask id when the live
// wait ran out: payload is then nil (nothing to publish); the caller ends
// the turn with Park and cancels the workflow (Ruling 17: no hidden drain;
// the resume gives the model the answer as context). err is set only when
// ctx ended (nothing to publish; the run is going away).
func answerAsk(ctx context.Context, u userAsker, input json.RawMessage) (payload []byte, parked string, err error) {
	var in map[string]any
	if jerr := json.Unmarshal(input, &in); jerr != nil || in == nil {
		return toolError("invalid ask: not a JSON object"), "", nil
	}
	a, perr := ask.ParseAsk(in) // checks too: no card for a bad ask
	if perr != nil {
		return toolError(perr.Error()), "", nil
	}
	ans, aerr := u.AskUser(ctx, a)
	switch {
	case aerr == nil:
		return []byte(ans.ToolResult()), "", nil
	case errors.Is(aerr, agent.ErrSkipped):
		return []byte(ask.Answer{AskID: ans.AskID, Status: ask.StatusSkipped}.ToolResult()), "", nil
	case errors.Is(aerr, agent.ErrParked):
		return nil, ans.AskID, nil
	case errors.Is(aerr, agent.ErrSuperseded):
		// The user wrote instead of answering: their text as context.
		text, _ := ans.Values["text"].(string)
		return mustJSON(struct {
			Status    string `json:"status"`
			UserWrote string `json:"userWrote"`
		}{"not_answered", text}), "", nil
	case errors.Is(aerr, agent.ErrUnsupported), errors.Is(aerr, agent.ErrRunEnded):
		return toolError(cantAsk), "", nil
	case ctx.Err() != nil:
		return nil, "", ctx.Err()
	}
	// ErrBusy or a malformed ask (ask.CheckAsk): no card was shown.
	return toolError(aerr.Error()), "", nil
}

// toolError is a client result the agent loop turns into a tool error.
func toolError(msg string) []byte { return mustJSON(map[string]any{"_error": msg}) }

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// withInputs puts answers to asks parked in earlier turns in front of the
// user's text: a parked workflow turn is not resumed mid-loop, so its model
// gets the answer as context and re-decides (spec §10 "Park semantics").
func withInputs(text string, inputs []agent.ValidInput) string {
	if len(inputs) == 0 {
		return text
	}
	lines := make([]string, 0, len(inputs)+1)
	for _, in := range inputs {
		lines = append(lines, ask.FormatForModel(in.Ask, in.AskInput))
	}
	if strings.TrimSpace(text) != "" {
		lines = append(lines, text)
	}
	return strings.Join(lines, "\n\n")
}

// bridgeAskWait is the live wait for this bridge: AGENT_ASK_WAIT (else 5m,
// the library default), capped at engineWait - askMargin.
func bridgeAskWait(engineWait time.Duration, lookup func(string) (string, bool)) time.Duration {
	wait := 5 * time.Minute
	if v, ok := lookup("AGENT_ASK_WAIT"); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil && d > 0 {
			wait = d
		}
	}
	if limit := engineWait - askMargin; wait > limit {
		wait = limit
	}
	if wait < time.Second {
		wait = time.Second
	}
	return wait
}

// sessionLocks serializes consults per key (see sessionKey).
type sessionLocks struct {
	mu sync.Mutex
	m  map[lockKey]*sessionLock
}

type sessionLock struct {
	ch   chan struct{} // holds one token while a consult runs
	refs int
}

// lockWaitTick is how often onWait runs while a consult waits for the lock.
const lockWaitTick = 10 * time.Second

// lockKey identifies one drain lock: the caller's identity plus the session
// id, as separate fields so no delimiter inside a (caller-controlled) part
// can make two different pairs collide. The zero key means "no lock".
type lockKey struct{ account, user, session string }

// sessionKey scopes the drain lock to the caller's identity (account and
// user from the turn's identity: the verified one, or the turn's UserID when
// unverified, as gate accepts it) plus the session id, so one caller can't
// hold up another's consult by reusing its session id. No session id = the
// zero key (no lock).
func sessionKey(id agent.Identity, sessionID string) lockKey {
	if sessionID == "" {
		return lockKey{}
	}
	return lockKey{account: id.AccountID, user: id.UserID, session: sessionID}
}

// lockWait bounds the wait for the lock: the consult timeout, and at most
// half of the caller's remaining budget so wait + consult fits in it.
func lockWait(consultTimeout time.Duration, b *wire.Budget) time.Duration {
	wait := consultTimeout
	if b != nil && b.RemainingMs > 0 {
		if half := time.Duration(b.RemainingMs) * time.Millisecond / 2; wait <= 0 || half < wait {
			wait = half
		}
	}
	return wait
}

// acquire waits up to wait (or ctx) for key. While it waits, onWait (if set)
// runs at once and then every lockWaitTick, so the caller can show the run
// is alive. On timeout it returns an error and a no-op unlock: the caller
// proceeds rather than hang. A key without a session id is never locked.
func (l *sessionLocks) acquire(ctx context.Context, key lockKey, wait time.Duration, onWait func()) (func(), error) {
	if key.session == "" {
		return func() {}, nil
	}
	l.mu.Lock()
	if l.m == nil {
		l.m = map[lockKey]*sessionLock{}
	}
	sl := l.m[key]
	if sl == nil {
		sl = &sessionLock{ch: make(chan struct{}, 1)}
		l.m[key] = sl
	}
	sl.refs++
	l.mu.Unlock()
	release := func() {
		l.mu.Lock()
		if sl.refs--; sl.refs == 0 {
			delete(l.m, key)
		}
		l.mu.Unlock()
	}
	select {
	case sl.ch <- struct{}{}: // free: no wait, no onWait
		return func() { <-sl.ch; release() }, nil
	default:
	}
	var timeout <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		timeout = t.C
	}
	tick := time.NewTicker(lockWaitTick)
	defer tick.Stop()
	for {
		if onWait != nil {
			onWait()
		}
		select {
		case sl.ch <- struct{}{}:
			return func() { <-sl.ch; release() }, nil
		case <-ctx.Done():
			release()
			return func() {}, ctx.Err()
		case <-timeout:
			release()
			return func() {}, errors.New("session busy: previous turn still finishing")
		case <-tick.C:
		}
	}
}
