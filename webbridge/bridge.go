package webbridge

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

const (
	// natsHdrAccount and natsHdrUser are the header names the chat workflow
	// expects (the agent half's nats-chat trigger identity headers). Distinct from
	// the host's internal Account-Id-Header used for non-chat NATS calls.
	natsHdrAccount  = "X-Account-Id"
	natsHdrUser     = "X-User-Id"
	natsHdrUserName = "X-User-Name"
	natsHdrTimeZone = "X-Time-Zone"
)

// bridge holds the per-Mount state and the injected host seams. Methods on
// *bridge use b.auth/b.authz/b.natsBasePath/b.logger so Mount can construct a
// bridge per call without package-level globals.
type bridge struct {
	auth         Authenticator
	authz        Authorizer
	natsBasePath string
	logger       *log.Logger
}

// wsFrame is the JSON envelope sent on the WebSocket. Mirrors the SSE event
// shape so the frontend renderer doesn't care about transport.
type wsFrame struct {
	Event    string          `json:"event"`
	Sequence int             `json:"sequence"`
	Data     json.RawMessage `json:"data"`
}

// One-time stream tokens. The TLS proxy in front of this service does not
// forward cookies onto WebSocket upstream dials, so we cannot read the session
// inside the WS handler. The browser first POSTs to /aichatviewer/token (a
// regular HTTP request that does carry the session cookie), receives a token,
// then opens the WebSocket with ?token=<value>. Tokens are single-use with a
// 60-second TTL.
type tokenEntry struct {
	accountId string
	userId    string
	userName  string // human display name (X-User-Name) for the chat workflow
	timeZone  string // IANA timezone (X-Time-Zone) captured from the browser
	sessionID string // host session id — key into the credential registry (live lookups)
	expires   time.Time
}

const tokenTTL = 60 * time.Second

// wsKeepaliveInterval is how often pumpEvents sends a no-op "ping" frame to the
// browser while a turn is in flight. After a tool call the agent can spend
// >60s generating the final answer (large context in long sessions); with no
// frames flowing, the TLS proxy (secureappproxy) idle-closes the WebSocket,
// which makes the bridge cancel the upstream NATS stream and abort the
// in-flight answer — the chart is rendered in S3 but the answer never streams,
// so nothing shows. A periodic keepalive keeps the connection warm so slow
// turns complete. The client ignores the "ping" event (reducer default case).
// Var (not const) so tests can shorten it.
var wsKeepaliveInterval = 20 * time.Second

var tokenStore = struct {
	sync.Mutex
	m map[string]tokenEntry
}{m: map[string]tokenEntry{}}

// issueToken mints a one-time stream token for any authenticated user.
// Body is `{"token":"<uuid>"}`.
func (b *bridge) issueToken(c *fiber.Ctx) error {
	id, err := b.auth.Identify(c)
	if err != nil || id.AccountID == "" {
		return c.Status(http.StatusUnauthorized).JSON(&fiber.Map{
			"status": http.StatusUnauthorized, "code": "error-account-not-found", "message": "Account not found",
		})
	}
	// Capture the session id so the WS handler can look up the *current*
	// credential in the registry at publish time (the WS dial drops cookies, so
	// we bridge the id through the one-time token like we used to bridge the
	// credential itself). WarmCredential captures the live credential for this
	// request so the registry is warm immediately.
	sessionID := id.SessionID
	b.auth.WarmCredential(c, sessionID)

	// Display name (best-effort) + browser timezone from the POST body. Both are
	// optional; the chat workflow falls back gracefully when they are empty.
	userName := id.UserName
	var tzBody struct {
		TimeZone string `json:"timeZone"`
	}
	_ = json.Unmarshal(c.Body(), &tzBody) // empty/malformed body tolerated

	token := uuid.NewString()
	tokenStore.Lock()
	tokenStore.m[token] = tokenEntry{accountId: id.AccountID, userId: id.UserID, userName: userName, timeZone: tzBody.TimeZone, sessionID: sessionID, expires: time.Now().Add(tokenTTL)}
	// Opportunistic sweep of expired tokens so the map stays bounded.
	now := time.Now()
	for k, v := range tokenStore.m {
		if now.After(v.expires) {
			delete(tokenStore.m, k)
		}
	}
	tokenStore.Unlock()
	return c.JSON(&fiber.Map{"token": token})
}

// listWorkflows proxies the chatApi ListWorkflows NATS endpoint to the browser
// for the config-form picker. Subject ${natsBasePath}.ListWorkflows where
// natsBasePath is the chat base path (== chatApi NATS_BASE_PATH).
func (b *bridge) listWorkflows(c *fiber.Ctx) error {
	if id, err := b.auth.Identify(c); err != nil || id.AccountID == "" {
		return c.Status(http.StatusUnauthorized).JSON(&fiber.Map{
			"status": http.StatusUnauthorized, "code": "error-account-not-found", "message": "Account not found",
		})
	}
	nc, err := getNatsConn()
	if err != nil {
		return c.Status(http.StatusBadGateway).JSON(fiber.Map{"error": "nats unavailable"})
	}
	resp, err := nc.Request(b.natsBasePath+".ListWorkflows", []byte("{}"), 10*time.Second)
	if err != nil {
		return c.Status(http.StatusBadGateway).JSON(fiber.Map{"error": "workflow list unavailable"})
	}
	c.Set("Content-Type", "application/json")
	return c.Send(resp.Data)
}

func consumeToken(token string) (accountId, userId, userName, timeZone, sessionID string, ok bool) {
	if token == "" {
		return "", "", "", "", "", false
	}
	tokenStore.Lock()
	defer tokenStore.Unlock()
	e, found := tokenStore.m[token]
	if !found {
		return "", "", "", "", "", false
	}
	if time.Now().After(e.expires) {
		delete(tokenStore.m, token)
		return "", "", "", "", "", false
	}
	delete(tokenStore.m, token)
	return e.accountId, e.userId, e.userName, e.timeZone, e.sessionID, true
}

func (b *bridge) preUpgrade(c *fiber.Ctx) error {
	if !websocket.IsWebSocketUpgrade(c) {
		return fiber.ErrUpgradeRequired
	}
	accountId, userId, userName, timeZone, sessionID, ok := consumeToken(c.Query("token"))
	if !ok {
		return c.Status(http.StatusUnauthorized).JSON(&fiber.Map{
			"status": http.StatusUnauthorized, "code": "invalid-token", "message": "Invalid or expired token",
		})
	}
	c.Locals(localAccountID, accountId)
	c.Locals(localUserID, userId)
	c.Locals(localUserName, userName)
	c.Locals(localTimeZone, timeZone)
	c.Locals(localSessionID, sessionID)
	return c.Next()
}

// streamChat is the WebSocket handler. Supports multi-turn conversations over
// a single WebSocket connection using typed ClientFrame envelopes. Falls back
// to the legacy one-shot {message, sessionId, workflowId} shape for backward
// compat during cutover.
//
// The FIRST c.ReadMessage() is done inline so the raw bytes are available for
// legacy-shape detection. After that, a single reader goroutine owns all
// subsequent reads; runOneTurn must never call c.ReadMessage() directly.
func (b *bridge) streamChat(c *websocket.Conn) {
	accountId, _ := c.Locals(localAccountID).(string)
	userId, _ := c.Locals(localUserID).(string)
	userName, _ := c.Locals(localUserName).(string)
	timeZone, _ := c.Locals(localTimeZone).(string)
	gfSessionID, _ := c.Locals(localSessionID).(string)

	// Drop this session's registry entry when the socket closes.
	defer b.auth.Release(gfSessionID)

	// First read: inline, supports both typed and legacy shapes.
	_, msgBytes, err := c.ReadMessage()
	if err != nil {
		return
	}

	var (
		initialText        string
		initialSessionID   string
		initialWorkflowID  string
		initialIndexName   string
		initialAttachments []ClientAttachment
	)
	if cf, perr := ParseClientFrame(msgBytes); perr == nil && cf.Event == ClientEventUserMessage && cf.UserMessage != nil {
		initialText = cf.UserMessage.Text
		initialSessionID = cf.UserMessage.SessionId
		initialWorkflowID = cf.UserMessage.WorkflowId
		initialIndexName = cf.UserMessage.IndexName
		initialAttachments = cf.UserMessage.Attachments
		// Prefer the timezone from the frame; the token-body value (Locals) can be
		// dropped by the portal proxy. Frame is the reliable channel.
		if cf.UserMessage.TimeZone != "" {
			timeZone = cf.UserMessage.TimeZone
		}
	} else {
		var req ChatStreamRequest
		if jerr := json.Unmarshal(msgBytes, &req); jerr != nil || strings.TrimSpace(req.Message) == "" {
			_ = c.WriteJSON(wsErrorFrame("invalid-body", "Invalid request body"))
			return
		}
		initialText = req.Message
		initialSessionID = req.SessionId
		initialWorkflowID = req.WorkflowId
	}

	b.logger.Printf("#User-Action-Log. UserId: %s, AccountId: %s, Text: AI Chat: %s", userId, accountId, truncateForAudit(initialText, 50))

	// Resolve the workflow id once; this exact value is both authorized and
	// used to build the publish subject (single source of truth). Empty id is
	// an immediate deny — never fall back to a default workflow.
	if initialWorkflowID == "" {
		_ = c.WriteJSON(wsErrorFrame("workflow-required", "No workflow selected for this Search"))
		return
	}
	// Empty here means the id contained subject-illegal chars (".", "*", ">", …).
	// Reject rather than route it anywhere — a generic bridge has no default.
	resolvedWorkflowID := resolveWorkflowID(initialWorkflowID)
	if resolvedWorkflowID == "" {
		_ = c.WriteJSON(wsErrorFrame("invalid-workflow", "Invalid workflow identifier"))
		return
	}

	// Authorization gate — ONCE per stream, fail-closed.
	if allow, reason := b.authz.Authorize(accountId, initialIndexName, resolvedWorkflowID); !allow {
		b.logger.Printf("IDT_METRIC event=chat.gate.deny index=%s workflow=%s reason=%s account=%s",
			initialIndexName, resolvedWorkflowID, reason, accountId)
		_ = c.WriteJSON(wsErrorFrame("workflow-not-authorized", "This AI assistant is not available for this Search"))
		return
	}

	// Single reader goroutine for SUBSEQUENT frames.
	closed := make(chan struct{})
	clientFrames := make(chan ClientFrame, 4)
	go func() {
		defer close(closed)
		for {
			_, msgBytes, err := c.ReadMessage()
			if err != nil {
				// DIAGNOSTIC (ws-pump-trace): the reader's read side erroring is
				// how a proxy/peer WS close surfaces; it closes `closed`, which
				// makes pumpEvents Cancel the upstream. Log the error so we can
				// tell a transport close apart from a normal teardown.
				if b.logger != nil {
					b.logger.Printf("ws-pump-trace: reader ReadMessage err=%v", err)
				}
				return
			}
			f, perr := ParseClientFrame(msgBytes)
			if perr != nil {
				continue
			}
			select {
			case clientFrames <- f:
			case <-closed:
				return
			}
		}
	}()

	// Multi-turn loop. activeSessionID threads the session across turns: the
	// agent mints/returns it in the start event of turn 1, and we replay it on
	// every subsequent turn so the agent's memory loads the prior conversation.
	activeSessionID := initialSessionID
	for {
		sid, fatal := b.runOneTurn(c, accountId, userId, userName, timeZone, gfSessionID, initialText, activeSessionID, resolvedWorkflowID, initialAttachments, clientFrames, closed)
		if sid != "" {
			activeSessionID = sid
		}
		if fatal {
			// Agent reported a terminal session error (e.g. IDT validation
			// denial). Returning here causes fiber to close the WebSocket
			// cleanly — the UI will already have received the session-expired
			// frame from pumpEvents.
			return
		}

		// Wait for next user_message OR peer close.
		var nextFrame ClientFrame
		select {
		case f, ok := <-clientFrames:
			if !ok {
				return
			}
			nextFrame = f
		case <-closed:
			return
		}
		// Skip non-user_message frames between turns (tool_result/cancel from
		// late widget clicks). Credential freshness is handled server-side via
		// the registry, so idt_update frames are no longer consumed.
		for nextFrame.Event != ClientEventUserMessage || nextFrame.UserMessage == nil {
			select {
			case f, ok := <-clientFrames:
				if !ok {
					return
				}
				nextFrame = f
			case <-closed:
				return
			}
		}
		initialText = nextFrame.UserMessage.Text
		initialAttachments = nextFrame.UserMessage.Attachments
		if nextFrame.UserMessage.TimeZone != "" {
			timeZone = nextFrame.UserMessage.TimeZone
		}
		// Workflow is fixed for the lifetime of the stream (authorized once at
		// start); subsequent turns reuse resolvedWorkflowID, so we no longer
		// touch initialWorkflowID here.
	}
}

// runOneTurn opens one streaming run and pumps it to completion. Returns the
// sessionId captured from the agent's start event (or "" if none was observed)
// and a fatal flag — true when the agent surfaced a terminal session error
// (e.g. IDT validation denial). streamChat must break its multi-turn loop on
// fatal so the WS closes cleanly; the user can't recover by sending another
// message against the same dead credential.
//
// clientFrames and closed are owned by streamChat's single reader goroutine;
// runOneTurn must never call c.ReadMessage() directly.
func (b *bridge) runOneTurn(c wsWriter, accountId, userId, userName, timeZone, gfSessionID, text, sessionID, workflowID string, atts []ClientAttachment, clientFrames <-chan ClientFrame, closed <-chan struct{}) (string, bool) {
	envelope := streamRequestEnvelope{Message: text, SessionId: sessionID, Attachments: atts}
	body, _ := json.Marshal(envelope)

	extra := map[string]string{
		natsHdrAccount:  accountId,
		natsHdrUser:     userId,
		natsHdrUserName: userName,
		natsHdrTimeZone: timeZone,
	}
	// workflowID is already resolved + authorized by streamChat (single source
	// of truth); do not re-resolve here or the published subject could diverge
	// from the authorized one. OutboundMessage attaches the live credential for
	// gfSessionID (kept fresh by the proxy's in-band refresh) plus the identity
	// headers; empty credential means it's off — the agent will fail closed or
	// open depending on its own config.
	subject := fmt.Sprintf("%s.%s", b.natsBasePath, workflowID)
	msg := b.auth.OutboundMessage(gfSessionID, subject, extra, body)
	b.logger.Printf("IDT_METRIC event=chat.outbound.attempt subject=%s session=%s",
		subject, sessionID)
	sub, err := doStreamingRequest(msg)
	if err != nil {
		_ = c.WriteJSON(wsErrorFrame("stream-open-failed", "Could not open AI stream"))
		return "", false
	}
	defer sub.Close()

	requestId := uuid.NewString()

	return b.pumpEvents(c, sub, requestId, closed, clientFrames)
}

// wsWriter is the subset of *websocket.Conn used by pumpEvents. Allows
// pumpEvents to be unit-tested without spinning a real WebSocket.
type wsWriter interface {
	WriteJSON(v any) error
}

// pumpEvents forwards events from sub to w as JSON frames. Returns the
// sessionId captured from the agent's start payload (or "" if start never
// arrived) so the caller can propagate it to subsequent turns, and a fatal
// flag — true when the agent emitted a terminal session error (synthesised
// from a nats-service non-200 response by parseStreamMessage). On fatal,
// pumpEvents writes a session-expired wsFrame after the raw error frame so
// the UI has a stable code to render. Exits when the stream terminates, the
// writer fails, closed fires (client disconnect), or a cancel frame arrives.
// clientFrames carries typed frames from the reader goroutine for routing
// within this turn.
func (b *bridge) pumpEvents(w wsWriter, sub streamSubscription, requestId string, closed <-chan struct{}, clientFrames <-chan ClientFrame) (string, bool) {
	var capturedSessionID string
	keepalive := time.NewTicker(wsKeepaliveInterval)
	defer keepalive.Stop()
	// DIAGNOSTIC (ws-pump-trace): characterize WHY this turn's pump exits and
	// HOW LONG since the last agent (server->client) frame. The answer-loss bug
	// shows up as return=closed ~60s after the last agent frame: the WS closed,
	// so we Cancel the upstream agent mid-generation. agentFrames excludes the
	// keepalive ping (only real stream events count) so sinceLastAgentFrameMs
	// measures the true generation gap.
	pumpStartedAt := time.Now()
	lastAgentFrameAt := pumpStartedAt
	agentFrames := 0
	pumpTrace := func(path, extra string) {
		if b.logger == nil {
			return
		}
		b.logger.Printf("ws-pump-trace: return=%s reqId=%s elapsedMs=%d sinceLastAgentFrameMs=%d agentFrames=%d%s",
			path, requestId, time.Since(pumpStartedAt).Milliseconds(),
			time.Since(lastAgentFrameAt).Milliseconds(), agentFrames, extra)
	}
	for {
		select {
		case <-closed:
			pumpTrace("closed", "")
			_ = sub.Cancel()
			return capturedSessionID, false
		case <-keepalive.C:
			// Heartbeat so the proxy doesn't idle-close the WS during a slow
			// post-tool generation. Sent from this single select loop, so it
			// never races other WriteJSON calls. The client ignores "ping".
			if err := w.WriteJSON(wsFrame{Event: "ping"}); err != nil {
				pumpTrace("keepalive-write-err", " err="+err.Error())
				_ = sub.Cancel()
				return capturedSessionID, false
			}
		case cf := <-clientFrames:
			switch cf.Event {
			case ClientEventToolResult:
				if cf.ToolResult != nil {
					_ = sub.PublishToolResult(cf.ToolResult.ToolCallID, cf.ToolResult.Result)
				}
			case ClientEventCancel:
				pumpTrace("client-cancel", "")
				_ = sub.Cancel()
				return capturedSessionID, false
			case ClientEventUserMessage:
				// New turn mid-flight: ignore until current turn terminates.
			}
		case evt, ok := <-sub.Events():
			if !ok {
				// Upstream stream ended/closed (terminator, sub.Close, or the
				// host-side 5min stream timeout) WITHOUT us returning first.
				pumpTrace("stream-eof", "")
				return capturedSessionID, false
			}
			if evt.Type == streamStart {
				var p startPayload
				if err := json.Unmarshal(evt.Data, &p); err == nil && p.SessionID != "" {
					capturedSessionID = p.SessionID
				}
			}
			frame := buildWSFrame(evt, requestId)
			if err := w.WriteJSON(frame); err != nil {
				pumpTrace("write-err", " err="+err.Error()+" evtType="+string(evt.Type))
				_ = sub.Cancel()
				return capturedSessionID, false
			}
			if evt.Type == streamError && isTerminalStreamError(evt.Data) {
				_ = w.WriteJSON(wsErrorFrame("session-expired", "Your session is no longer valid. Please reload the page."))
				return capturedSessionID, true
			}
			agentFrames++
			lastAgentFrameAt = time.Now()
			if evt.Type == streamComplete || evt.Type == streamError {
				pumpTrace("terminal", " evtType="+string(evt.Type))
				return capturedSessionID, false
			}
		}
	}
}

// isTerminalStreamError returns true when a streamError event was synthesised
// from a nats-service auth/permission failure — the user cannot recover by
// retrying with the same credential. Currently treats status 401/403 as
// terminal; 5xx (server error) is transient and stays non-fatal.
func isTerminalStreamError(data []byte) bool {
	var probe struct {
		Status int `json:"status"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return false
	}
	return probe.Status == 401 || probe.Status == 403
}

// buildWSFrame returns the JSON frame for one event. For the start event we
// substitute the agent's body with one that carries our bridge-generated
// requestId so the client has a stable handle independent of the agent's
// internal stream id.
func buildWSFrame(evt streamEvent, requestId string) wsFrame {
	if evt.Type == streamStart {
		var p startPayload
		_ = json.Unmarshal(evt.Data, &p)
		out := startEventOut{SessionId: p.SessionID, WorkflowId: p.WorkflowID, RequestId: requestId}
		b, _ := json.Marshal(out)
		return wsFrame{Event: string(evt.Type), Sequence: evt.Sequence, Data: b}
	}
	return wsFrame{Event: string(evt.Type), Sequence: evt.Sequence, Data: evt.Data}
}

func wsErrorFrame(code, msg string) wsFrame {
	body, _ := json.Marshal(map[string]string{"code": code, "message": msg})
	return wsFrame{Event: "error", Sequence: 0, Data: body}
}

// truncateForAudit returns s if shorter than max, else s[:max]+"…".
func truncateForAudit(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// resolveWorkflowID validates the workflow id used to build the NATS subject.
// Only [A-Za-z0-9_-] are permitted so a client can't smuggle subject tokens
// (".", "*", ">") via the WS request body. Empty or non-conforming ids return
// "" — the caller rejects the request rather than falling back to any default
// workflow (a generic bridge has no tenant workflow to fall back to).
func resolveWorkflowID(raw string) string {
	if raw == "" {
		return ""
	}
	for _, r := range raw {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-':
		default:
			return ""
		}
	}
	return raw
}
