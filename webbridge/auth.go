package webbridge

import (
	"github.com/gofiber/fiber/v2"
	"github.com/nats-io/nats.go"
)

// Identity carries the resolved caller for a single request or session.
type Identity struct {
	AccountID string
	UserID    string
	UserName  string
	TimeZone  string
	SessionID string
}

// Authenticator is the entire host coupling for identity and credential lifecycle.
type Authenticator interface {
	// Identify resolves identity from a cookie-authenticated HTTP request
	// (token mint, upload, chart, workflows). SessionID keys the IDT registry.
	Identify(c *fiber.Ctx) (Identity, error)
	// WarmCredential captures the live IDT for sessionID from the current
	// request, so the WS handler can look it up later. No-op if not applicable.
	WarmCredential(c *fiber.Ctx, sessionID string)
	// OutboundMessage builds the NATS message for one turn: attaches the live
	// credential for sessionID plus the extra identity headers and body.
	OutboundMessage(sessionID, subject string, headers map[string]string, body []byte) *nats.Msg
	// Release drops any per-session credential state when the WS closes.
	Release(sessionID string)
}

// Authorizer is the entire host coupling for workflow-access gating.
type Authorizer interface {
	// Authorize is called once per stream, fail-closed on (false, reason).
	Authorize(accountID, indexName, workflowID string) (allow bool, reason string)
}

// AllowAll is the default Authorizer when Options.Authorizer is nil.
type AllowAll struct{}

func (AllowAll) Authorize(_, _, _ string) (bool, string) { return true, "" }
