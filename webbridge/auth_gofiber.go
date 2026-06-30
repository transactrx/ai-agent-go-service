package webbridge

import (
	"encoding/json"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/session/v2"
	"github.com/nats-io/nats.go"
	gofibersession "github.com/transactrx/trx-gofiber-session/pkg/gofiber-session"
	"github.com/transactrx/trx-gofiber-session/pkg/gofiber-session/idtnats"
)

// GoFiberSessionAuth is the default Authenticator for TransactRx apps using
// trx-gofiber-session. Identity-field extraction is injected via Resolve (the
// host's session-detail shape is app-local); the IDT lifecycle + outbound
// message use trx-gofiber-session.
type GoFiberSessionAuth struct {
	Session  *session.Session                                                  // gofiber/session v2 instance
	Resolve  func(c *fiber.Ctx) (accountID, userID, userName string, err error) // host session-detail extractor
	ConnName string                                                            // optional nats.Name; default "ai-agent-webbridge"
}

// Compile-time assertion that GoFiberSessionAuth satisfies Authenticator.
var _ Authenticator = GoFiberSessionAuth{}

func (a GoFiberSessionAuth) Identify(c *fiber.Ctx) (Identity, error) {
	accountID, userID, userName, err := a.Resolve(c)
	if err != nil {
		return Identity{}, err
	}
	sessionID := ""
	if a.Session != nil {
		if store := a.Session.Get(c); store != nil {
			sessionID = store.ID()
		}
	}
	var tzBody struct {
		TimeZone string `json:"timeZone"`
	}
	_ = json.Unmarshal(c.Body(), &tzBody)
	return Identity{AccountID: accountID, UserID: userID, UserName: userName, TimeZone: tzBody.TimeZone, SessionID: sessionID}, nil
}

func (a GoFiberSessionAuth) WarmCredential(c *fiber.Ctx, sessionID string) {
	if sessionID == "" {
		return
	}
	if idt := gofibersession.ReadIDT(c); idt != "" {
		gofibersession.CaptureIDT(sessionID, idt)
	}
}

func (a GoFiberSessionAuth) OutboundMessage(sessionID, subject string, headers map[string]string, body []byte) *nats.Msg {
	idt := gofibersession.CurrentIDT(sessionID)
	// Preserve the outbound IDT metric that lived in the bridge before the seam
	// (Task 2.3 dropped it from bridge.go because the credential is no longer in
	// scope there). Emitting it here keeps log-based dashboards working.
	log.Printf("IDT_METRIC event=chat.outbound.attempt subject=%s idtid=%s idt_present=%v",
		subject, idtPrefix(idt), idt != "")
	return idtnats.BuildMsg(subject, idt, headers, body)
}

// idtPrefix returns the leading "IDT-{uuid}" segment of an IDT wire token for log
// breadcrumbs only (never used for auth). Moved here from the old bridge.
func idtPrefix(wire string) string {
	for i := 0; i < len(wire); i++ {
		if wire[i] == '.' {
			return wire[:i]
		}
	}
	return wire
}

func (a GoFiberSessionAuth) Release(sessionID string) {
	gofibersession.ForgetIDT(sessionID)
}
