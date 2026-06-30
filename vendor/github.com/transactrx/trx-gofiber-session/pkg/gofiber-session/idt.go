package gofiber_session

import (
	"strings"

	"github.com/gofiber/fiber/v2"
)

// Inbound HTTP header set by secureappproxy when an IDT is available for the
// current request. Empty when IDT is disabled or never minted.
const TRX_IDT = "TRX_IDT"

// Outbound NATS header carrying the IDT. Webapps add this when invoking
// agents over NATS so the agent can validate via Identity.
const NATS_HEADER_TRX_IDT = "X-TRX-IDT"

// ReadIDT returns the IDT carried on the inbound HTTP request, or "" when
// absent (legacy proxy / IDT disabled / non-IDT request).
func ReadIDT(ctx *fiber.Ctx) string {
	if ctx == nil || ctx.Request() == nil {
		return ""
	}
	v := ctx.Request().Header.Peek(TRX_IDT)
	return strings.TrimSpace(string(v))
}

// BuildOutboundIDTHeaders returns a single-entry map with X-TRX-IDT set to idt,
// or nil if idt is empty. Transport-agnostic on purpose so the core library
// stays nats-dep-free; the webapp converts to nats.Header at the call site.
func BuildOutboundIDTHeaders(idt string) map[string]string {
	idt = strings.TrimSpace(idt)
	if idt == "" {
		return nil
	}
	return map[string]string{NATS_HEADER_TRX_IDT: idt}
}
