// Package idtnats wires the IDT helper from trx-gofiber-session into a
// NATS-based fan-out. Splitting it from the core pkg/gofiber-session keeps the
// core nats-dep-free: consumers that don't talk NATS (HTTP-only, gRPC) never
// pull github.com/nats-io/nats.go into their import graph.
package idtnats

import (
	"errors"

	"github.com/nats-io/nats.go"

	gofibersession "github.com/transactrx/trx-gofiber-session/pkg/gofiber-session"
)

// ErrEmptySubject is returned by Publish when called with subject == "".
var ErrEmptySubject = errors.New("idtnats: empty subject")

// BuildMsg constructs a *nats.Msg with extraHeaders plus X-TRX-IDT derived
// from the passed-in idt. Returns nil if subject is empty. If idt is empty,
// X-TRX-IDT is not set — preserves the documented "IDT off" pass-through.
//
// When extraHeaders contains X-TRX-IDT, the idt arg overwrites it.
func BuildMsg(subject, idt string, extraHeaders map[string]string, body []byte) *nats.Msg {
	if subject == "" {
		return nil
	}
	header := nats.Header{}
	for k, v := range extraHeaders {
		header.Set(k, v)
	}
	for k, v := range gofibersession.BuildOutboundIDTHeaders(idt) {
		header.Set(k, v)
	}
	return &nats.Msg{
		Subject: subject,
		Header:  header,
		Data:    body,
	}
}

// Publish builds via BuildMsg and publishes on nc. Fire-and-forget — no
// reply, no retry, no timeout. Callers that need request/reply or streaming
// should use BuildMsg + their own nc.RequestMsg/streaming wrapper.
func Publish(nc *nats.Conn, subject, idt string, extraHeaders map[string]string, body []byte) error {
	msg := BuildMsg(subject, idt, extraHeaders, body)
	if msg == nil {
		return ErrEmptySubject
	}
	return nc.PublishMsg(msg)
}
