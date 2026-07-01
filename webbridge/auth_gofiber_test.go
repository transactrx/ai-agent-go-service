package webbridge

import (
	"testing"
)

func TestGoFiberSessionAuth_OutboundMessageSetsHeaders(t *testing.T) {
	a := GoFiberSessionAuth{} // IDT lookup returns "" for unknown session; headers still set
	msg := a.OutboundMessage("sess-1", "trx.test.wf", map[string]string{"X-Account-Id": "acct"}, []byte(`{"m":1}`))
	if msg.Subject != "trx.test.wf" {
		t.Fatalf("subject = %q", msg.Subject)
	}
	if msg.Header.Get("X-Account-Id") != "acct" {
		t.Fatalf("missing identity header")
	}
}

func TestIdtPrefix(t *testing.T) {
	cases := map[string]string{
		"IDT-abc.def": "IDT-abc",
		"":            "",
		"IDT-abc":     "IDT-abc",
		"a.b.c":       "a",
		".leading":    "",
	}
	for in, want := range cases {
		if got := idtPrefix(in); got != want {
			t.Errorf("idtPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}
