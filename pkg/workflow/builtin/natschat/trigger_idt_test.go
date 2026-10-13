package natschat

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/idt"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// lockedBuf is a goroutine-safe log sink (the observe-only path logs from a goroutine).
type lockedBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// idtStub answers validateInternalToken on a real NATS server. Token "good" is
// granted exactly the functions given to newIDTStub.
type idtStub struct {
	mu     sync.Mutex
	asked  []string
	grants map[string]bool
}

func (s *idtStub) functions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.asked...)
}

func newIDTStub(t *testing.T, grants ...string) (*nats.Conn, *idtStub) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	s := &idtStub{grants: map[string]bool{}}
	for _, g := range grants {
		s.grants[g] = true
	}
	if _, err := nc.Subscribe("test.identity.validateInternalToken", func(m *nats.Msg) {
		var req struct {
			IDT        string `json:"idt"`
			FunctionID string `json:"functionId"`
		}
		_ = json.Unmarshal(m.Data, &req)
		s.mu.Lock()
		s.asked = append(s.asked, req.FunctionID)
		granted := s.grants[req.FunctionID]
		s.mu.Unlock()
		switch {
		case req.IDT != "good":
			_ = m.Respond([]byte(`{"valid":false,"functionGranted":false,"reason":"INVALID_IDT"}`))
		case granted:
			_ = m.Respond([]byte(`{"valid":true,"functionGranted":true,"userId":"u-1","accountId":"acct-9"}`))
		default:
			_ = m.Respond([]byte(`{"valid":true,"functionGranted":false,"userId":"u-1","accountId":"acct-9","reason":"DENIED_FN"}`))
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return nc, s
}

// newIDTTestTrigger builds a real trigger with a real idt.Validator on a real NATS server.
func newIDTTestTrigger(t *testing.T, validation, observeOnly string, access node.IndexAccess, grants ...string) (*natsChatTrigger, *idtStub, *lockedBuf) {
	t.Helper()
	t.Setenv("IDT_VALIDATION", validation)
	t.Setenv("IDT_OBSERVE_ONLY", observeOnly)
	t.Setenv("IDT_FAIL_OPEN", "false")
	t.Setenv("APP_ID", "APPX")
	t.Setenv("APP_FUNCTION_ID", "FN-APP")
	t.Setenv("NATS_IDENTITY_BASE_PATH", "test.identity")
	t.Setenv("NATS_IDENTITY_VALIDATE_SUBJECT", "validateInternalToken")
	nc, stub := newIDTStub(t, grants...)
	v, err := idt.NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	buf := &lockedBuf{}
	tr := newTestTrigger(t, `{"responseMode":"streaming"}`)
	tr.idtValidator = v
	tr.logger = log.New(buf, "", 0)
	tr.workflowID = "wfX"
	tr.SetIndexAccess(access)
	return tr, stub, buf
}

func idtMsg(token string) *nats_service.NatsMessage {
	m := testMsg(`{"message":"q"}`)
	if token != "" {
		m.Header.Set(idt.HeaderIDT, token)
	}
	return m
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("condition not met within 2s")
}

var boundIdx = node.IndexAccess{Status: node.IndexAccessBound, Index: "dev.cpe-*", FunctionID: "FN-IDX"}

func TestAuthorizeIDTBoundFunctionGranted(t *testing.T) {
	tr, stub, buf := newIDTTestTrigger(t, "true", "false", boundIdx, "FN-IDX")
	user, acct, err := tr.authorizeIDT(idtMsg("good"), "hdr-user", "hdr-acct")
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	if user != "u-1" || acct != "acct-9" {
		t.Fatalf("user=%q acct=%q, want identity's values", user, acct)
	}
	if got := stub.functions(); len(got) != 1 || got[0] != "FN-IDX" {
		t.Fatalf("identity asked for %v, want [FN-IDX]", got)
	}
	if !strings.Contains(buf.String(), "function=FN-IDX") {
		t.Fatalf("log lacks function=FN-IDX:\n%s", buf.String())
	}
}

func TestAuthorizeIDTUserWithOnlyTheOldAppFunctionIsDenied(t *testing.T) {
	tr, _, _ := newIDTTestTrigger(t, "true", "false", boundIdx, "FN-APP")
	_, _, err := tr.authorizeIDT(idtMsg("good"), "hdr-user", "hdr-acct")
	if err == nil || err.ApiStatusCode != 403 || !strings.Contains(err.ErrorMessage, "DENIED_FN") {
		t.Fatalf("err = %+v, want 403 DENIED_FN", err)
	}
}

func TestAuthorizeIDTMissingTokenIsStillDeniedWhenBound(t *testing.T) {
	tr, _, _ := newIDTTestTrigger(t, "true", "false", boundIdx, "FN-IDX")
	_, _, err := tr.authorizeIDT(idtMsg(""), "hdr-user", "hdr-acct")
	if err == nil || err.ApiStatusCode != 403 || !strings.Contains(err.ErrorMessage, "MISSING_IDT") {
		t.Fatalf("err = %+v, want 403 MISSING_IDT", err)
	}
}

func TestAuthorizeIDTUnboundDeniesWithoutAskingIdentity(t *testing.T) {
	unbound := node.IndexAccess{Status: node.IndexAccessUnbound, Index: "x-*", Cause: "no row"}
	tr, stub, _ := newIDTTestTrigger(t, "true", "false", unbound, "FN-IDX", "FN-APP")
	_, _, err := tr.authorizeIDT(idtMsg("good"), "hdr-user", "hdr-acct")
	if err == nil || err.ApiStatusCode != 403 || !strings.Contains(err.ErrorMessage, "INDEX_FUNCTION_UNBOUND") {
		t.Fatalf("err = %+v, want 403 INDEX_FUNCTION_UNBOUND", err)
	}
	if got := stub.functions(); len(got) != 0 {
		t.Fatalf("identity must not be asked, got %v", got)
	}
}

func TestAuthorizeIDTUnknownStatusDeniesLikeUnbound(t *testing.T) {
	garbage := node.IndexAccess{Status: "garbage"}
	tr, stub, _ := newIDTTestTrigger(t, "true", "false", garbage, "FN-IDX", "FN-APP")
	_, _, err := tr.authorizeIDT(idtMsg("good"), "hdr-user", "hdr-acct")
	if err == nil || err.ApiStatusCode != 403 || !strings.Contains(err.ErrorMessage, "INDEX_FUNCTION_UNBOUND") {
		t.Fatalf("err = %+v, want 403 INDEX_FUNCTION_UNBOUND", err)
	}
	if got := stub.functions(); len(got) != 0 {
		t.Fatalf("identity must not be asked, got %v", got)
	}
}

func TestAuthorizeIDTBoundWithBlankFunctionDeniesLikeUnbound(t *testing.T) {
	blank := node.IndexAccess{Status: node.IndexAccessBound, Index: "dev.cpe-*", FunctionID: "  "}
	tr, stub, _ := newIDTTestTrigger(t, "true", "false", blank, "FN-IDX", "FN-APP", "", "  ")
	_, _, err := tr.authorizeIDT(idtMsg("good"), "hdr-user", "hdr-acct")
	if err == nil || err.ApiStatusCode != 403 || !strings.Contains(err.ErrorMessage, "INDEX_FUNCTION_UNBOUND") {
		t.Fatalf("err = %+v, want 403 INDEX_FUNCTION_UNBOUND", err)
	}
	if got := stub.functions(); len(got) != 0 {
		t.Fatalf("identity must not be asked, got %v", got)
	}
}

func TestAuthorizeIDTWorkflowWithoutAnIndexKeepsTheAppFunction(t *testing.T) {
	for _, access := range []node.IndexAccess{{}, {Status: node.IndexAccessNone}} {
		tr, stub, buf := newIDTTestTrigger(t, "true", "false", access, "FN-APP")
		if _, _, err := tr.authorizeIDT(idtMsg("good"), "hdr-user", "hdr-acct"); err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		if got := stub.functions(); len(got) != 1 || got[0] != "FN-APP" {
			t.Fatalf("identity asked for %v, want [FN-APP]", got)
		}
		if !strings.Contains(buf.String(), "function=app-default") {
			t.Fatalf("log lacks function=app-default:\n%s", buf.String())
		}
	}
}

func TestAuthorizeIDTObserveOnlyNeverBlocksAnUnboundWorkflow(t *testing.T) {
	unbound := node.IndexAccess{Status: node.IndexAccessUnbound, Index: "x-*", Cause: "no row"}
	tr, _, buf := newIDTTestTrigger(t, "true", "true", unbound)
	user, acct, err := tr.authorizeIDT(idtMsg("good"), "hdr-user", "hdr-acct")
	if err != nil || user != "hdr-user" || acct != "hdr-acct" {
		t.Fatalf("observe-only must pass through unchanged: user=%q acct=%q err=%+v", user, acct, err)
	}
	waitFor(t, func() bool {
		return strings.Contains(buf.String(), "would_allow=false") && strings.Contains(buf.String(), "INDEX_FUNCTION_UNBOUND")
	})
}

func TestAuthorizeIDTObserveOnlyNeverOverridesWhenBoundAndGranted(t *testing.T) {
	tr, stub, _ := newIDTTestTrigger(t, "true", "true", boundIdx, "FN-IDX")
	user, acct, err := tr.authorizeIDT(idtMsg("good"), "hdr-user", "hdr-acct")
	if err != nil || user != "hdr-user" || acct != "hdr-acct" {
		t.Fatalf("observe-only must not override identity: user=%q acct=%q err=%+v", user, acct, err)
	}
	waitFor(t, func() bool {
		for _, fn := range stub.functions() {
			if fn == "FN-IDX" {
				return true
			}
		}
		return false
	})
}

func TestAuthorizeIDTValidationOffPassesThroughEvenWhenUnbound(t *testing.T) {
	unbound := node.IndexAccess{Status: node.IndexAccessUnbound, Index: "x-*", Cause: "no row"}
	tr, stub, _ := newIDTTestTrigger(t, "false", "false", unbound)
	user, acct, err := tr.authorizeIDT(idtMsg(""), "hdr-user", "hdr-acct")
	if err != nil || user != "hdr-user" || acct != "hdr-acct" {
		t.Fatalf("validation off must pass through: user=%q acct=%q err=%+v", user, acct, err)
	}
	if got := stub.functions(); len(got) != 0 {
		t.Fatalf("identity must not be asked, got %v", got)
	}
}
