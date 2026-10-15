package idt

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// identityStub answers validateInternalToken on a real NATS server. Token
// "good" is granted exactly the functions passed to newIdentityStub.
type identityStub struct {
	mu     sync.Mutex
	asked  []string
	grants map[string]bool
}

func (s *identityStub) functions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.asked...)
}

func newIdentityStub(t *testing.T, grants ...string) (*nats.Conn, *identityStub) {
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
	s := &identityStub{grants: map[string]bool{}}
	for _, g := range grants {
		s.grants[g] = true
	}
	if _, err := nc.Subscribe("test.identity.validateInternalToken", func(m *nats.Msg) {
		var req struct {
			IDT        string `json:"idt"`
			AgentID    string `json:"agentId"`
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

func setIDTEnv(t *testing.T, enabled string) {
	t.Helper()
	t.Setenv("IDT_VALIDATION", enabled)
	t.Setenv("IDT_FAIL_OPEN", "false")
	t.Setenv("IDT_OBSERVE_ONLY", "false")
	t.Setenv("APP_ID", "APPX")
	t.Setenv("APP_FUNCTION_ID", "FN-APP")
	t.Setenv("NATS_IDENTITY_BASE_PATH", "test.identity")
	t.Setenv("NATS_IDENTITY_VALIDATE_SUBJECT", "validateInternalToken")
}

func TestValidateForFunctionUsesTheGivenFunction(t *testing.T) {
	setIDTEnv(t, "true")
	nc, stub := newIdentityStub(t, "FN-IDX")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	allow, reason, user, acct := v.ValidateForFunction("good", "FN-IDX")
	if !allow || reason != "" || user != "u-1" || acct != "acct-9" {
		t.Fatalf("allow=%v reason=%q user=%q acct=%q", allow, reason, user, acct)
	}
	if got := stub.functions(); len(got) != 1 || got[0] != "FN-IDX" {
		t.Fatalf("identity was asked for %v, want [FN-IDX]", got)
	}
}

func TestValidateForFunctionDeniesAUserWhoLacksIt(t *testing.T) {
	setIDTEnv(t, "true")
	nc, _ := newIdentityStub(t, "FN-APP") // holds the old app-wide function only
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	allow, reason, _, _ := v.ValidateForFunction("good", "FN-IDX")
	if allow || reason != "DENIED_FN" {
		t.Fatalf("allow=%v reason=%q, want deny DENIED_FN", allow, reason)
	}
}

func TestValidateIfEnabledStillSubstitutesTheEnvFunction(t *testing.T) {
	setIDTEnv(t, "true")
	nc, stub := newIdentityStub(t, "FN-APP")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	allow, _, _, _ := v.ValidateIfEnabled("good", "some-workflow-id")
	if !allow {
		t.Fatal("env function must still be used and granted")
	}
	if got := stub.functions(); len(got) != 1 || got[0] != "FN-APP" {
		t.Fatalf("identity was asked for %v, want [FN-APP]", got)
	}
}

func TestValidateForFunctionMissingTokenIsDeniedWithoutAskingIdentity(t *testing.T) {
	setIDTEnv(t, "true")
	nc, stub := newIdentityStub(t, "FN-IDX")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	allow, reason, _, _ := v.ValidateForFunction("", "FN-IDX")
	if allow || reason != "MISSING_IDT" {
		t.Fatalf("allow=%v reason=%q, want deny MISSING_IDT", allow, reason)
	}
	if got := stub.functions(); len(got) != 0 {
		t.Fatalf("identity must not be asked, got %v", got)
	}
}

func TestValidateForFunctionPassesThroughWhenValidationIsOff(t *testing.T) {
	setIDTEnv(t, "false")
	nc, stub := newIdentityStub(t)
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	allow, _, _, _ := v.ValidateForFunction("", "FN-IDX")
	if !allow {
		t.Fatal("validation off must pass through")
	}
	if got := stub.functions(); len(got) != 0 {
		t.Fatalf("identity must not be asked, got %v", got)
	}
}

func cacheEnv(t *testing.T, secs string) {
	t.Helper()
	setIDTEnv(t, "true")
	t.Setenv("IDT_VALIDATE_CACHE_SECONDS", secs)
}

func TestValidateDoesNotCacheByDefault(t *testing.T) {
	setIDTEnv(t, "true")
	nc, stub := newIdentityStub(t, "FN-IDX")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	v.ValidateForFunction("good", "FN-IDX")
	v.ValidateForFunction("good", "FN-IDX")
	if got := stub.functions(); len(got) != 2 {
		t.Fatalf("identity asked %d times, want 2: the cache must be opt-in", len(got))
	}
}

func TestValidateCachesAnAllowWhenEnabled(t *testing.T) {
	cacheEnv(t, "60")
	nc, stub := newIdentityStub(t, "FN-IDX")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if allow, _, user, acct := v.ValidateForFunction("good", "FN-IDX"); !allow || user != "u-1" || acct != "acct-9" {
			t.Fatalf("call %d: allow=%v user=%q acct=%q", i, allow, user, acct)
		}
	}
	if got := stub.functions(); len(got) != 1 {
		t.Fatalf("identity asked %d times, want 1", len(got))
	}
}

func TestValidateNeverCachesADeny(t *testing.T) {
	cacheEnv(t, "60")
	nc, stub := newIdentityStub(t, "FN-OTHER")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	v.ValidateForFunction("good", "FN-IDX")
	v.ValidateForFunction("good", "FN-IDX")
	if got := stub.functions(); len(got) != 2 {
		t.Fatalf("identity asked %d times, want 2", len(got))
	}
}

func TestValidateCacheIsPerFunctionAndToken(t *testing.T) {
	cacheEnv(t, "60")
	nc, stub := newIdentityStub(t, "FN-A", "FN-B")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	v.ValidateForFunction("good", "FN-A")
	v.ValidateForFunction("good", "FN-B")
	if allow, _, _, _ := v.ValidateForFunction("forged", "FN-A"); allow {
		t.Fatal("a different token must not ride a cached allow")
	}
	if got := stub.functions(); len(got) != 3 {
		t.Fatalf("identity asked %v, want 3 calls", got)
	}
}

func TestValidateCacheExpires(t *testing.T) {
	cacheEnv(t, "60")
	nc, stub := newIdentityStub(t, "FN-IDX")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	v.now = func() time.Time { return now }
	v.ValidateForFunction("good", "FN-IDX")
	now = now.Add(61 * time.Second)
	v.ValidateForFunction("good", "FN-IDX")
	if got := stub.functions(); len(got) != 2 {
		t.Fatalf("identity asked %d times, want 2 after expiry", len(got))
	}
}

func TestValidateDoesNotCacheInObserveOnly(t *testing.T) {
	cacheEnv(t, "60")
	t.Setenv("IDT_OBSERVE_ONLY", "true")
	nc, stub := newIdentityStub(t, "FN-IDX")
	v, err := NewFromEnv(nc)
	if err != nil {
		t.Fatal(err)
	}
	v.ValidateForFunction("good", "FN-IDX")
	v.ValidateForFunction("good", "FN-IDX")
	if got := stub.functions(); len(got) != 2 {
		t.Fatalf("identity asked %d times, want 2", len(got))
	}
}
