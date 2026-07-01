package webbridge

import "testing"

func TestAllowAll_Authorizes(t *testing.T) {
	allow, reason := AllowAll{}.Authorize("acct", "idx", "wf")
	if !allow || reason != "" {
		t.Fatalf("AllowAll should allow with no reason, got allow=%v reason=%q", allow, reason)
	}
}

func TestIdentity_ZeroValueIsEmpty(t *testing.T) {
	var id Identity
	if id.AccountID != "" || id.UserID != "" || id.UserName != "" || id.TimeZone != "" || id.SessionID != "" {
		t.Fatal("zero Identity must be empty")
	}
}
