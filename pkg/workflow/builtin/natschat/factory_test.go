package natschat

import (
	"encoding/json"
	"testing"
)

func TestIdentitySourceDefaultsNameAndTz(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"responseMode":"streaming"}`))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	tr := n.(*natsChatTrigger)
	if tr.cfg.IdentitySource.UserNameHeader != "X-User-Name" {
		t.Errorf("UserNameHeader default = %q", tr.cfg.IdentitySource.UserNameHeader)
	}
	if tr.cfg.IdentitySource.TimeZoneHeader != "X-Time-Zone" {
		t.Errorf("TimeZoneHeader default = %q", tr.cfg.IdentitySource.TimeZoneHeader)
	}
}
