package natschat

import (
	"encoding/json"
	"testing"
)

// newTestTrigger builds a trigger through the real Factory so config defaults
// apply, without running Init (no NATS in unit tests).
func newTestTrigger(t *testing.T, cfgJSON string) *natsChatTrigger {
	t.Helper()
	n, err := Factory.New(json.RawMessage(cfgJSON))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	return n.(*natsChatTrigger)
}

func TestResolveResponseMode(t *testing.T) {
	cases := []struct {
		name      string
		cfg       string
		requested string
		want      string
		wantErr   bool
	}{
		{"flag off, no field -> config default", `{"responseMode":"streaming"}`, "", "streaming", false},
		{"flag off, field sent -> silently ignored", `{"responseMode":"streaming"}`, "single", "streaming", false},
		{"flag off, invalid field -> still ignored", `{"responseMode":"streaming"}`, "bogus", "streaming", false},
		{"flag on, no field -> config default", `{"responseMode":"single","allowResponseModeOverride":true}`, "", "single", false},
		{"flag on, override to streaming", `{"responseMode":"single","allowResponseModeOverride":true}`, "streaming", "streaming", false},
		{"flag on, override to single", `{"responseMode":"streaming","allowResponseModeOverride":true}`, "single", "single", false},
		{"flag on, invalid -> error", `{"responseMode":"single","allowResponseModeOverride":true}`, "bogus", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTestTrigger(t, tc.cfg)
			got, err := tr.resolveResponseMode(tc.requested)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got mode %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("mode = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestChatRequestBodyParsesResponseMode(t *testing.T) {
	var b chatRequestBody
	if err := json.Unmarshal([]byte(`{"message":"q","responseMode":"single"}`), &b); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if b.ResponseMode != "single" {
		t.Errorf("ResponseMode = %q, want %q", b.ResponseMode, "single")
	}
}
