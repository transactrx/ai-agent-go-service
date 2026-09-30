package inferencegateway

import (
	"strings"
	"testing"
)

func TestInvokeSubject(t *testing.T) {
	t.Setenv(basePathEnv, "")
	if got := invokeSubject(""); got != "example.inferenceGateway.invokeStream" {
		t.Fatalf("default = %q", got)
	}
	t.Setenv(basePathEnv, " trx.inferenceGateway ")
	if got := invokeSubject(""); got != "trx.inferenceGateway.invokeStream" {
		t.Fatalf("env = %q", got)
	}
	if got := invokeSubject(" trx.other "); got != "trx.other.invokeStream" {
		t.Fatalf("override = %q", got)
	}
}

func TestParseAck(t *testing.T) {
	ok := `{"accepted":true,"modelId":"anthropic.claude-opus-5-5","invokeId":"us.anthropic.claude-opus-5-5","streamSubject":"_INBOX.x"}`
	cases := []struct {
		name, status, body, wantInvoke, wantErr string
	}{
		{"success", "200", ok, "us.anthropic.claude-opus-5-5", ""},
		{"no status header", "", ok, "us.anthropic.claude-opus-5-5", ""},
		{"error status with message", "400", `{"status":400,"errorMessage":"alias NOPE not found"}`, "", "gateway status 400: alias NOPE not found"},
		{"error status raw body", "500", `Server Error`, "", "gateway status 500: Server Error"},
		{"non numeric status", "abc", ok, "", "gateway status abc"},
		{"not accepted", "200", `{"accepted":false}`, "", "gateway did not accept"},
		{"not json", "200", `nope`, "", "not JSON"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ack, err := parseAck(c.status, []byte(c.body))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, c.wantErr)
				}
				return
			}
			if err != nil || ack.InvokeID != c.wantInvoke {
				t.Fatalf("ack = %+v, err = %v", ack, err)
			}
		})
	}
}
