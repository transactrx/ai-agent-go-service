package rsassistant

import (
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestAgentAccess(t *testing.T) {
	cfg := Config{AppID: "APPX", FunctionID: "FN-APP"}
	cases := []struct {
		name     string
		in       node.IndexAccess
		wantFn   string
		wantSkip string
	}{
		{"zero value keeps the app function", node.IndexAccess{}, "FN-APP", ""},
		{"none keeps the app function", node.IndexAccess{Status: node.IndexAccessNone}, "FN-APP", ""},
		{"bound uses the index function", node.IndexAccess{Status: node.IndexAccessBound, FunctionID: "FN-IDX"}, "FN-IDX", ""},
		{"unbound is skipped", node.IndexAccess{Status: node.IndexAccessUnbound, Cause: "no row"}, "", "index function unbound: no row"},
		{"unbound without a cause still explains", node.IndexAccess{Status: node.IndexAccessUnbound}, "", "index function unbound: no usable function"},
		{"bound with a blank function is skipped", node.IndexAccess{Status: node.IndexAccessBound, FunctionID: "  "}, "", "index function unbound: bound access has no function id"},
		{"unknown status is skipped", node.IndexAccess{Status: "garbage"}, "", "index function unbound: unknown index access status"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, skip := agentAccess(cfg, tc.in)
			if skip != tc.wantSkip {
				t.Fatalf("skip = %q, want %q", skip, tc.wantSkip)
			}
			if tc.wantSkip != "" {
				return
			}
			if got.AppID != "APPX" || got.FunctionID != tc.wantFn {
				t.Fatalf("access = %+v, want appId APPX function %q", got, tc.wantFn)
			}
		})
	}
}
