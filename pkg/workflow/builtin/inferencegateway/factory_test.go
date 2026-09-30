package inferencegateway

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestFactoryConfigValidation(t *testing.T) {
	cases := []struct {
		name, raw, wantErr string
	}{
		{"alias ok", `{"alias":"POWERLINE_CLAIM_SEARCH_MODEL"}`, ""},
		{"alias with timeouts and basePath", `{"alias":"A1","streamTimeoutSeconds":5,"idleTimeoutSeconds":2,"basePath":"trx.x"}`, ""},
		{"none", `{}`, "alias is required"},
		{"empty alias", `{"alias":""}`, "alias is required"},
		{"model id in alias", `{"alias":"us.anthropic.claude-opus-5-5"}`, "alias must match"},
		{"lowercase alias", `{"alias":"powerline"}`, "alias must match"},
		{"one char alias", `{"alias":"A"}`, "alias must match"},
		{"bad json", `{"alias":1}`, "parse config"},
		{"negative timeout", `{"alias":"A1","idleTimeoutSeconds":-1}`, "timeouts must be positive"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, err := Factory.New(json.RawMessage(c.raw))
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if n.Spec().Type != "ai/inference-gateway" || n.Spec().Role != node.RoleLLM {
					t.Fatalf("spec = %+v", n.Spec())
				}
				if _, ok := n.(node.LLMProvider); !ok {
					t.Fatal("node must implement LLMProvider")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, c.wantErr)
			}
		})
	}
}

// Every knob and selector the gateway alias now owns must be refused at load,
// so a stale workflow JSON (or an inherited base key) fails loudly.
func TestFactoryRejectsUnknownFields(t *testing.T) {
	for _, key := range []string{"maxTokens", "temperature", "modelId", "lab", "family", "model", "region", "autoUpdate"} {
		raw := `{"alias":"A1","` + key + `":1}`
		_, err := Factory.New(json.RawMessage(raw))
		if err == nil || !strings.Contains(err.Error(), `unknown field "`+key+`"`) {
			t.Fatalf("%s: err = %v, want unknown field", key, err)
		}
	}
}

func TestNewLLMDefaults(t *testing.T) {
	g := newLLM(Config{Alias: "A1"})
	if g.streamTimeout != 600*time.Second || g.idleTimeout != 120*time.Second {
		t.Fatalf("defaults = stream %s idle %s", g.streamTimeout, g.idleTimeout)
	}
	g = newLLM(Config{Alias: "A1", StreamTimeoutSeconds: 5, IdleTimeoutSeconds: 2})
	if g.streamTimeout != 5*time.Second || g.idleTimeout != 2*time.Second {
		t.Fatalf("overrides not applied: %+v", g)
	}
}
