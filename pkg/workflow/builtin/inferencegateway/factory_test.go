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
		{"alias ok", `{"alias":"MAX_MODEL"}`, ""},
		{"modelId ok", `{"modelId":"us.anthropic.claude-opus-5-5"}`, ""},
		{"lab+family ok", `{"lab":"anthropic","family":"claude-opus"}`, ""},
		{"none", `{}`, "exactly one of alias, modelId, lab+family"},
		{"two selectors", `{"alias":"A","modelId":"m"}`, "exactly one of alias, modelId, lab+family"},
		{"lab without family", `{"lab":"anthropic"}`, "lab and family must be set together"},
		{"family without lab", `{"family":"claude-opus"}`, "lab and family must be set together"},
		{"bad json", `{"alias":1}`, "parse config"},
		{"negative maxTokens", `{"alias":"A","maxTokens":-1}`, "maxTokens must not be negative"},
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

func TestNewLLMDefaults(t *testing.T) {
	g := newLLM(Config{Alias: "A"})
	if g.cfg.MaxTokens != 4096 || g.streamTimeout != 600*time.Second || g.idleTimeout != 120*time.Second {
		t.Fatalf("defaults = maxTokens %d stream %s idle %s", g.cfg.MaxTokens, g.streamTimeout, g.idleTimeout)
	}
	g = newLLM(Config{Alias: "A", MaxTokens: 10, StreamTimeoutSeconds: 5, IdleTimeoutSeconds: 2})
	if g.cfg.MaxTokens != 10 || g.streamTimeout != 5*time.Second || g.idleTimeout != 2*time.Second {
		t.Fatalf("overrides not applied: %+v", g)
	}
}
