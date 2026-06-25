// Package promptadmin exposes NATS endpoints to read/save/list prompt
// overrides and broadcasts changes so every instance hot-reconfigures.
package promptadmin

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/loader"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/render"
)

// maxContentBytes caps a flexible prompt (32KB).
const maxContentBytes = 32 * 1024

// overrideLint rejects obvious attempts to neutralize the fixed backbone.
// Heuristic, not a guarantee — code-side enforcement is the real backstop.
// The pattern requires a trigger verb followed (within 120 chars, same
// sentence) by an instruction-noun, covering both "ignore previous
// instructions" and "disregard the rules below" word orderings WITHOUT
// false-positiving benign uses like "ignore null values in all aggregations"
// (no rule/instruction noun).
var overrideLint = regexp.MustCompile(`(?i)\b(ignore|disregard|forget|override)\b[^.\n]{0,120}\b(instructions?|rules?|sections?|prompts?|guidelines?|policies|policy)\b`)

// ValidateContent enforces the spec's save-time rules: non-empty, size cap,
// env vars resolvable, {{templates}} in catalog, no override-attempt phrases.
func ValidateContent(content string, r *render.Renderer, lookupEnv func(string) (string, bool)) error {
	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("content is empty")
	}
	if len(content) > maxContentBytes {
		return fmt.Errorf("content exceeds %d bytes", maxContentBytes)
	}
	resolved, rerrs := loader.Resolve(content, lookupEnv)
	if len(rerrs) > 0 {
		return fmt.Errorf("unresolvable env vars: %v", rerrs)
	}
	resolvedStr, _ := resolved.(string)
	if err := r.Validate(resolvedStr); err != nil {
		return fmt.Errorf("template: %w", err)
	}
	if m := overrideLint.FindString(content); m != "" {
		return fmt.Errorf("content contains a rule-override phrase (%q) — rephrase", m)
	}
	return nil
}
