package loader

import (
	"fmt"
	"regexp"
	"strings"
)

// sensitiveNamePattern matches field names whose suffix is a sensitive
// keyword (case-insensitive), with an optional "Env" tail. Anchored at end
// of name so substrings (e.g., "maxTokens") don't false-match.
//
// Matches: password, passwordEnv, PASSWORD, token, apiToken, apiKey,
// apiKeyEnv, dsn, dsnEnv.
// Does NOT match: maxTokens, tokens, dsnUrl, etc.
var sensitiveNamePattern = regexp.MustCompile(`(?i)(password|secret|apikey|api_key|token|dsn)(env)?$`)

// ValidateSensitiveFields walks workflowJSON and reports every field whose
// NAME is sensitive but whose form violates spec §8.3:
//   - Literal value (no *Env suffix) → rejected.
//   - *Env form whose value contains "${" → rejected (must be bare env var name).
func ValidateSensitiveFields(v any) []error {
	var errs []error
	var walk func(node any, path string)
	walk = func(node any, path string) {
		switch n := node.(type) {
		case map[string]any:
			for k, val := range n {
				full := path + "." + k
				if sensitiveNamePattern.MatchString(k) {
					if !strings.HasSuffix(k, "Env") {
						errs = append(errs, fmt.Errorf(
							"%s: sensitive field %q must use *Env suffix (e.g., %sEnv) and reference an env-var NAME",
							full, k, k))
					}
					if strings.HasSuffix(k, "Env") {
						if s, ok := val.(string); ok && strings.Contains(s, "${") {
							errs = append(errs, fmt.Errorf(
								"%s: sensitive field value must be an env-var NAME, not a ${VAR} reference", full))
						}
					}
				}
				walk(val, full)
			}
		case []any:
			for i, val := range n {
				walk(val, fmt.Sprintf("%s[%d]", path, i))
			}
		}
	}
	walk(v, "$")
	return errs
}
