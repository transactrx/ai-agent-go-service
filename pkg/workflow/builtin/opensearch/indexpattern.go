// pkg/workflow/builtin/opensearch/indexpattern.go
package opensearch

import (
	"fmt"
	"regexp"
	"strings"
)

// compileIndexPattern turns an env-driven pattern (e.g. "prod.cpe-*") into a
// regex that recognizes acceptable indexPath ELEMENTS. The wildcard '*' is
// expanded to the character class [0-9.\-*] — digits, dots, dashes, and the
// wildcard char itself. This lets the LLM send a literal date
// ("prod.cpe-2026-04-30"), a narrower wildcard ("prod.cpe-2026-*"), or the
// workflow pattern itself ("prod.cpe-*"), and nothing else.
//
// Patterns without a wildcard compile to exact-match regexes.
// Patterns with multiple wildcards are rejected — cycle-1 only supports one.
func compileIndexPattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, fmt.Errorf("opensearch: allowedIndexPattern is empty")
	}
	n := strings.Count(pattern, "*")
	if n > 1 {
		return nil, fmt.Errorf("opensearch: allowedIndexPattern %q has %d wildcards; only one is supported", pattern, n)
	}
	if n == 0 {
		return regexp.Compile("^" + regexp.QuoteMeta(pattern) + "$")
	}
	star := strings.IndexByte(pattern, '*')
	prefix := regexp.QuoteMeta(pattern[:star])
	suffix := regexp.QuoteMeta(pattern[star+1:])
	return regexp.Compile("^" + prefix + `[0-9.\-*]+` + suffix + "$")
}

// validateIndexPath splits a comma-separated indexPath into elements and
// requires every element to match the compiled pattern.
func validateIndexPath(indexPath string, allowed *regexp.Regexp) error {
	if indexPath == "" {
		return fmt.Errorf("opensearch: indexPath is empty")
	}
	for _, raw := range strings.Split(indexPath, ",") {
		elem := strings.TrimSpace(raw)
		if elem == "" {
			return fmt.Errorf("opensearch: indexPath has empty element")
		}
		if !allowed.MatchString(elem) {
			return fmt.Errorf("opensearch: index %q is not in the allowed pattern", elem)
		}
	}
	return nil
}
