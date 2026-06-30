// Package loader contains the workflow-load pipeline: envsubst, schema
// validation, sensitive-name validation, factory dispatch, port validation,
// and Init orchestration. See spec §4.
package loader

import (
	"fmt"
	"regexp"
)

var envVarRegex = regexp.MustCompile(`\$\{([A-Z_][A-Z0-9_]*)(?::([^}]*))?\}`)

// ResolveError reports a single unresolved env var with its JSON path.
type ResolveError struct {
	Path string
	Var  string
}

func (e ResolveError) Error() string {
	return fmt.Sprintf("unresolved env var %s at %s", e.Var, e.Path)
}

// Resolve walks v (any JSON-like structure) and substitutes ${VAR} /
// ${VAR:default} in string leaves. Returns the rewritten structure plus the
// list of errors. The lookup function returns (value, present).
func Resolve(v any, lookup func(string) (string, bool)) (any, []ResolveError) {
	var errs []ResolveError
	var walk func(node any, path string) any
	walk = func(node any, path string) any {
		switch n := node.(type) {
		case map[string]any:
			for k, val := range n {
				n[k] = walk(val, path+"."+k)
			}
			return n
		case []any:
			for i, val := range n {
				n[i] = walk(val, fmt.Sprintf("%s[%d]", path, i))
			}
			return n
		case string:
			return envVarRegex.ReplaceAllStringFunc(n, func(match string) string {
				m := envVarRegex.FindStringSubmatch(match)
				varName, def := m[1], m[2]
				if val, ok := lookup(varName); ok && val != "" {
					return val
				}
				if def != "" {
					return def
				}
				errs = append(errs, ResolveError{Path: path, Var: varName})
				return match
			})
		}
		return node
	}
	out := walk(v, "$")
	return out, errs
}
