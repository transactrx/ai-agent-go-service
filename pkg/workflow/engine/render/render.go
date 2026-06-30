// Package render expands {{ }} placeholders in workflow config strings at
// request time. Catalog: nowUtc[:fmt], nowUtcDate, nowUtcWeekday, sessionId,
// userId, requestId, trigger.<jsonpath>, nodes.<id>.<jsonpath>, and the
// per-request identity/mapping additions userName, userTimeZone, nowLocal[:fmt],
// nowLocalWeekday, indexMapping.
package render

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// resolverFn produces a string for a placeholder. The arg is whatever follows
// the first separator (":" for nowUtc, "." for trigger/nodes).
type resolverFn func(arg string, ctx node.RenderCtx) (string, error)

// Renderer holds the placeholder catalog.
type Renderer struct {
	catalog map[string]resolverFn
}

var templateRegex = regexp.MustCompile(`\{\{([^{}]+)\}\}`)

// NewRenderer returns a renderer wired with the cycle-1 catalog.
func NewRenderer() *Renderer {
	r := &Renderer{catalog: map[string]resolverFn{}}
	r.catalog["nowUtc"] = func(arg string, c node.RenderCtx) (string, error) {
		if arg == "" {
			return c.Now.UTC().Format("2006-01-02 15:04"), nil
		}
		return c.Now.UTC().Format(arg), nil
	}
	r.catalog["nowUtcDate"] = func(_ string, c node.RenderCtx) (string, error) {
		return c.Now.UTC().Format("2006-01-02"), nil
	}
	r.catalog["nowUtcWeekday"] = func(_ string, c node.RenderCtx) (string, error) {
		return c.Now.UTC().Format("Monday"), nil
	}
	r.catalog["sessionId"] = func(_ string, c node.RenderCtx) (string, error) { return c.SessionID, nil }
	r.catalog["userId"] = func(_ string, c node.RenderCtx) (string, error) { return c.UserID, nil }
	r.catalog["requestId"] = func(_ string, c node.RenderCtx) (string, error) { return c.RequestID, nil }
	r.catalog["trigger"] = func(arg string, c node.RenderCtx) (string, error) {
		return jsonPath(c.TriggerEvent, arg)
	}
	r.catalog["nodes"] = func(arg string, c node.RenderCtx) (string, error) {
		// arg = "<nodeId>.<rest>"; strip nodeId.
		dot := strings.IndexByte(arg, '.')
		if dot < 0 {
			return jsonPath(c.NodeOutputs[arg], "")
		}
		nodeID := arg[:dot]
		return jsonPath(c.NodeOutputs[nodeID], arg[dot+1:])
	}
	// userLocation resolves the asker's tz, falling back to UTC on empty/invalid.
	userLocation := func(c node.RenderCtx) *time.Location {
		if c.TimeZone != "" {
			if loc, err := time.LoadLocation(c.TimeZone); err == nil {
				return loc
			}
		}
		return time.UTC
	}
	r.catalog["userName"] = func(_ string, c node.RenderCtx) (string, error) {
		if c.UserName == "" {
			return "the user", nil
		}
		return c.UserName, nil
	}
	r.catalog["userTimeZone"] = func(_ string, c node.RenderCtx) (string, error) {
		// Echo only a tz Go can actually load; otherwise report UTC so the prompt
		// stays consistent with nowLocal (which also falls back to UTC) and a
		// garbage client-supplied value can't be injected as prose.
		if c.TimeZone == "" {
			return "UTC", nil
		}
		if _, err := time.LoadLocation(c.TimeZone); err != nil {
			return "UTC", nil
		}
		return c.TimeZone, nil
	}
	r.catalog["nowLocal"] = func(arg string, c node.RenderCtx) (string, error) {
		t := c.Now.In(userLocation(c))
		if arg == "" {
			return t.Format("2006-01-02 15:04"), nil
		}
		return t.Format(arg), nil
	}
	r.catalog["nowLocalWeekday"] = func(_ string, c node.RenderCtx) (string, error) {
		return c.Now.In(userLocation(c)).Format("Monday"), nil
	}
	r.catalog["indexMapping"] = func(_ string, c node.RenderCtx) (string, error) {
		if c.IndexMapping == "" {
			return "(index mapping temporarily unavailable)", nil
		}
		return c.IndexMapping, nil
	}
	return r
}

// Render expands placeholders in template using ctx.
func (r *Renderer) Render(template string, ctx node.RenderCtx) (string, error) {
	var firstErr error
	out := templateRegex.ReplaceAllStringFunc(template, func(match string) string {
		if firstErr != nil {
			return match
		}
		inner := templateRegex.FindStringSubmatch(match)[1]
		name, arg := splitNameArg(inner)
		resolver, ok := r.catalog[name]
		if !ok {
			firstErr = fmt.Errorf("unknown placeholder %q at %q", name, inner)
			return match
		}
		val, err := resolver(arg, ctx)
		if err != nil {
			firstErr = fmt.Errorf("resolving %q: %w", inner, err)
			return match
		}
		return val
	})
	return out, firstErr
}

// Validate scans template and confirms every placeholder name is in the
// catalog. Used at workflow load time. Does NOT verify time-format strings
// (Go's time.Format silently echoes unknown verbs).
func (r *Renderer) Validate(template string) error {
	matches := templateRegex.FindAllStringSubmatch(template, -1)
	for _, m := range matches {
		name, _ := splitNameArg(m[1])
		if _, ok := r.catalog[name]; !ok {
			return fmt.Errorf("unknown placeholder %q in template", name)
		}
	}
	return nil
}

// splitNameArg splits "name:arg" or "name.arg" — picks whichever appears first.
func splitNameArg(inner string) (string, string) {
	for i := 0; i < len(inner); i++ {
		if inner[i] == ':' || inner[i] == '.' {
			return inner[:i], inner[i+1:]
		}
	}
	return inner, ""
}

// jsonPath does a dotted-path lookup against any. Empty path returns the
// formatted root.
func jsonPath(root any, path string) (string, error) {
	if root == nil {
		if path == "" {
			return "", nil
		}
		return "", fmt.Errorf("nil root for path %q", path)
	}
	if path == "" {
		return formatScalar(root)
	}
	parts := strings.Split(path, ".")
	cur := root
	for i, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", fmt.Errorf("path %q: not an object at segment %d", path, i)
		}
		next, ok := m[p]
		if !ok {
			return "", fmt.Errorf("path %q: missing key %q", path, p)
		}
		cur = next
	}
	return formatScalar(cur)
}

func formatScalar(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case fmt.Stringer:
		return x.String(), nil
	default:
		return fmt.Sprint(v), nil
	}
}
