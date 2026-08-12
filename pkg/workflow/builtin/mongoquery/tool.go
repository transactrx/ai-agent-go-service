// Package mongoquery is the tool/mongo-query node: read-only find/aggregate/count.
package mongoquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// resolveEJSONDates walks the filter/pipeline tree and converts MongoDB
// Extended-JSON Date markers into actual time.Time values that the Go driver
// recognises as BSON Date comparisons.
//
// Accepted forms (all converted in place):
//   - {"$date": "2026-05-19T00:00:00Z"}            (extended-JSON canonical)
//   - {"$date": "2026-05-19T00:00:00Z+00:00"}      (RFC 3339 with offset)
//   - {"$date": <number>}                           (epoch milliseconds)
//   - {"$dateMillis": <number>}                     (explicit epoch ms, our own
//     alias — matches what the
//     {{startOfTodayUserTzMillis}}
//     template emits)
//
// Anything that doesn't parse is left untouched, so non-date markers (e.g.
// `$gte`, `$in`, etc.) flow through to the driver normally.
//
// Without this pass, an LLM-generated filter like
//
//	{"dateAdded": {"$gte": {"$date": "2026-05-19T00:00:00Z"}}}
//
// reaches Mongo as a *document literal* under `$gte` rather than a Date value,
// and matches nothing — which is what was happening in the wild.
func resolveEJSONDates(v any) any {
	switch x := v.(type) {
	case map[string]any:
		// Single-key date marker — replace with time.Time
		if len(x) == 1 {
			if raw, ok := x["$date"]; ok {
				if t, ok := parseDateLike(raw); ok {
					return t
				}
			}
			if raw, ok := x["$dateMillis"]; ok {
				if t, ok := parseEpochMillis(raw); ok {
					return t
				}
			}
		}
		for k, child := range x {
			x[k] = resolveEJSONDates(child)
		}
		return x
	case []any:
		for i, child := range x {
			x[i] = resolveEJSONDates(child)
		}
		return x
	case []map[string]any:
		for i, child := range x {
			x[i] = resolveEJSONDates(child).(map[string]any)
		}
		return x
	default:
		return x
	}
}

func parseDateLike(raw any) (time.Time, bool) {
	switch v := raw.(type) {
	case string:
		// Accept RFC 3339 (with or without offset) and the JS-style
		// 2026-05-19T00:00:00.000Z form.
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z", "2006-01-02"} {
			if t, err := time.Parse(layout, v); err == nil {
				return t.UTC(), true
			}
		}
		return time.Time{}, false
	case float64:
		return time.UnixMilli(int64(v)).UTC(), true
	case int64:
		return time.UnixMilli(v).UTC(), true
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return time.UnixMilli(n).UTC(), true
		}
	}
	return time.Time{}, false
}

func parseEpochMillis(raw any) (time.Time, bool) {
	switch v := raw.(type) {
	case float64:
		return time.UnixMilli(int64(v)).UTC(), true
	case int64:
		return time.UnixMilli(v).UTC(), true
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return time.UnixMilli(n).UTC(), true
		}
	}
	return time.Time{}, false
}

// FindOpts groups find-specific options. Kept here so MongoAPI's
// surface stays stable.
type FindOpts struct {
	Projection map[string]any `json:"projection,omitempty"`
	Sort       map[string]any `json:"sort,omitempty"`
	Limit      int            `json:"limit,omitempty"`
}

// MongoAPI is the surface this tool uses. The production implementation
// wraps *mongo.Client; tests use fakeMongo (defined in tool_test.go).
type MongoAPI interface {
	Find(ctx context.Context, db, coll string, filter map[string]any, opts FindOpts) ([]map[string]any, error)
	Count(ctx context.Context, db, coll string, filter map[string]any) (int64, error)
	Aggregate(ctx context.Context, db, coll string, pipeline []map[string]any) ([]map[string]any, error)
}

// ConnConfig holds per-connection settings.
type ConnConfig struct {
	URIEnv           string   `json:"uriEnv"` // env VAR NAME holding the full Mongo URI
	AllowedDatabases []string `json:"allowedDatabases"`
	MaxDocsDefault   int      `json:"maxDocsDefault,omitempty"`
	QueryTimeoutMs   int      `json:"queryTimeoutMs,omitempty"`
}

// Config is decoded from the workflow-JSON config block.
type Config struct {
	Connections     map[string]ConnConfig `json:"connections"`
	ToolName        string                `json:"toolName,omitempty"`      // default "mongo_query"
	ToolDescription string                `json:"toolDescription"`         // required
	FailurePolicy   string                `json:"failurePolicy,omitempty"` // default "surface-to-llm"
}

type queryInput struct {
	Connection string           `json:"connection"`
	Database   string           `json:"database"`
	Collection string           `json:"collection"`
	Op         string           `json:"op"`
	Filter     map[string]any   `json:"filter,omitempty"`
	Projection map[string]any   `json:"projection,omitempty"`
	Sort       map[string]any   `json:"sort,omitempty"`
	Limit      int              `json:"limit,omitempty"`
	Pipeline   []map[string]any `json:"pipeline,omitempty"`
}

// queryOutput is the JSON shape returned to the LLM. Notes on omitempty:
//   - Docs has NO omitempty so empty result sets render as "docs": [] instead
//     of being silently dropped (which left the LLM seeing only {"op":"find"}
//     and incorrectly inferring a connection / tool failure).
//   - Count is a pointer so count=0 still marshals; non-count ops set it nil
//     and the field is dropped via omitempty.
//   - Truncated marks that the doc cap bit — the LLM should narrow the query
//     rather than trust a partial result as complete.
type queryOutput struct {
	Op        string           `json:"op"`
	Count     *int64           `json:"count,omitempty"`
	Docs      []map[string]any `json:"docs"`
	Truncated bool             `json:"truncated,omitempty"`
}

var allowedOps = map[string]bool{"find": true, "aggregate": true, "count": true}

// forbiddenOperators are Mongo operators/stages that write, execute
// server-side JavaScript, or otherwise breach the tool's read-only contract.
// The read-only DB credential is the last line of defense; this is the first.
// Keys are matched case-sensitively against any map key anywhere in the
// filter or pipeline tree.
var forbiddenOperators = map[string]string{
	"$out":               "writes the aggregation result into a collection",
	"$merge":             "writes/updates documents in a collection",
	"$where":             "runs server-side JavaScript",
	"$function":          "runs server-side JavaScript",
	"$accumulator":       "runs server-side JavaScript",
	"$listLocalSessions": "exposes server session state",
}

// scanForbidden walks a decoded filter/pipeline tree and returns the first
// forbidden operator it finds, or "" if the tree is clean.
func scanForbidden(v any) string {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if _, bad := forbiddenOperators[k]; bad {
				return k
			}
			if found := scanForbidden(child); found != "" {
				return found
			}
		}
	case []any:
		for _, child := range x {
			if found := scanForbidden(child); found != "" {
				return found
			}
		}
	case []map[string]any:
		for _, child := range x {
			if found := scanForbidden(child); found != "" {
				return found
			}
		}
	}
	return ""
}

// Tool implements the tool/mongo-query node.
type Tool struct {
	node.BaseTool
	cfg     Config
	clients map[string]MongoAPI
	// defaultDBs maps connection name -> the database named in the
	// connection URI's path. Populated at Init; the URI (and therefore the
	// per-environment database it names) comes from each deployment's own
	// secret, so this is the environment-correct default.
	defaultDBs map[string]string
	owned      []*mongo.Client // closed in Close()
}

// uriDefaultDB extracts the database from a Mongo connection URI path
// (mongodb://.../<db>?..., mongodb+srv://.../<db>?...). Returns "" when the
// URI names no database or cannot be parsed.
func uriDefaultDB(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return ""
	}
	return strings.Trim(u.Path, "/")
}

func (t *Tool) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type:        NodeType,
		Role:        node.RoleTool,
		Description: "Read-only find/aggregate/count against named Mongo connections.",
		OutputPorts: []node.PortSpec{{Name: node.PortAITool, Direction: node.PortOut, Cardinality: node.CardOne}},
	}
}

func (t *Tool) Init(ctx context.Context, env node.NodeEnv) error {
	t.InitRetry(env)
	if t.clients != nil { // test path
		return nil
	}
	t.clients = map[string]MongoAPI{}
	t.defaultDBs = map[string]string{}
	for name, cc := range t.cfg.Connections {
		uri, err := env.Secret(cc.URIEnv)
		if err != nil {
			return fmt.Errorf("mongo-query %s: %w", name, err)
		}
		mc, err := mongo.Connect(options.Client().ApplyURI(uri.Reveal()))
		if err != nil {
			return fmt.Errorf("mongo-query %s: connect: %w", name, err)
		}
		t.clients[name] = &clientAdapter{c: mc}
		t.defaultDBs[name] = uriDefaultDB(uri.Reveal())
		t.owned = append(t.owned, mc)
	}
	return nil
}

func (t *Tool) Close(ctx context.Context) error {
	for _, mc := range t.owned {
		_ = mc.Disconnect(ctx)
	}
	return nil
}

// ToolSpec returns the LLM-facing contract for this tool.
func (t *Tool) ToolSpec() node.ToolSpec {
	name := t.cfg.ToolName
	if name == "" {
		name = "mongo_query"
	}
	return node.ToolSpec{
		Name:        name,
		Description: t.cfg.ToolDescription,
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"connection":{"type":"string"},
				"database":{"type":"string","description":"Optional; defaults to the connection's own database. Omit unless the connection explicitly allows others."},
				"collection":{"type":"string"},
				"op":{"type":"string","enum":["find","aggregate","count"]},
				"filter":{"type":"object"},
				"projection":{"type":"object"},"sort":{"type":"object"},"limit":{"type":"integer"},
				"pipeline":{"type":"array","items":{"type":"object"}}
			},
			"required":["connection","collection","op"]
		}`),
	}
}

// FailurePolicy returns the configured policy, defaulting to surface-to-llm.
func (t *Tool) FailurePolicy() node.FailurePolicy {
	if t.cfg.FailurePolicy == "" {
		return node.FailureSurfaceToLLM
	}
	return node.FailurePolicy(t.cfg.FailurePolicy)
}

// Invoke executes the requested read-only Mongo operation.
func (t *Tool) Invoke(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	var in queryInput
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("parse args: %w", err)
	}
	if !allowedOps[in.Op] {
		return nil, fmt.Errorf("mongo-query refuses op %q (allowed: find, aggregate, count)", in.Op)
	}
	cc, ok := t.cfg.Connections[in.Connection]
	if !ok {
		return nil, fmt.Errorf("unknown connection %q", in.Connection)
	}
	// Resolve the database. The connection URI (from the deployment's own
	// secret) names the environment-correct database; an omitted input
	// database falls back to it, and an empty allowedDatabases list means
	// "only the URI's database". A non-empty allowedDatabases keeps the
	// explicit allowlist contract.
	defaultDB := t.defaultDBs[in.Connection]
	if in.Database == "" {
		in.Database = defaultDB
		if in.Database == "" {
			return nil, fmt.Errorf("database is required: connection %q's URI names no default database", in.Connection)
		}
	}
	if len(cc.AllowedDatabases) > 0 {
		if !containsString(cc.AllowedDatabases, in.Database) {
			return nil, fmt.Errorf("database %q not in allowed list %v for connection %q", in.Database, cc.AllowedDatabases, in.Connection)
		}
	} else if in.Database != defaultDB {
		return nil, fmt.Errorf("database %q not allowed for connection %q (only its default database %q; omit \"database\" to use it)", in.Database, in.Connection, defaultDB)
	}
	api, ok := t.clients[in.Connection]
	if !ok {
		return nil, fmt.Errorf("connection %q has no client wired in main.go", in.Connection)
	}

	// Enforce the connection's declared query timeout (default 30s) — the
	// config field is the contract the workflow declares; without this an
	// unindexed collection scan holds the turn until the agent's own budget
	// dies.
	timeoutMs := cc.QueryTimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 30000
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	// Refuse write stages ($out/$merge) and server-side JS ($where/$function/
	// $accumulator) BEFORE touching the driver — the read-only credential is a
	// backstop, not the only guard.
	if bad := scanForbidden(in.Filter); bad != "" {
		return nil, fmt.Errorf("mongo-query refuses operator %q (%s); this tool is read-only", bad, forbiddenOperators[bad])
	}
	if bad := scanForbidden(in.Pipeline); bad != "" {
		return nil, fmt.Errorf("mongo-query refuses operator %q (%s); this tool is read-only", bad, forbiddenOperators[bad])
	}

	// Convert any EJSON Date markers (`$date`, `$dateMillis`) in the filter /
	// pipeline into real time.Time values so the Go driver issues a BSON Date
	// comparison instead of comparing the field to a document literal.
	//
	// resolveEJSONDates returns a non-map when the ROOT is itself a bare date
	// marker (e.g. an LLM sends `filter: {"$date": ...}`). That is malformed —
	// a filter/stage must be a document — so reject it instead of asserting the
	// type (which used to panic and crash the Invoke goroutine).
	if in.Filter != nil {
		resolved, ok := resolveEJSONDates(in.Filter).(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mongo-query: filter must be an object, not a bare date/value")
		}
		in.Filter = resolved
	}
	for i := range in.Pipeline {
		resolved, ok := resolveEJSONDates(in.Pipeline[i]).(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mongo-query: pipeline stage %d must be an object, not a bare date/value", i)
		}
		in.Pipeline[i] = resolved
	}

	// Default a nil filter to {} so find/count get a real (match-all) document
	// rather than the driver's "filter is nil" error.
	if in.Filter == nil && in.Op != "aggregate" {
		in.Filter = map[string]any{}
	}

	limit := effectiveLimit(in.Limit, cc.MaxDocsDefault)

	switch in.Op {
	case "find":
		// Ask for one extra doc so we can DETECT (not just apply) truncation.
		docs, err := api.Find(ctx, in.Database, in.Collection, in.Filter, FindOpts{
			Projection: in.Projection, Sort: in.Sort, Limit: limit + 1,
		})
		if err != nil {
			return nil, classifyMongoError("find", err)
		}
		docs, truncated := capDocs(docs, limit)
		return json.Marshal(queryOutput{Op: "find", Docs: docs, Truncated: truncated})
	case "count":
		n, err := api.Count(ctx, in.Database, in.Collection, in.Filter)
		if err != nil {
			return nil, classifyMongoError("count", err)
		}
		return json.Marshal(queryOutput{Op: "count", Count: &n})
	case "aggregate":
		// Bound server-side work AND detect truncation: append $limit(limit+1).
		pipeline := append(clonePipeline(in.Pipeline), map[string]any{"$limit": int64(limit + 1)})
		docs, err := api.Aggregate(ctx, in.Database, in.Collection, pipeline)
		if err != nil {
			return nil, classifyMongoError("aggregate", err)
		}
		docs, truncated := capDocs(docs, limit)
		return json.Marshal(queryOutput{Op: "aggregate", Docs: docs, Truncated: truncated})
	}
	return nil, fmt.Errorf("unreachable")
}

// classifyMongoError wraps a driver error as a node.ToolError when it's a
// timeout/deadline, so the engine's retry helper sees a typed "timeout" class
// instead of parsing the message. Non-timeouts keep their op-prefixed wrap.
func classifyMongoError(op string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		mongo.IsTimeout(err) {
		return &node.ToolError{Code: "timeout", Retryable: false,
			Cause: fmt.Errorf("%s timed out (queryTimeoutMs) — narrow the filter or reduce the scan: %w", op, err)}
	}
	return fmt.Errorf("%s: %w", op, err)
}

// effectiveLimit resolves the per-call doc cap: an explicit in-range limit
// wins; otherwise the connection default (or 100).
func effectiveLimit(reqLimit, connDefault int) int {
	def := connDefault
	if def <= 0 {
		def = 100
	}
	if reqLimit > 0 && reqLimit <= def {
		return reqLimit
	}
	return def
}

// capDocs trims docs to limit, returning (trimmed, wasTruncated). A nil slice
// becomes []. Callers pass a slice fetched with limit+1 so an over-cap result
// is observable.
func capDocs(docs []map[string]any, limit int) ([]map[string]any, bool) {
	if docs == nil {
		return []map[string]any{}, false
	}
	if len(docs) > limit {
		return docs[:limit], true
	}
	return docs, false
}

// clonePipeline shallow-copies the stage slice so appending $limit never
// mutates the caller's input.
func clonePipeline(p []map[string]any) []map[string]any {
	out := make([]map[string]any, len(p))
	copy(out, p)
	return out
}

func containsString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// Compile-time check that Tool satisfies the node.Tool interface.
var _ node.Tool = (*Tool)(nil)
