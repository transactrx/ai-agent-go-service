package opensearch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const testScope = `{"bool":{"minimum_should_match":1,"should":[{"term":{"origin.keyword":"Axys-Dev"}}]}}`

type scopePolicy struct{}

func (scopePolicy) Spec() node.NodeSpec                          { return node.NodeSpec{Role: node.RolePolicy} }
func (scopePolicy) Init(_ context.Context, _ node.NodeEnv) error { return nil }
func (scopePolicy) Close(_ context.Context) error                { return nil }
func (scopePolicy) Resolve(_ context.Context, _ node.PolicyRequest) (node.PolicyResult, error) {
	return node.PolicyResult{InjectMustClauses: []json.RawMessage{json.RawMessage(testScope)}}, nil
}

// invokeCapture runs the tool against a fake OpenSearch and returns the body
// it received ("" when the tool refused before calling it).
func invokeCapture(t *testing.T, args string) (string, error) {
	t.Helper()
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen = string(b)
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":0}}}`))
	}))
	defer srv.Close()
	re, err := compileIndexPattern("dev.cpe-*")
	if err != nil {
		t.Fatal(err)
	}
	tool := &opensearchTool{
		cfg:           Config{Host: srv.URL, MaxResultSize: 500, AllowedIndexPattern: "dev.cpe-*"},
		http:          srv.Client(),
		policy:        scopePolicy{},
		user:          secret.New("u"),
		pass:          secret.New("p"),
		allowedRegexp: re,
	}
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	_, err = tool.Invoke(ctx, json.RawMessage(args))
	return seen, err
}

func TestInvokeRejectsDocumentReads(t *testing.T) {
	cases := map[string]string{
		"terms lookup in filter":        `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"filter":[{"terms":{"bin.keyword":{"index":"dev.cpe-2026-09-24","id":"1","path":"bin"}}}]}}`,
		"terms lookup nested in must":   `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"must":[{"bool":{"should":[{"terms":{"npi.keyword":{"id":"1","path":"npi"}}}]}}]}}`,
		"terms lookup in must_not":      `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"must_not":[{"terms":{"origin.keyword":{"index":"x","id":"1","path":"origin"}}}]}}`,
		"more_like_this document":       `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"must":[{"more_like_this":{"fields":["origin"],"like":[{"_index":"dev.cpe-2026-09-24","_id":"1"}]}}]}}`,
		"more_like_this unlike doc":     `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"must":[{"more_like_this":{"fields":["origin"],"like":"x","unlike":{"_id":"1"}}}]}}`,
		"terms lookup in filter agg":    `{"indexPath":"dev.cpe-2026-09-24","queryBody":{},"aggregations":{"f":{"filter":{"terms":{"bin.keyword":{"index":"x","id":"1","path":"bin"}}}}}}`,
		"more_like_this in filters agg": `{"indexPath":"dev.cpe-2026-09-24","queryBody":{},"aggregations":{"f":{"filters":{"filters":{"a":{"more_like_this":{"fields":["origin"],"like":[{"_id":"1"}]}}}}}}}`,
		"terms lookup in nested sort":   `{"indexPath":"dev.cpe-2026-09-24","queryBody":{},"sort":[{"x.y":{"order":"asc","nested":{"path":"x","filter":{"terms":{"x.z":{"index":"i","id":"1","path":"z"}}}}}}]}`,
		"stringified aggs with lookup":  `{"indexPath":"dev.cpe-2026-09-24","queryBody":{},"aggregations":"{\"f\":{\"filter\":{\"terms\":{\"bin.keyword\":{\"index\":\"x\",\"id\":\"1\",\"path\":\"bin\"}}}}}"}`,
	}
	for name, args := range cases {
		seen, err := invokeCapture(t, args)
		if err == nil || !strings.Contains(err.Error(), "outside the security scope") {
			t.Errorf("%s: want document-read refusal, got err=%v", name, err)
		}
		if seen != "" {
			t.Errorf("%s: OpenSearch was called: %s", name, seen)
		}
	}
}

func TestInvokeAllowsValueListsAndText(t *testing.T) {
	cases := map[string]string{
		"terms list":            `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"filter":[{"terms":{"bin.keyword":["880156","880151"]}}]}}`,
		"terms list with boost": `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"filter":[{"terms":{"bin.keyword":["880156"],"boost":2}}]}}`,
		"more_like_this text":   `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"must":[{"more_like_this":{"fields":["origin"],"like":"Axys Dev"}}]}}`,
		"terms agg with order":  `{"indexPath":"dev.cpe-2026-09-24","queryBody":{},"aggregations":{"by_bin":{"terms":{"field":"bin.keyword","size":10,"order":{"_count":"desc"}}}}}`,
		"terms agg partition":   `{"indexPath":"dev.cpe-2026-09-24","queryBody":{},"aggregations":{"by_bin":{"terms":{"field":"bin.keyword","include":{"partition":0,"num_partitions":4}}}}}`,
		"filter agg term":       `{"indexPath":"dev.cpe-2026-09-24","queryBody":{},"aggregations":{"f":{"filter":{"term":{"bin.keyword":"880156"}}}}}`,
		"plain sort":            `{"indexPath":"dev.cpe-2026-09-24","queryBody":{},"sort":[{"created":{"order":"desc"}}]}`,
	}
	for name, args := range cases {
		seen, err := invokeCapture(t, args)
		if err != nil {
			t.Errorf("%s: refused a normal query: %v", name, err)
			continue
		}
		if !strings.Contains(seen, "Axys-Dev") {
			t.Errorf("%s: scope missing from request: %s", name, seen)
		}
	}
}

// A single clause object in must/must_not/filter is kept (wrapped in a list),
// never silently dropped; the scope stays in must.
func TestInvokeKeepsSingleObjectClause(t *testing.T) {
	for _, key := range []string{"must", "must_not", "filter"} {
		args := `{"indexPath":"dev.cpe-2026-09-24","queryBody":{"` + key + `":{"term":{"bin.keyword":"880156"}}}}`
		seen, err := invokeCapture(t, args)
		if err != nil {
			t.Fatalf("%s object refused: %v", key, err)
		}
		var body struct {
			Query struct {
				Bool map[string][]json.RawMessage `json:"bool"`
			} `json:"query"`
		}
		if err := json.Unmarshal([]byte(seen), &body); err != nil {
			t.Fatalf("%s: request is not a bool of lists: %v\n%s", key, err, seen)
		}
		if got := body.Query.Bool[key]; len(got) == 0 || !strings.Contains(string(got[0]), "880156") {
			t.Errorf("%s: clause dropped: %s", key, seen)
		}
		if !strings.Contains(string(mustJoin(body.Query.Bool["must"])), "Axys-Dev") {
			t.Errorf("%s: scope missing: %s", key, seen)
		}
	}
}

func TestInvokeRejectsBadClauseShape(t *testing.T) {
	for _, qb := range []string{`{"must":"x"}`, `{"filter":[1]}`, `{"must_not":[["a"]]}`, `{"must":true}`} {
		seen, err := invokeCapture(t, `{"indexPath":"dev.cpe-2026-09-24","queryBody":`+qb+`}`)
		if err == nil || !strings.Contains(err.Error(), "list of query clauses") {
			t.Errorf("%s: want shape error, got %v", qb, err)
		}
		if seen != "" {
			t.Errorf("%s: OpenSearch was called", qb)
		}
	}
}

func mustJoin(list []json.RawMessage) []byte {
	b, _ := json.Marshal(list)
	return b
}
