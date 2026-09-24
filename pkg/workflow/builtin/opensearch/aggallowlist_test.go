package opensearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
)

func TestValidateAggregationsAllowsInScopeTypes(t *testing.T) {
	// Shapes taken from real Production queries (terms > terms > cardinality/filter,
	// date_histogram, pipelines, nested, composite).
	ok := []string{
		``, `null`, `{}`,
		`{"by_origin":{"terms":{"field":"origin.keyword","size":20}}}`,
		`{"origin":{"terms":{"field":"origin.keyword"},"aggs":{"ra":{"terms":{"field":"routingAddress.keyword"},"aggs":{"npis":{"cardinality":{"field":"npi.keyword"}},"rejects":{"filter":{"bool":{"must_not":[{"term":{"rejectCodes.keyword":""}}]}}},"errors":{"filter":{"term":{"error":true}}}}}}}}`,
		`{"hourly":{"date_histogram":{"field":"created","calendar_interval":"hour"},"aggregations":{"avg_ms":{"avg":{"field":"ms"}}}}}`,
		`{"by_day":{"date_histogram":{"field":"created","calendar_interval":"day"},"aggs":{"rej":{"filter":{"term":{"error":true}}},"rate":{"bucket_script":{"buckets_path":{"r":"rej._count","t":"_count"},"script":"params.r/params.t"}}}},"max_day":{"max_bucket":{"buckets_path":"by_day._count"}}}`,
		`{"n":{"nested":{"path":"items"},"aggs":{"c":{"value_count":{"field":"items.id"}}}}}`,
		`{"c":{"composite":{"sources":[{"b":{"terms":{"field":"bin.keyword"}}}]},"meta":{"note":"x"}}}`,
		`{"p":{"percentiles":{"field":"ms"}},"s":{"stats":{"field":"ms"}},"top":{"top_hits":{"size":3}}}`,
	}
	for _, raw := range ok {
		if err := validateAggregations(json.RawMessage(raw)); err != nil {
			t.Fatalf("expected allowed, got %v for %s", err, raw)
		}
	}
}

func TestValidateAggregationsRejectsScopeEscapes(t *testing.T) {
	bad := map[string]string{
		"global top level":       `{"all":{"global":{}}}`,
		"global with sub-aggs":   `{"all":{"global":{},"aggs":{"o":{"terms":{"field":"origin.keyword"}}}}}`,
		"global nested deep":     `{"a":{"terms":{"field":"x"},"aggs":{"b":{"global":{}}}}}`,
		"significant_terms":      `{"sig":{"significant_terms":{"field":"origin.keyword"}}}`,
		"significant_text":       `{"sig":{"significant_text":{"field":"msg"}}}`,
		"sampler":                `{"s":{"sampler":{"shard_size":100}}}`,
		"diversified_sampler":    `{"s":{"diversified_sampler":{"field":"x"}}}`,
		"scripted_metric":        `{"m":{"scripted_metric":{"map_script":"state.x=1"}}}`,
		"unknown type":           `{"m":{"made_up_agg":{}}}`,
		"two types in one agg":   `{"m":{"terms":{"field":"x"},"avg":{"field":"y"}}}`,
		"no type":                `{"m":{"aggs":{"c":{"avg":{"field":"y"}}}}}`,
		"not an object":          `["terms"]`,
		"agg body not an object": `{"m":"terms"}`,
		"sub-aggs not an object": `{"m":{"terms":{"field":"x"},"aggs":[1]}}`,
	}
	for name, raw := range bad {
		if err := validateAggregations(json.RawMessage(raw)); err == nil {
			t.Fatalf("%s: expected rejection for %s", name, raw)
		}
	}
	err := validateAggregations(json.RawMessage(`{"all":{"global":{}}}`))
	if !strings.Contains(err.Error(), `"global"`) || !strings.Contains(err.Error(), "security") {
		t.Fatalf("error must name the type and explain the rule, got %q", err)
	}
}

func TestValidateAggregationsDepthLimit(t *testing.T) {
	raw := `{"a0":{"terms":{"field":"x"}}}`
	for i := 1; i <= maxAggDepth+1; i++ {
		raw = `{"a` + string(rune('a'+i%26)) + `":{"terms":{"field":"x"},"aggs":` + raw + `}}`
	}
	if err := validateAggregations(json.RawMessage(raw)); err == nil {
		t.Fatal("expected depth limit error")
	}
}

// TestInvokeRejectsGlobalAggregationBeforeCallingOpenSearch proves the request
// never reaches OpenSearch when the model asks for a scope-escaping aggregation.
func TestInvokeRejectsGlobalAggregationBeforeCallingOpenSearch(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	re, _ := compileIndexPattern("prod.cpe-*")
	tool := &opensearchTool{
		cfg:           Config{Host: srv.URL, RequestTimeoutSeconds: 5, MaxResultSize: 10, AllowedIndexPattern: "prod.cpe-*"},
		http:          srv.Client(),
		policy:        stubPolicy{},
		user:          secret.New("u"),
		pass:          secret.New("p"),
		allowedRegexp: re,
	}
	ctx := identity.WithIdentity(context.Background(), identity.Identity{AccountID: "AM-1"})
	_, err := tool.Invoke(ctx, json.RawMessage(`{"indexPath":"prod.cpe-2026-04-30","queryBody":{"filter":[]},"size":0,"aggregations":{"all":{"global":{}}}}`))
	if err == nil || !strings.Contains(err.Error(), "global") {
		t.Fatalf("expected global rejection, got %v", err)
	}
	if called {
		t.Fatal("OpenSearch must not be called for a rejected aggregation")
	}
}

func TestNormalizeAggregationsDecodesStringifiedObject(t *testing.T) {
	out, err := normalizeAggregations(json.RawMessage(`"{\"by_bin\":{\"terms\":{\"field\":\"bin.keyword\"}}}"`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(out), `{"by_bin"`) {
		t.Fatalf("expected decoded object, got %s", out)
	}
}

// TestNormalizeAggregationsSendsOnlyValidatedTree: with duplicate names the
// decoded (validated) tree is what gets serialized, never the raw bytes.
func TestNormalizeAggregationsSendsOnlyValidatedTree(t *testing.T) {
	if _, err := normalizeAggregations(json.RawMessage(`{"a":{"terms":{"field":"x"}},"a":{"global":{}}}`)); err == nil {
		t.Fatal("duplicate name whose surviving value is global must be rejected")
	}
	out, err := normalizeAggregations(json.RawMessage(`{"a":{"global":{}},"a":{"terms":{"field":"x"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "global") {
		t.Fatalf("serialized aggs must be the validated tree, got %s", out)
	}
	if _, err := normalizeAggregations(json.RawMessage(`{"a":{"terms":{}}} {"b":{"global":{}}}`)); err == nil {
		t.Fatal("trailing JSON must be rejected")
	}
}
