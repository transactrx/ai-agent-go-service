package opensearch

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Aggregation allowlist.
//
// The account security constraints are injected into the query's bool.must.
// Every aggregation type below is computed only over the documents that query
// matched, so it is scoped exactly like the query. A few OpenSearch types are
// NOT bound by the query — "global" computes over the whole index, and
// "significant_terms"/"significant_text" compare against an index-wide
// background set — and would leak other accounts' data. Only the listed types
// are accepted; anything else (including unknown or future types) is rejected
// before OpenSearch is called. Fail closed: a new type must be added here
// deliberately after checking it cannot escape the query.
var allowedAggTypes = map[string]bool{
	// bucket
	"terms": true, "multi_terms": true, "rare_terms": true,
	"date_histogram": true, "auto_date_histogram": true, "histogram": true,
	"range": true, "date_range": true,
	"filter": true, "filters": true, "missing": true,
	"composite": true, "nested": true,
	// metric
	"avg": true, "sum": true, "min": true, "max": true,
	"stats": true, "extended_stats": true, "value_count": true, "cardinality": true,
	"percentiles": true, "percentile_ranks": true, "median_absolute_deviation": true,
	"weighted_avg": true, "top_hits": true,
	// pipeline (operate on the buckets above)
	"bucket_script": true, "bucket_selector": true, "bucket_sort": true,
	"avg_bucket": true, "sum_bucket": true, "min_bucket": true, "max_bucket": true,
	"stats_bucket": true, "extended_stats_bucket": true, "percentiles_bucket": true,
	"cumulative_sum": true, "derivative": true, "moving_fn": true, "serial_diff": true,
}

// maxAggDepth bounds nesting so a pathological tree cannot exhaust the walker.
const maxAggDepth = 10

// validateAggregations walks the model-supplied aggregation tree and rejects
// any aggregation type that is not on the allowlist. nil/empty/null is valid.
func validateAggregations(raw json.RawMessage) error {
	_, err := normalizeAggregations(raw)
	return err
}

// normalizeAggregations returns the aggregation object to send to OpenSearch,
// after validating it. Models sometimes pass the object as a JSON string
// ("{\"by_bin\":{...}}"); that string is decoded first. The result is
// re-serialized from the decoded tree, so OpenSearch receives exactly what was
// validated (duplicate keys or other raw-byte tricks cannot differ between the
// validator and OpenSearch). nil means "no aggregations".
func normalizeAggregations(raw json.RawMessage) (json.RawMessage, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return nil, nil
	}
	if strings.HasPrefix(s, `"`) {
		var inner string
		if err := json.Unmarshal(raw, &inner); err != nil {
			return nil, errAggsNotObject
		}
		s = strings.TrimSpace(inner)
		if s == "" || s == "null" {
			return nil, nil
		}
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var tree any
	if err := dec.Decode(&tree); err != nil || dec.More() {
		return nil, errAggsNotObject
	}
	aggs, ok := tree.(map[string]any)
	if !ok {
		return nil, errAggsNotObject
	}
	if len(aggs) == 0 {
		return nil, nil
	}
	if err := walkAggs(aggs, 1); err != nil {
		return nil, err
	}
	out, err := json.Marshal(aggs)
	if err != nil {
		return nil, errAggsNotObject
	}
	return out, nil
}

var errAggsNotObject = fmt.Errorf("tool/opensearch: aggregations must be a JSON object of named aggregations")

func walkAggs(aggs map[string]any, depth int) error {
	if depth > maxAggDepth {
		return fmt.Errorf("tool/opensearch: aggregations nested deeper than %d levels", maxAggDepth)
	}
	names := make([]string, 0, len(aggs))
	for n := range aggs {
		names = append(names, n)
	}
	sort.Strings(names) // deterministic error messages
	for _, name := range names {
		body, ok := aggs[name].(map[string]any)
		if !ok {
			return fmt.Errorf("tool/opensearch: aggregation %q must be a JSON object", name)
		}
		types := 0
		for key, val := range body {
			switch key {
			case "aggs", "aggregations":
				sub, ok := val.(map[string]any)
				if !ok {
					return fmt.Errorf("tool/opensearch: sub-aggregations of %q must be a JSON object", name)
				}
				if err := walkAggs(sub, depth+1); err != nil {
					return err
				}
			case "meta":
				// free-form metadata, not an aggregation
			default:
				if !allowedAggTypes[key] {
					return fmt.Errorf("tool/opensearch: aggregation type %q (in %q) is not allowed: "+
						"aggregations must stay inside the security-scoped query, so types that "+
						"compute over the whole index (global, significant_terms, significant_text, "+
						"samplers, scripted_metric) are rejected. Do not try to remove or work around "+
						"the security scope; use terms, filter(s), date_histogram, metrics, or pipelines instead", key, name)
				}
				types++
			}
		}
		if types != 1 {
			return fmt.Errorf("tool/opensearch: aggregation %q must have exactly one aggregation type (found %d)", name, types)
		}
	}
	return nil
}
