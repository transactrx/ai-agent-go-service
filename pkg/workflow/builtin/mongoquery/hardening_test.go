package mongoquery

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func hardTool(fake MongoAPI, maxDocs int) *Tool {
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{
		"prod": {AllowedDatabases: []string{"repository"}, MaxDocsDefault: maxDocs},
	}}
	return newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
}

func invokeJSON(t *testing.T, tool *Tool, req map[string]any) (map[string]any, error) {
	t.Helper()
	args, _ := json.Marshal(req)
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		return nil, err
	}
	var resp map[string]any
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("bad output json: %v", err)
	}
	return resp, nil
}

// --- write-stage / server-side-JS blocklist -------------------------------

func TestMongoQuery_Aggregate_OutStage_Refused(t *testing.T) {
	fake := &fakeMongo{}
	tool := hardTool(fake, 100)
	_, err := invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "aggregate", "pipeline": []map[string]any{
			{"$match": map[string]any{"a": 1}},
			{"$out": "stolen"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "$out") {
		t.Fatalf("expected $out refusal, got %v", err)
	}
	if fake.lastOp == "aggregate" {
		t.Fatal("aggregate reached the driver despite $out stage")
	}
}

func TestMongoQuery_Aggregate_MergeStage_Refused(t *testing.T) {
	tool := hardTool(&fakeMongo{}, 100)
	_, err := invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "aggregate", "pipeline": []map[string]any{
			{"$merge": map[string]any{"into": "x"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "$merge") {
		t.Fatalf("expected $merge refusal, got %v", err)
	}
}

func TestMongoQuery_Where_Refused_InFilterAndPipeline(t *testing.T) {
	tool := hardTool(&fakeMongo{}, 100)
	// $where in a find filter
	_, err := invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "find", "filter": map[string]any{"$where": "this.a == 1"},
	})
	if err == nil || !strings.Contains(err.Error(), "$where") {
		t.Fatalf("expected $where refusal in filter, got %v", err)
	}
	// $where nested inside a $match stage
	_, err = invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "aggregate", "pipeline": []map[string]any{
			{"$match": map[string]any{"$and": []any{map[string]any{"$where": "true"}}}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "$where") {
		t.Fatalf("expected $where refusal in pipeline, got %v", err)
	}
}

func TestMongoQuery_FunctionAndAccumulator_Refused(t *testing.T) {
	tool := hardTool(&fakeMongo{}, 100)
	_, err := invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "aggregate", "pipeline": []map[string]any{
			{"$project": map[string]any{"v": map[string]any{"$function": map[string]any{"body": "x", "args": []any{}, "lang": "js"}}}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "$function") {
		t.Fatalf("expected $function refusal, got %v", err)
	}
}

// --- aggregate result cap ---------------------------------------------------

func TestMongoQuery_Aggregate_CappedAndFlagged(t *testing.T) {
	many := make([]map[string]any, 7)
	for i := range many {
		many[i] = map[string]any{"i": i}
	}
	fake := &fakeMongo{aggregateResult: many}
	tool := hardTool(fake, 5)
	resp, err := invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "aggregate", "pipeline": []map[string]any{{"$match": map[string]any{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(resp["docs"].([]any)); n != 5 {
		t.Fatalf("docs = %d, want capped 5", n)
	}
	if resp["truncated"] != true {
		t.Fatalf("truncated flag missing: %v", resp)
	}
	// the driver call must carry a $limit stage bounding server-side work
	last := fake.lastPipeline[len(fake.lastPipeline)-1]
	if _, ok := last["$limit"]; !ok {
		t.Fatalf("no $limit appended to pipeline: %v", fake.lastPipeline)
	}
}

// --- find truncation flag ----------------------------------------------------

func TestMongoQuery_Find_TruncationFlagged(t *testing.T) {
	many := make([]map[string]any, 6)
	for i := range many {
		many[i] = map[string]any{"i": i}
	}
	fake := &fakeMongo{findResult: many}
	tool := hardTool(fake, 5)
	resp, err := invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches", "op": "find",
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(resp["docs"].([]any)); n != 5 {
		t.Fatalf("docs = %d, want capped 5", n)
	}
	if resp["truncated"] != true {
		t.Fatalf("truncated flag missing: %v", resp)
	}
}

// --- panic + nil-filter robustness -------------------------------------------

func TestMongoQuery_BareDateMarkerRoot_NoPanic(t *testing.T) {
	tool := hardTool(&fakeMongo{}, 100)
	// A filter that IS a single $date marker used to panic on the
	// map[string]any type assertion after resolveEJSONDates returned time.Time.
	_, err := invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "find", "filter": map[string]any{"$date": "2026-05-19T00:00:00Z"},
	})
	if err == nil {
		t.Fatal("expected a validation error for bare $date filter, got nil")
	}
	// Same for a pipeline stage that is a bare marker.
	_, err = invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "aggregate", "pipeline": []map[string]any{{"$date": "2026-05-19T00:00:00Z"}},
	})
	if err == nil {
		t.Fatal("expected a validation error for bare $date stage, got nil")
	}
}

func TestMongoQuery_NilFilter_DefaultsToEmpty(t *testing.T) {
	fake := &fakeMongo{findResult: []map[string]any{}}
	tool := hardTool(fake, 5)
	if _, err := invokeJSON(t, tool, map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches", "op": "find",
	}); err != nil {
		t.Fatal(err)
	}
	if fake.lastFilter == nil {
		t.Fatal("nil filter reached the driver — should default to {}")
	}
}
