package mongoquery

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeMongo records calls. Implements MongoAPI for tests.
type fakeMongo struct {
	findResult      []map[string]any
	countResult     int64
	aggregateResult []map[string]any
	lastOp          string
	lastFilter      map[string]any
	lastPipeline    []map[string]any
}

func (f *fakeMongo) Find(_ context.Context, _, _ string, filter map[string]any, _ FindOpts) ([]map[string]any, error) {
	f.lastOp = "find"
	f.lastFilter = filter
	return f.findResult, nil
}
func (f *fakeMongo) Count(_ context.Context, _, _ string, filter map[string]any) (int64, error) {
	f.lastOp = "count"
	f.lastFilter = filter
	return f.countResult, nil
}
func (f *fakeMongo) Aggregate(_ context.Context, _, _ string, pipeline []map[string]any) ([]map[string]any, error) {
	f.lastOp = "aggregate"
	f.lastPipeline = pipeline
	return f.aggregateResult, nil
}

func TestMongoQuery_Find(t *testing.T) {
	fake := &fakeMongo{findResult: []map[string]any{{"_id": "x"}}}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{
		"prod": {AllowedDatabases: []string{"batches"}, MaxDocsDefault: 100},
	}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})

	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "batches", "collection": "claims",
		"op": "find", "filter": map[string]any{"status": "open"},
	})
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	docs := resp["docs"].([]any)
	if len(docs) != 1 {
		t.Fatalf("got %d docs", len(docs))
	}
	if fake.lastOp != "find" {
		t.Errorf("lastOp = %q", fake.lastOp)
	}
}

func TestMongoQuery_Count(t *testing.T) {
	fake := &fakeMongo{countResult: 42}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{"prod": {AllowedDatabases: []string{"x"}}}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})

	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "x", "collection": "y", "op": "count",
		"filter": map[string]any{"a": 1},
	})
	out, err := tool.Invoke(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]any
	_ = json.Unmarshal(out, &resp)
	if resp["count"].(float64) != 42 {
		t.Errorf("count = %v", resp["count"])
	}
}

func TestMongoQuery_Aggregate(t *testing.T) {
	fake := &fakeMongo{aggregateResult: []map[string]any{{"sum": 100}}}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{"prod": {AllowedDatabases: []string{"x"}}}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})

	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "x", "collection": "y", "op": "aggregate",
		"pipeline": []map[string]any{{"$match": map[string]any{"status": "open"}}},
	})
	if _, err := tool.Invoke(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if fake.lastOp != "aggregate" {
		t.Errorf("lastOp = %q", fake.lastOp)
	}
}

// Empty result sets must render as "docs": [] (or "count": 0), NOT silently
// drop the field via omitempty — otherwise the LLM mistakes "no matches" for
// "connection failed" and tells the user the system is down.
func TestMongoQuery_EmptyResultsRenderExplicitly(t *testing.T) {
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{"prod": {AllowedDatabases: []string{"x"}, MaxDocsDefault: 100}}}

	// aggregate with no matches → docs: []
	t.Run("aggregate empty", func(t *testing.T) {
		fake := &fakeMongo{aggregateResult: nil}
		tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
		args, _ := json.Marshal(map[string]any{
			"connection": "prod", "database": "x", "collection": "y", "op": "aggregate",
			"pipeline": []map[string]any{{"$match": map[string]any{"id": "none"}}},
		})
		raw, err := tool.Invoke(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		got := string(raw)
		if !strings.Contains(got, `"docs":[]`) {
			t.Errorf("aggregate empty result must include explicit docs:[]; got %s", got)
		}
	})

	// find with no matches → docs: []
	t.Run("find empty", func(t *testing.T) {
		fake := &fakeMongo{findResult: nil}
		tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
		args, _ := json.Marshal(map[string]any{
			"connection": "prod", "database": "x", "collection": "y", "op": "find",
			"filter": map[string]any{"id": "none"},
		})
		raw, err := tool.Invoke(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		got := string(raw)
		if !strings.Contains(got, `"docs":[]`) {
			t.Errorf("find empty result must include explicit docs:[]; got %s", got)
		}
	})

	// count of zero → explicit "count": 0
	t.Run("count zero", func(t *testing.T) {
		fake := &fakeMongo{countResult: 0}
		tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
		args, _ := json.Marshal(map[string]any{
			"connection": "prod", "database": "x", "collection": "y", "op": "count",
			"filter": map[string]any{"id": "none"},
		})
		raw, err := tool.Invoke(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		got := string(raw)
		if !strings.Contains(got, `"count":0`) {
			t.Errorf("count=0 must render explicitly; got %s", got)
		}
	})
}

func TestMongoQuery_DisallowedOp(t *testing.T) {
	tool := newToolForTest(Config{ToolDescription: "d", Connections: map[string]ConnConfig{"prod": {AllowedDatabases: []string{"x"}}}}, nil)
	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "x", "collection": "y", "op": "update",
	})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "update") {
		t.Fatalf("expected op-not-allowed, got %v", err)
	}
}

func TestMongoQuery_DisallowedDatabase(t *testing.T) {
	fake := &fakeMongo{}
	tool := newToolForTest(Config{ToolDescription: "d", Connections: map[string]ConnConfig{"prod": {AllowedDatabases: []string{"batches"}}}}, map[string]MongoAPI{"prod": fake})
	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "secret", "collection": "y", "op": "find",
	})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "secret") {
		t.Fatalf("expected database-not-allowed, got %v", err)
	}
}

func TestMongoQuery_UnknownConnection(t *testing.T) {
	tool := newToolForTest(Config{ToolDescription: "d", Connections: map[string]ConnConfig{"prod": {AllowedDatabases: []string{"x"}}}}, nil)
	args, _ := json.Marshal(map[string]any{"connection": "nope", "database": "x", "collection": "y", "op": "find"})
	_, err := tool.Invoke(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("expected unknown-connection, got %v", err)
	}
}

// EJSON date markers ($date and $dateMillis) must be unwrapped into real
// time.Time values before being handed to the driver — otherwise Mongo sees a
// document literal under $gte and matches nothing. This was the root cause
// of the "no batches found today" behaviour in production.
func TestMongoQuery_EJSONDateResolution(t *testing.T) {
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{"prod": {AllowedDatabases: []string{"x"}, MaxDocsDefault: 100}}}

	t.Run("$date string in filter is converted to time.Time", func(t *testing.T) {
		fake := &fakeMongo{}
		tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
		args, _ := json.Marshal(map[string]any{
			"connection": "prod", "database": "x", "collection": "y", "op": "find",
			"filter": map[string]any{
				"dateAdded": map[string]any{
					"$gte": map[string]any{"$date": "2026-05-19T00:00:00Z"},
				},
			},
		})
		_, err := tool.Invoke(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		gteVal := fake.lastFilter["dateAdded"].(map[string]any)["$gte"]
		got, ok := gteVal.(time.Time)
		if !ok {
			t.Fatalf("$gte should be time.Time after resolution, got %T = %v", gteVal, gteVal)
		}
		want := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("$dateMillis number in filter is converted to time.Time", func(t *testing.T) {
		fake := &fakeMongo{}
		tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
		args, _ := json.Marshal(map[string]any{
			"connection": "prod", "database": "x", "collection": "y", "op": "find",
			"filter": map[string]any{
				"dateAdded": map[string]any{
					"$lt": map[string]any{"$dateMillis": 1779148800000}, // 2026-05-19 00:00 UTC
				},
			},
		})
		_, err := tool.Invoke(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		ltVal := fake.lastFilter["dateAdded"].(map[string]any)["$lt"]
		got, ok := ltVal.(time.Time)
		if !ok {
			t.Fatalf("$lt should be time.Time after resolution, got %T = %v", ltVal, ltVal)
		}
		want := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("$date in aggregate pipeline is converted", func(t *testing.T) {
		fake := &fakeMongo{}
		tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
		args, _ := json.Marshal(map[string]any{
			"connection": "prod", "database": "x", "collection": "y", "op": "aggregate",
			"pipeline": []map[string]any{
				{"$match": map[string]any{
					"dateAdded": map[string]any{
						"$gte": map[string]any{"$date": "2026-05-19T04:00:00Z"},
					},
				}},
			},
		})
		_, err := tool.Invoke(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		matchStage := fake.lastPipeline[0]["$match"].(map[string]any)
		gteVal := matchStage["dateAdded"].(map[string]any)["$gte"]
		got, ok := gteVal.(time.Time)
		if !ok {
			t.Fatalf("pipeline $gte should be time.Time, got %T = %v", gteVal, gteVal)
		}
		want := time.Date(2026, 5, 19, 4, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("non-date $-markers pass through untouched", func(t *testing.T) {
		fake := &fakeMongo{}
		tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
		args, _ := json.Marshal(map[string]any{
			"connection": "prod", "database": "x", "collection": "y", "op": "find",
			"filter": map[string]any{
				"status": map[string]any{"$in": []any{"PROCESSED", "FILE_IMPORTED"}},
			},
		})
		_, err := tool.Invoke(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		inVal := fake.lastFilter["status"].(map[string]any)["$in"]
		arr, ok := inVal.([]any)
		if !ok || len(arr) != 2 {
			t.Fatalf("$in should be untouched 2-element slice, got %T = %v", inVal, inVal)
		}
	})

	t.Run("plain numeric millis (no marker) is left as is", func(t *testing.T) {
		// We do NOT auto-convert raw numbers — only the $date / $dateMillis
		// markers. This keeps the door open for legitimate numeric-typed
		// comparisons on other fields. The agent is instructed to use the
		// marker form in the prompt.
		fake := &fakeMongo{}
		tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
		args, _ := json.Marshal(map[string]any{
			"connection": "prod", "database": "x", "collection": "y", "op": "find",
			"filter": map[string]any{
				"size": map[string]any{"$gte": float64(1000)},
			},
		})
		_, err := tool.Invoke(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		gteVal := fake.lastFilter["size"].(map[string]any)["$gte"]
		if _, isTime := gteVal.(time.Time); isTime {
			t.Fatalf("plain number should NOT be coerced to time.Time")
		}
	})
}

func TestFactoryRequiresToolDescription(t *testing.T) {
	_, err := Factory.New(json.RawMessage(`{"connections":{"prod":{"uriEnv":"MONGO_URI","allowedDatabases":["repository"]}}}`))
	if err == nil {
		t.Fatal("expected error when toolDescription missing")
	}
}

func TestToolSpecUsesConfiguredName(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"toolName":"mongo_query","toolDescription":"d","connections":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := n.(node.Tool).ToolSpec().Name; got != "mongo_query" {
		t.Fatalf("ToolSpec().Name = %q", got)
	}
}

func TestToolSpecDefaultsName(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"toolDescription":"d","connections":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := n.(node.Tool).ToolSpec().Name; got != "mongo_query" {
		t.Fatalf("ToolSpec().Name = %q, want default mongo_query", got)
	}
}

// Helper to bypass Init and inject clients directly.
func newToolForTest(cfg Config, clients map[string]MongoAPI) *Tool {
	return &Tool{cfg: cfg, clients: clients}
}

func TestURIDefaultDB(t *testing.T) {
	for uri, want := range map[string]string{
		"mongodb+srv://u:p@prod-pl-0.example.mongodb.net/devrepository?retryWrites=true": "devrepository",
		"mongodb://u:p@host:27017/repository":                                            "repository",
		"mongodb+srv://u:p@host.example.net/?retryWrites=true":                           "",
		"mongodb://u:p@host:27017":                                                       "",
	} {
		if got := uriDefaultDB(uri); got != want {
			t.Errorf("uriDefaultDB(%q) = %q, want %q", uri, got, want)
		}
	}
}

func TestMongoQuery_OmittedDatabaseUsesURIDefault(t *testing.T) {
	fake := &fakeMongo{countResult: 7}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{
		"prod": {}, // no allowlist: URI default only
	}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
	tool.defaultDBs = map[string]string{"prod": "devrepository"}

	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "collection": "batches", "op": "count",
	})
	if _, err := tool.Invoke(context.Background(), args); err != nil {
		t.Fatalf("omitted database should fall back to URI default, got %v", err)
	}
}

func TestMongoQuery_EmptyAllowlistRejectsOtherDatabases(t *testing.T) {
	fake := &fakeMongo{}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{
		"prod": {},
	}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
	tool.defaultDBs = map[string]string{"prod": "devrepository"}

	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches", "op": "count",
	})
	if _, err := tool.Invoke(context.Background(), args); err == nil {
		t.Fatal("database outside the URI default should be rejected when no allowlist is set")
	}
}

func TestMongoQuery_NoDefaultAndNoDatabaseErrors(t *testing.T) {
	fake := &fakeMongo{}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{
		"prod": {},
	}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})

	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "collection": "batches", "op": "count",
	})
	if _, err := tool.Invoke(context.Background(), args); err == nil {
		t.Fatal("expected error when database omitted and URI names no default")
	}
}

func TestMongoQuery_AllowlistStillHonored(t *testing.T) {
	fake := &fakeMongo{countResult: 1}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{
		"prod": {AllowedDatabases: []string{"repository", "other"}},
	}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})
	tool.defaultDBs = map[string]string{"prod": "repository"}

	ok, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "other", "collection": "c", "op": "count",
	})
	if _, err := tool.Invoke(context.Background(), ok); err != nil {
		t.Fatalf("allowlisted database rejected: %v", err)
	}
	bad, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "forbidden", "collection": "c", "op": "count",
	})
	if _, err := tool.Invoke(context.Background(), bad); err == nil {
		t.Fatal("non-allowlisted database should be rejected")
	}
}
