package mongoquery

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// deadlineMongo captures the ctx deadline each call receives.
type deadlineMongo struct {
	fakeMongo
	deadline    time.Time
	hadDeadline bool
}

func (d *deadlineMongo) Find(ctx context.Context, db, coll string, filter map[string]any, opts FindOpts) ([]map[string]any, error) {
	d.deadline, d.hadDeadline = ctx.Deadline()
	return d.fakeMongo.Find(ctx, db, coll, filter, opts)
}

// TestMongoQuery_QueryTimeoutEnforced proves queryTimeoutMs actually bounds
// the driver call — the config field is a contract, not documentation.
func TestMongoQuery_QueryTimeoutEnforced(t *testing.T) {
	fake := &deadlineMongo{}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{
		"prod": {AllowedDatabases: []string{"repository"}, QueryTimeoutMs: 1500},
	}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})

	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches",
		"op": "find", "filter": map[string]any{},
	})
	if _, err := tool.Invoke(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if !fake.hadDeadline {
		t.Fatal("driver call had no ctx deadline — queryTimeoutMs not enforced")
	}
	if remaining := time.Until(fake.deadline); remaining > 1500*time.Millisecond || remaining <= 0 {
		t.Fatalf("deadline %v out of expected <=1.5s window", remaining)
	}
}

// TestMongoQuery_QueryTimeoutDefault proves the 30s default applies when the
// connection omits queryTimeoutMs.
func TestMongoQuery_QueryTimeoutDefault(t *testing.T) {
	fake := &deadlineMongo{}
	cfg := Config{ToolDescription: "d", Connections: map[string]ConnConfig{
		"prod": {AllowedDatabases: []string{"repository"}},
	}}
	tool := newToolForTest(cfg, map[string]MongoAPI{"prod": fake})

	args, _ := json.Marshal(map[string]any{
		"connection": "prod", "database": "repository", "collection": "batches", "op": "find",
	})
	if _, err := tool.Invoke(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if !fake.hadDeadline {
		t.Fatal("driver call had no ctx deadline — default timeout not applied")
	}
}
