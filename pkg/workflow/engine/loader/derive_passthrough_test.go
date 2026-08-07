package loader

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestMergePassthroughUntouchedFields is a golden regression test: fields the
// overlay never mentions must survive MergeDerived byte-for-byte (deep-equal),
// and only the fields the overlay actually targets should change. Node types
// are kept neutral ("t") to stay clear of the ai/agent prompt carve-out
// (enforcePromptOwnership in derive.go), which is out of scope here.
func TestMergePassthroughUntouchedFields(t *testing.T) {
	base := []byte(`{"id":"p","version":1,"description":"d",
      "settings":{"a":1,"b":[1,2,3],"c":{"deep":true},"zero":0,"empty":""},
      "nodes":[{"id":"n1","type":"t","config":{"x":1,"arr":["q"],"nested":{"k":"v"}}},
               {"id":"n2","type":"t","config":{"y":2}}],
      "connections":[{"from":{"node":"n1","port":"out"},"to":{"node":"n2","port":"in"}}]}`)
	overlay := []byte(`{"id":"c","extends":"p","nodes":[{"id":"n2","config":{"y":9}}]}`)
	merged, err := MergeDerived(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	var got, want map[string]any
	_ = json.Unmarshal(merged, &got)
	_ = json.Unmarshal(base, &want)
	// untouched subtrees must be deep-equal
	for _, k := range []string{"version", "description", "settings", "connections"} {
		if !reflect.DeepEqual(got[k], want[k]) {
			t.Fatalf("field %q altered by merge:\n got %#v\nwant %#v", k, got[k], want[k])
		}
	}

	// extract nodes by id from both merged and base
	nodeByID := func(nodes []any) map[string]map[string]any {
		out := map[string]map[string]any{}
		for _, n := range nodes {
			m := n.(map[string]any)
			out[m["id"].(string)] = m
		}
		return out
	}
	gotNodes := nodeByID(got["nodes"].([]any))
	wantNodes := nodeByID(want["nodes"].([]any))

	// n1 untouched (deep-equal to base)
	if !reflect.DeepEqual(gotNodes["n1"], wantNodes["n1"]) {
		t.Fatalf("node n1 altered by merge:\n got %#v\nwant %#v", gotNodes["n1"], wantNodes["n1"])
	}

	// n2.config.y overridden by overlay
	n2Cfg, _ := gotNodes["n2"]["config"].(map[string]any)
	if y, _ := n2Cfg["y"].(float64); y != 9 {
		t.Fatalf("node n2.config.y = %v, want 9", n2Cfg["y"])
	}
}
