package loader

import (
	"encoding/json"
	"testing"
)

func TestRawWorkflowKeepsRSAssistantBlock(t *testing.T) {
	raw := []byte(`{"id":"wf","version":1,"trigger":"t","nodes":[],"connections":[],
	  "rsassistant":{"name":"claimSearch","tags":["a"]}}`)
	var w rawWorkflow
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	if len(w.RSAssistant) == 0 {
		t.Fatal("rsassistant block was dropped by rawWorkflow decode")
	}
	var m map[string]any
	if err := json.Unmarshal(w.RSAssistant, &m); err != nil {
		t.Fatal(err)
	}
	if m["name"] != "claimSearch" {
		t.Fatalf("name = %v, want claimSearch", m["name"])
	}
}

func TestRawWorkflowWithoutBlockHasNilManifest(t *testing.T) {
	var w rawWorkflow
	if err := json.Unmarshal([]byte(`{"id":"wf","version":1,"trigger":"t","nodes":[],"connections":[]}`), &w); err != nil {
		t.Fatal(err)
	}
	if w.RSAssistant != nil {
		t.Fatalf("expected nil manifest, got %s", w.RSAssistant)
	}
}

func TestMergeDerivedInheritsRSAssistantBlock(t *testing.T) {
	base := []byte(`{"id":"p","version":1,"trigger":"n1",
	  "rsassistant":{"name":"claimSearch","description":"base desc"},
	  "nodes":[{"id":"n1","type":"t","config":{}}],"connections":[]}`)
	overlay := []byte(`{"id":"c","extends":"p","rsassistant":{"description":"child desc"}}`)
	merged, err := MergeDerived(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	var w rawWorkflow
	if err := json.Unmarshal(merged, &w); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(w.RSAssistant, &m)
	if m["name"] != "claimSearch" || m["description"] != "child desc" {
		t.Fatalf("merged manifest = %v, want name from base and description from overlay", m)
	}
}
