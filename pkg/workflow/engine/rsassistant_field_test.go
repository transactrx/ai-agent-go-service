// pkg/workflow/engine/rsassistant_field_test.go
package engine_test

import (
	"context"
	"encoding/json"
	"testing"
)

func TestEngineWorkflowCarriesRSAssistantManifest(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "wf1.json", `{
	  "id": "wf1", "version": 1, "trigger": "trig",
	  "rsassistant": {"name": "claimSearch", "displayName": "Claim Search"},
	  "nodes": [
	    {"id": "trig",  "type": "test/trigger", "config": {}},
	    {"id": "agent", "type": "test/agent",   "config": {}}
	  ],
	  "connections": [
	    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}
	  ]
	}`)
	writeFile(t, dir, "wf2.json", `{
	  "id": "wf2", "version": 1, "trigger": "trig",
	  "nodes": [
	    {"id": "trig",  "type": "test/trigger", "config": {}},
	    {"id": "agent", "type": "test/agent",   "config": {}}
	  ],
	  "connections": [
	    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}
	  ]
	}`)
	eng := newTestEngine(t, dir)
	if err := eng.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	wf1, ok := eng.Workflow("wf1")
	if !ok {
		t.Fatal("wf1 not registered")
	}
	var m map[string]any
	if err := json.Unmarshal(wf1.RSAssistant, &m); err != nil {
		t.Fatalf("wf1.RSAssistant not JSON: %v (%s)", err, wf1.RSAssistant)
	}
	if m["displayName"] != "Claim Search" {
		t.Fatalf("displayName = %v", m["displayName"])
	}
	wf2, _ := eng.Workflow("wf2")
	if wf2.RSAssistant != nil {
		t.Fatalf("wf2 should have nil manifest, got %s", wf2.RSAssistant)
	}
}
