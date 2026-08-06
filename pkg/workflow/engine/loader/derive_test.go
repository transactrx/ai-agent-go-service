package loader

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestExtendsTarget(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{"present", `{"id":"d","extends":"base"}`, "base", true},
		{"absent", `{"id":"plain"}`, "", false},
		{"empty string", `{"id":"d","extends":""}`, "", false},
		{"malformed json", `{"id":`, "", false},
		{"wrong type", `{"id":"d","extends":42}`, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := ExtendsTarget([]byte(c.raw))
			if got != c.want || ok != c.ok {
				t.Fatalf("ExtendsTarget(%s) = (%q,%v), want (%q,%v)", c.raw, got, ok, c.want, c.ok)
			}
		})
	}
}

func TestDeepMerge(t *testing.T) {
	base := map[string]any{
		"a": "base", "keep": 1.0,
		"obj": map[string]any{"x": "bx", "y": "by"},
	}
	overlay := map[string]any{
		"a": "over",
		"obj": map[string]any{"x": "ox", "z": "oz"},
		"new": true,
	}
	got := deepMerge(base, overlay)
	want := map[string]any{
		"a": "over", "keep": 1.0,
		"obj": map[string]any{"x": "ox", "y": "by", "z": "oz"},
		"new": true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deepMerge = %#v, want %#v", got, want)
	}
	// inputs not mutated
	if base["a"] != "base" || base["obj"].(map[string]any)["x"] != "bx" {
		t.Fatal("deepMerge mutated base")
	}
}

func TestDeepMergeNullDeletes(t *testing.T) {
	base := map[string]any{"cfg": map[string]any{"chartUrlPrefix": "https://x", "width": 800.0}}
	overlay := map[string]any{"cfg": map[string]any{"chartUrlPrefix": nil}}
	got := deepMerge(base, overlay)
	cfg := got["cfg"].(map[string]any)
	if _, exists := cfg["chartUrlPrefix"]; exists {
		t.Fatal("null overlay value must delete the key")
	}
	if cfg["width"] != 800.0 {
		t.Fatal("sibling keys must survive")
	}
}

func TestDeepMergeScalarOverArray(t *testing.T) {
	// non-map overlay values replace wholesale (arrays, scalars)
	base := map[string]any{"list": []any{"a", "b"}}
	overlay := map[string]any{"list": []any{"c"}}
	got := deepMerge(base, overlay)
	if !reflect.DeepEqual(got["list"], []any{"c"}) {
		t.Fatal("arrays must replace, not concatenate")
	}
}

const baseDoc = `{
  "id": "base", "version": 1, "description": "base wf", "trigger": "t1",
  "nodes": [
    {"id": "t1", "type": "trigger/nats-chat", "config": {"responseMode": "streaming"}},
    {"id": "tool1", "type": "tool/opensearch", "config": {"allowedIndexPattern": "x-*"}},
    {"id": "ui1", "type": "tool/ui-confirm", "config": {}},
    {"id": "chart1", "type": "tool/quickchart", "config": {"chartUrlPrefix": "https://c", "width": 800}}
  ],
  "connections": [
    {"from": {"node": "t1", "port": "out"}, "to": {"node": "tool1", "port": "in"}},
    {"from": {"node": "t1", "port": "out"}, "to": {"node": "ui1", "port": "in"}},
    {"from": {"node": "ui1", "port": "out"}, "to": {"node": "chart1", "port": "in"}}
  ]
}`

func mustMerge(t *testing.T, overlay string) map[string]any {
	t.Helper()
	merged, err := MergeDerived([]byte(baseDoc), []byte(overlay))
	if err != nil {
		t.Fatalf("MergeDerived: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(merged, &doc); err != nil {
		t.Fatalf("merged output not JSON: %v", err)
	}
	return doc
}

func nodesByID(doc map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, n := range doc["nodes"].([]any) {
		nm := n.(map[string]any)
		out[nm["id"].(string)] = nm
	}
	return out
}

func TestMergeDerivedTopLevelAndNodeConfig(t *testing.T) {
	doc := mustMerge(t, `{
	  "id": "derived", "extends": "base", "description": "derived wf",
	  "nodes": [
	    {"id": "t1", "config": {"responseMode": "single", "allowResponseModeOverride": true}},
	    {"id": "chart1", "config": {"chartUrlPrefix": null}}
	  ]
	}`)
	if doc["id"] != "derived" || doc["description"] != "derived wf" {
		t.Fatalf("top-level overlay lost: id=%v desc=%v", doc["id"], doc["description"])
	}
	if _, has := doc["extends"]; has {
		t.Fatal("extends key must be stripped from merged output")
	}
	ns := nodesByID(doc)
	tcfg := ns["t1"]["config"].(map[string]any)
	if tcfg["responseMode"] != "single" || tcfg["allowResponseModeOverride"] != true {
		t.Fatalf("trigger config not merged: %v", tcfg)
	}
	// type inherited from base even though overlay omitted it
	if ns["t1"]["type"] != "trigger/nats-chat" {
		t.Fatalf("node type must inherit from base, got %v", ns["t1"]["type"])
	}
	ccfg := ns["chart1"]["config"].(map[string]any)
	if _, has := ccfg["chartUrlPrefix"]; has {
		t.Fatal("null must delete chartUrlPrefix")
	}
	if ccfg["width"] != 800.0 {
		t.Fatal("untouched config keys must survive")
	}
	// untouched node fully inherited
	if ns["tool1"]["config"].(map[string]any)["allowedIndexPattern"] != "x-*" {
		t.Fatal("untouched base node lost")
	}
}

func TestMergeDerivedRemoveNodePrunesConnections(t *testing.T) {
	doc := mustMerge(t, `{
	  "id": "derived", "extends": "base",
	  "nodes": [{"id": "ui1", "$remove": true}]
	}`)
	ns := nodesByID(doc)
	if _, has := ns["ui1"]; has {
		t.Fatal("$remove'd node still present")
	}
	conns := doc["connections"].([]any)
	if len(conns) != 1 {
		t.Fatalf("connections touching ui1 must be pruned, got %d: %v", len(conns), conns)
	}
	c := conns[0].(map[string]any)
	if c["to"].(map[string]any)["node"] != "tool1" {
		t.Fatalf("wrong surviving connection: %v", c)
	}
}

func TestMergeDerivedAddNodeAndConnection(t *testing.T) {
	doc := mustMerge(t, `{
	  "id": "derived", "extends": "base",
	  "nodes": [{"id": "extra1", "type": "tool/serpapi", "config": {"k": "v"}}],
	  "connections": [{"from": {"node": "t1", "port": "out"}, "to": {"node": "extra1", "port": "in"}}]
	}`)
	ns := nodesByID(doc)
	if ns["extra1"] == nil || ns["extra1"]["type"] != "tool/serpapi" {
		t.Fatal("appended node missing")
	}
	if len(doc["connections"].([]any)) != 4 {
		t.Fatalf("overlay connection not appended: %v", doc["connections"])
	}
}

func TestMergeDerivedDuplicateOverlayConnectionNotDoubled(t *testing.T) {
	doc := mustMerge(t, `{
	  "id": "derived", "extends": "base",
	  "connections": [{"from": {"node": "t1", "port": "out"}, "to": {"node": "tool1", "port": "in"}}]
	}`)
	if n := len(doc["connections"].([]any)); n != 3 {
		t.Fatalf("duplicate connection appended: %d", n)
	}
}

func TestMergeDerivedErrors(t *testing.T) {
	cases := []struct {
		name, overlay, wantSub string
	}{
		{"remove unknown node", `{"id":"d","extends":"base","nodes":[{"id":"ghost","$remove":true}]}`, `"ghost"`},
		{"connection to removed node", `{"id":"d","extends":"base","nodes":[{"id":"ui1","$remove":true}],"connections":[{"from":{"node":"t1","port":"out"},"to":{"node":"ui1","port":"in"}}]}`, `"ui1"`},
		{"connection to unknown node", `{"id":"d","extends":"base","connections":[{"from":{"node":"t1","port":"out"},"to":{"node":"nope","port":"in"}}]}`, `"nope"`},
		{"missing id", `{"extends":"base"}`, "id"},
		{"malformed overlay", `{"id":`, "parse"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := MergeDerived([]byte(baseDoc), []byte(c.overlay))
			if err == nil {
				t.Fatalf("want error containing %q, got nil", c.wantSub)
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Fatalf("error %q does not mention %q", err, c.wantSub)
			}
		})
	}
}

const baseWithAgent = `{
  "id": "abase", "version": 1, "trigger": "t1",
  "nodes": [
    {"id": "t1", "type": "trigger/nats-chat", "config": {}},
    {"id": "agent1", "type": "ai/agent",
     "config": {"systemMessageFixed": "base fixed", "systemMessageFlexible": "base flex"}}
  ],
  "connections": [{"from": {"node": "t1", "port": "out"}, "to": {"node": "agent1", "port": "in"}}]
}`

func TestPromptCarveOutMissingPromptsFails(t *testing.T) {
	cases := []struct{ name, overlay string }{
		{"agent not in overlay at all", `{"id":"d","extends":"abase"}`},
		{"only fixed supplied", `{"id":"d","extends":"abase","nodes":[{"id":"agent1","config":{"systemMessageFixed":"own"}}]}`},
		{"only flexible supplied", `{"id":"d","extends":"abase","nodes":[{"id":"agent1","config":{"systemMessageFlexible":"own"}}]}`},
		{"empty string fixed", `{"id":"d","extends":"abase","nodes":[{"id":"agent1","config":{"systemMessageFixed":"","systemMessageFlexible":"own"}}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := MergeDerived([]byte(baseWithAgent), []byte(c.overlay))
			if err == nil {
				t.Fatal("want prompt-ownership error, got nil")
			}
			if !strings.Contains(err.Error(), "prompts are never inherited") {
				t.Fatalf("wrong error: %v", err)
			}
		})
	}
}

func TestPromptCarveOutSatisfied(t *testing.T) {
	doc := mustMergeWith(t, baseWithAgent, `{
	  "id": "d", "extends": "abase",
	  "nodes": [{"id": "agent1", "config": {
	    "systemMessageFixed": "own fixed", "systemMessageFlexible": "own flex"}}]
	}`)
	cfg := nodesByID(doc)["agent1"]["config"].(map[string]any)
	if cfg["systemMessageFixed"] != "own fixed" || cfg["systemMessageFlexible"] != "own flex" {
		t.Fatalf("overlay prompts must win: %v", cfg)
	}
}

func TestPromptCarveOutRemovedAgentNotRequired(t *testing.T) {
	if _, err := MergeDerived([]byte(baseWithAgent),
		[]byte(`{"id":"d","extends":"abase","nodes":[{"id":"agent1","$remove":true}]}`)); err != nil {
		t.Fatalf("removed agent must not demand prompts: %v", err)
	}
}

func TestPromptCarveOutAppendedAgentRequiresPrompts(t *testing.T) {
	_, err := MergeDerived([]byte(baseDoc),
		[]byte(`{"id":"d","extends":"base","nodes":[{"id":"agent9","type":"ai/agent","config":{}}]}`))
	if err == nil || !strings.Contains(err.Error(), "prompts are never inherited") {
		t.Fatalf("appended agent without prompts must fail, got: %v", err)
	}
}

// mustMergeWith is mustMerge with a custom base.
func mustMergeWith(t *testing.T, base, overlay string) map[string]any {
	t.Helper()
	merged, err := MergeDerived([]byte(base), []byte(overlay))
	if err != nil {
		t.Fatalf("MergeDerived: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(merged, &doc); err != nil {
		t.Fatalf("merged output not JSON: %v", err)
	}
	return doc
}
