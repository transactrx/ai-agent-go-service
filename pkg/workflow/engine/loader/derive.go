// Package-internal derivation support (spec: workflow-derivation Option F).
// A workflow file may declare {"extends": "<baseId>"}; MergeDerived (Task 2+)
// merges it over the base's raw JSON before the normal load pipeline runs.
package loader

import "encoding/json"

// ExtendsTarget reports the "extends" marker of a raw workflow document.
// Malformed JSON returns ("", false) — the load pipeline surfaces the real
// parse error for non-derived files, and MergeDerived re-parses anyway.
func ExtendsTarget(raw []byte) (string, bool) {
	var peek struct {
		Extends string `json:"extends"`
	}
	if err := json.Unmarshal(raw, &peek); err != nil {
		return "", false
	}
	if peek.Extends == "" {
		return "", false
	}
	return peek.Extends, true
}

// deepMerge returns base merged with overlay: maps merge recursively with
// overlay winning; a nil overlay value deletes the key; every other overlay
// value (scalar, array) replaces wholesale. Inputs are not mutated.
func deepMerge(base, overlay map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(overlay))
	for k, v := range base {
		out[k] = v
	}
	for k, ov := range overlay {
		if ov == nil {
			delete(out, k)
			continue
		}
		om, oIsMap := ov.(map[string]any)
		bm, bIsMap := out[k].(map[string]any)
		if oIsMap && bIsMap {
			out[k] = deepMerge(bm, om)
			continue
		}
		out[k] = ov
	}
	return out
}
