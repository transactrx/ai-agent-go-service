// Package-internal derivation support (spec: workflow-derivation Option F).
// A workflow file may declare {"extends": "<baseId>"}; MergeDerived (Task 2+)
// merges it over the base's raw JSON before the normal load pipeline runs.
package loader

import (
	"encoding/json"
	"fmt"
)

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

// MergeDerived merges a derived workflow document (one carrying "extends")
// over its base's raw JSON. The result is a complete standalone document —
// the "extends" key is stripped — ready for the normal load pipeline.
// Prompt carve-out for ai/agent nodes is enforced here (never inherited).
func MergeDerived(baseRaw, overlayRaw []byte) ([]byte, error) {
	var base, overlay map[string]any
	if err := json.Unmarshal(baseRaw, &base); err != nil {
		return nil, fmt.Errorf("derived merge: parse base: %w", err)
	}
	if err := json.Unmarshal(overlayRaw, &overlay); err != nil {
		return nil, fmt.Errorf("derived merge: parse overlay: %w", err)
	}
	derivedID, _ := overlay["id"].(string)
	if derivedID == "" {
		return nil, fmt.Errorf("derived merge: overlay must set its own id")
	}

	baseNodes := asObjectList(base["nodes"])
	overlayNodes := asObjectList(overlay["nodes"])
	baseConns := asObjectList(base["connections"])
	overlayConns := asObjectList(overlay["connections"])

	mergedNodes, removed, err := mergeNodes(derivedID, baseNodes, overlayNodes)
	if err != nil {
		return nil, err
	}
	mergedConns, err := mergeConnections(derivedID, baseConns, overlayConns, mergedNodes, removed)
	if err != nil {
		return nil, err
	}

	// Top-level: deep-merge everything except the list fields handled above.
	topBase := shallowCopyWithout(base, "nodes", "connections")
	topOverlay := shallowCopyWithout(overlay, "nodes", "connections", "extends")
	merged := deepMerge(topBase, topOverlay)
	merged["nodes"] = mergedNodes
	merged["connections"] = mergedConns

	out, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("derived workflow %q: re-marshal: %w", derivedID, err)
	}
	return out, nil
}

// asObjectList coerces a decoded JSON array into []map[string]any, skipping
// non-object entries (envelope validation reports those later).
func asObjectList(v any) []map[string]any {
	arr, _ := v.([]any)
	out := make([]map[string]any, 0, len(arr))
	for _, e := range arr {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func shallowCopyWithout(m map[string]any, drop ...string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	for _, k := range drop {
		delete(out, k)
	}
	return out
}

// mergeNodes applies overlay node entries to the base list by id. Returns the
// merged list plus the set of removed node ids.
func mergeNodes(derivedID string, base, overlay []map[string]any) ([]map[string]any, map[string]bool, error) {
	byID := map[string]int{}
	merged := make([]map[string]any, len(base))
	for i, n := range base {
		id, _ := n["id"].(string)
		byID[id] = i
		merged[i] = n
	}
	removed := map[string]bool{}
	var appended []map[string]any
	for _, on := range overlay {
		id, _ := on["id"].(string)
		if id == "" {
			return nil, nil, fmt.Errorf("derived workflow %q: overlay node missing id", derivedID)
		}
		idx, exists := byID[id]
		if rm, _ := on["$remove"].(bool); rm {
			if !exists {
				return nil, nil, fmt.Errorf("derived workflow %q: $remove targets unknown node %q", derivedID, id)
			}
			removed[id] = true
			continue
		}
		if !exists {
			appended = append(appended, on)
			continue
		}
		merged[idx] = deepMerge(merged[idx], on)
	}
	out := make([]map[string]any, 0, len(merged)+len(appended))
	for _, n := range merged {
		id, _ := n["id"].(string)
		if !removed[id] {
			out = append(out, n)
		}
	}
	out = append(out, appended...)

	if err := enforcePromptOwnership(derivedID, out, overlay); err != nil {
		return nil, nil, err
	}
	return out, removed, nil
}

// mergeConnections keeps base connections not touching removed nodes and
// appends new overlay connections. Overlay connections must reference nodes
// that exist in the merged document.
func mergeConnections(derivedID string, base, overlay []map[string]any, nodes []map[string]any, removed map[string]bool) ([]map[string]any, error) {
	exists := map[string]bool{}
	for _, n := range nodes {
		id, _ := n["id"].(string)
		exists[id] = true
	}
	connKey := func(c map[string]any) string {
		f, _ := c["from"].(map[string]any)
		t, _ := c["to"].(map[string]any)
		fn, _ := f["node"].(string)
		fp, _ := f["port"].(string)
		tn, _ := t["node"].(string)
		tp, _ := t["port"].(string)
		return fn + "\x00" + fp + "\x00" + tn + "\x00" + tp
	}
	touchesRemoved := func(c map[string]any) (string, bool) {
		f, _ := c["from"].(map[string]any)
		t, _ := c["to"].(map[string]any)
		for _, ep := range []map[string]any{f, t} {
			n, _ := ep["node"].(string)
			if removed[n] {
				return n, true
			}
			if !exists[n] {
				return n, true
			}
		}
		return "", false
	}

	var out []map[string]any
	seen := map[string]bool{}
	for _, c := range base {
		if _, gone := touchesRemoved(c); gone {
			continue // base connections touching removed nodes are pruned silently (spec)
		}
		out = append(out, c)
		seen[connKey(c)] = true
	}
	for _, c := range overlay {
		if n, bad := touchesRemoved(c); bad {
			return nil, fmt.Errorf("derived workflow %q: overlay connection references unknown or removed node %q", derivedID, n)
		}
		if seen[connKey(c)] {
			continue
		}
		out = append(out, c)
		seen[connKey(c)] = true
	}
	return out, nil
}

// enforcePromptOwnership is implemented in Task 3 (prompt carve-out).
func enforcePromptOwnership(derivedID string, merged []map[string]any, overlay []map[string]any) error {
	return nil
}
