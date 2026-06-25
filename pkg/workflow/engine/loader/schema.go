package loader

import "fmt"

// validateEnvelope checks the top-level shape of a parsed workflow JSON
// (rules 1–2 from spec §2.4). It does NOT validate per-node config — that's
// done after the factory is found.
func validateEnvelope(w rawWorkflow) []error {
	var errs []error
	if w.ID == "" {
		errs = append(errs, fmt.Errorf("$.id: required"))
	}
	if w.Version == 0 {
		errs = append(errs, fmt.Errorf("$.version: required"))
	}
	if w.Version != 1 {
		errs = append(errs, fmt.Errorf("$.version: only version 1 is supported in cycle 1 (got %d)", w.Version))
	}
	if w.Trigger == "" {
		errs = append(errs, fmt.Errorf("$.trigger: required"))
	}
	if len(w.Nodes) == 0 {
		errs = append(errs, fmt.Errorf("$.nodes: must contain at least one node"))
	}
	if len(w.Connections) == 0 {
		errs = append(errs, fmt.Errorf("$.connections: must contain at least one connection"))
	}
	seen := map[string]bool{}
	for i, n := range w.Nodes {
		if n.ID == "" {
			errs = append(errs, fmt.Errorf("$.nodes[%d].id: required", i))
			continue
		}
		if seen[n.ID] {
			errs = append(errs, fmt.Errorf("$.nodes[%d].id: duplicate id %q", i, n.ID))
		}
		seen[n.ID] = true
		if n.Type == "" {
			errs = append(errs, fmt.Errorf("$.nodes[%d].type: required", i))
		}
	}
	if !seen[w.Trigger] {
		errs = append(errs, fmt.Errorf("$.trigger: %q does not match any node id", w.Trigger))
	}
	for i, c := range w.Connections {
		if c.From.Node == "" || c.From.Port == "" {
			errs = append(errs, fmt.Errorf("$.connections[%d].from: node and port required", i))
		}
		if c.To.Node == "" || c.To.Port == "" {
			errs = append(errs, fmt.Errorf("$.connections[%d].to: node and port required", i))
		}
		if c.From.Node != "" && !seen[c.From.Node] {
			errs = append(errs, fmt.Errorf("$.connections[%d].from.node: %q is not a defined node", i, c.From.Node))
		}
		if c.To.Node != "" && !seen[c.To.Node] {
			errs = append(errs, fmt.Errorf("$.connections[%d].to.node: %q is not a defined node", i, c.To.Node))
		}
	}
	return errs
}
