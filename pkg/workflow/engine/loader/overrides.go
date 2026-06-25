package loader

import (
	"context"
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// PromptOverrides is the slice of promptstore.Store the loader needs.
// Declared here so loader has no promptstore dependency.
type PromptOverrides interface {
	Latest(ctx context.Context, workflowID, nodeID, field string) (string, bool, error)
}

// applyOverrides resolves stored override values for the node's declared
// OverridableFields and, when any apply cleanly, rebuilds the instance from
// the patched config. Every failure falls back to the original instance —
// a bad stored value must never break workflow load.
func (l *Loader) applyOverrides(ctx context.Context, workflowID string, raw rawNode, instance node.Node) node.Node {
	spec := instance.Spec()
	if l.prompts == nil || len(spec.OverridableFields) == 0 {
		return instance
	}
	var cfg map[string]any
	if len(raw.Config) > 0 {
		if err := json.Unmarshal(raw.Config, &cfg); err != nil {
			l.logger.Printf("promptoverride: node %s: config not an object, skipping overrides", raw.ID)
			return instance
		}
	}
	if cfg == nil {
		cfg = map[string]any{}
	}
	changed := false
	for _, field := range spec.OverridableFields {
		content, found, err := l.prompts.Latest(ctx, workflowID, raw.ID, field)
		if err != nil {
			l.logger.Printf("promptoverride: node %s field %s: store error, using default: %v", raw.ID, field, err)
			continue
		}
		if !found {
			continue
		}
		resolved, rerrs := Resolve(content, l.lookupEnv)
		if len(rerrs) > 0 {
			l.logger.Printf("promptoverride: node %s field %s: env substitution failed, using default: %v", raw.ID, field, rerrs)
			continue
		}
		resolvedStr, ok := resolved.(string)
		if !ok {
			continue
		}
		if err := l.renderer.Validate(resolvedStr); err != nil {
			l.logger.Printf("promptoverride: node %s field %s: template invalid, using default: %v", raw.ID, field, err)
			continue
		}
		cfg[field] = resolvedStr
		changed = true
		l.logger.Printf("promptoverride: node %s field %s: override applied from store", raw.ID, field)
	}
	if !changed {
		return instance
	}
	patched, err := json.Marshal(cfg)
	if err != nil {
		l.logger.Printf("promptoverride: node %s: re-marshal failed, using default: %v", raw.ID, err)
		return instance
	}
	f, _ := l.registry.Get(raw.Type)
	rebuilt, err := f.New(patched)
	if err != nil {
		l.logger.Printf("promptoverride: node %s: factory rejected patched config, using default: %v", raw.ID, err)
		return instance
	}
	return rebuilt
}
