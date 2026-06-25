package loader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/render"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/retry"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// LoadResult is what the loader returns for one workflow.
type LoadResult struct {
	ID          string
	Description string
	Trigger     node.Trigger
	Nodes       map[string]node.Node
	TopoOrder   []string
	Connections []RawConn
	RetryByNode map[string]*retry.Policy
	RawConfigs  map[string]json.RawMessage
}

// Loader orchestrates the per-workflow load pipeline (spec §4.3).
type Loader struct {
	registry  *node.Registry
	hosts     map[string]any
	renderer  *render.Renderer
	logger    *log.Logger
	lookupEnv func(string) (string, bool)
	prompts   PromptOverrides

	// envForNode is the engine's per-node-env builder; injected so the loader
	// can wire the peer resolver after building the connection map. The
	// engine package wires this in.
	envForNode EnvBuilder
}

// EnvBuilder constructs a NodeEnv for one node. It returns the env plus a
// hook the loader uses to install the peer resolver after connections are
// resolved.
type EnvBuilder func(workflowID, nodeID string, policy *retry.Policy) (env node.NodeEnv, setPeer func(func(string) ([]node.Node, error)))

// NewLoader returns a configured loader.
func NewLoader(registry *node.Registry, hosts map[string]any, renderer *render.Renderer, logger *log.Logger, lookupEnv func(string) (string, bool), envBuilder EnvBuilder, prompts PromptOverrides) *Loader {
	return &Loader{
		registry:   registry,
		hosts:      hosts,
		renderer:   renderer,
		logger:     logger,
		lookupEnv:  lookupEnv,
		envForNode: envBuilder,
		prompts:    prompts,
	}
}

// LoadOne parses, validates, builds, and Inits one workflow's nodes. Returns a
// fully wired LoadResult or an error describing the first failure.
func (l *Loader) LoadOne(ctx context.Context, raw []byte) (*LoadResult, error) {
	// 1. Parse to map[string]any for envsubst, then re-marshal into rawWorkflow.
	var anyDoc any
	if err := json.Unmarshal(raw, &anyDoc); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	// 2. ${VAR} substitution.
	resolved, resolveErrs := Resolve(anyDoc, l.lookupEnv)
	if len(resolveErrs) > 0 {
		return nil, fmt.Errorf("env-var substitution: %v", resolveErrs)
	}

	// 3. Sensitive-name validation.
	if errs := ValidateSensitiveFields(resolved); len(errs) > 0 {
		return nil, fmt.Errorf("sensitive fields: %v", errs)
	}

	// 4. Re-encode and decode into typed envelope.
	resolvedBytes, err := json.Marshal(resolved)
	if err != nil {
		return nil, fmt.Errorf("re-marshal: %w", err)
	}
	var w rawWorkflow
	if err := json.Unmarshal(resolvedBytes, &w); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}

	// 5. Envelope validation.
	if errs := validateEnvelope(w); len(errs) > 0 {
		return nil, fmt.Errorf("envelope: %v", errs)
	}

	// 6. Build node instances via factories. Factories validate their own configs.
	nodes := map[string]node.Node{}
	rawConfigs := map[string]json.RawMessage{}
	for _, n := range w.Nodes {
		f, ok := l.registry.Get(n.Type)
		if !ok {
			return nil, fmt.Errorf("node %q: unknown type %q", n.ID, n.Type)
		}
		instance, err := f.New(n.Config)
		if err != nil {
			return nil, fmt.Errorf("node %q: factory: %w", n.ID, err)
		}
		instance = l.applyOverrides(ctx, w.ID, n, instance)
		nodes[n.ID] = instance
		rawConfigs[n.ID] = n.Config
	}

	// 6b. Parse universal retry block per node; reject for v1-disallowed roles.
	retryByNode, err := parseRetryByNode(w.Nodes, nodes)
	if err != nil {
		return nil, err
	}

	// 7. Validate templated config fields against the renderer catalog.
	for _, n := range w.Nodes {
		spec := nodes[n.ID].Spec()
		if len(spec.AllowedTemplates) == 0 {
			continue
		}
		if err := validateAllowedTemplates(n.Config, spec.AllowedTemplates, l.renderer); err != nil {
			return nil, fmt.Errorf("node %q: templates: %w", n.ID, err)
		}
	}

	// 8. Connection + port + cardinality + peer-role validation.
	conns := make([]RawConn, len(w.Connections))
	for i, c := range w.Connections {
		conns[i] = RawConn{
			From: Endpoint{Node: c.From.Node, Port: c.From.Port},
			To:   Endpoint{Node: c.To.Node, Port: c.To.Port},
		}
	}
	if errs := ValidateConnections(nodes, conns); len(errs) > 0 {
		return nil, fmt.Errorf("connections: %v", errs)
	}

	// 9. Topological order.
	order, err := TopoOrder(nodes, conns)
	if err != nil {
		return nil, err
	}

	// 10. Build per-node NodeEnv with peer resolver and call Init.
	for _, id := range order {
		env, setPeer := l.envForNode(w.ID, id, retryByNode[id])
		setPeer(makePeerResolver(id, nodes, conns))
		if err := nodes[id].Init(ctx, env); err != nil {
			return nil, fmt.Errorf("node %q: Init: %w", id, err)
		}
	}

	// 11. Type-assert the trigger node.
	trigger, ok := nodes[w.Trigger].(node.Trigger)
	if !ok {
		return nil, fmt.Errorf("trigger node %q does not implement node.Trigger", w.Trigger)
	}

	return &LoadResult{
		ID:          w.ID,
		Description: w.Description,
		Trigger:     trigger,
		Nodes:       nodes,
		TopoOrder:   order,
		Connections: conns,
		RetryByNode: retryByNode,
		RawConfigs:  rawConfigs,
	}, nil
}

// makePeerResolver builds the closure exposed via NodeEnv.Peer for one node.
func makePeerResolver(forNode string, nodes map[string]node.Node, conns []RawConn) func(string) ([]node.Node, error) {
	// Pre-compute incoming peers per port for forNode.
	byPort := map[string][]node.Node{}
	for _, c := range conns {
		if c.To.Node != forNode {
			continue
		}
		byPort[c.To.Port] = append(byPort[c.To.Port], nodes[c.From.Node])
	}
	return func(port string) ([]node.Node, error) {
		return byPort[port], nil
	}
}

// parseRetryByNode walks the raw nodes for `retry` blocks. For each one it
// looks up the instantiated node, checks its role against the v1 allowlist
// (tool/agent/llm), and rejects all other roles. Returns the parsed policy
// keyed by node ID; nodes without a retry block (or with an empty one that
// parses to nil) are simply omitted.
func parseRetryByNode(rawNodes []rawNode, instances map[string]node.Node) (map[string]*retry.Policy, error) {
	out := map[string]*retry.Policy{}
	for _, n := range rawNodes {
		if len(n.Retry) == 0 {
			continue
		}
		instance, ok := instances[n.ID]
		if !ok {
			return nil, fmt.Errorf("node %q: missing instance for retry resolution", n.ID)
		}
		role := instance.Spec().Role
		switch role {
		case node.RoleTool, node.RoleAgent, node.RoleLLM:
			// allowed in v1
		default:
			return nil, fmt.Errorf("node %q: retry is not supported for role=%s in v1", n.ID, role)
		}
		pol, perr := retry.ParsePolicy(n.Retry)
		if perr != nil {
			return nil, fmt.Errorf("node %q: %w", n.ID, perr)
		}
		if pol == nil {
			continue
		}
		out[n.ID] = pol
	}
	return out, nil
}

// validateAllowedTemplates walks the node's raw config and runs the renderer's
// Validate over every string field that appears in the AllowedTemplates list.
// Cycle 1 only checks top-level fields by name.
func validateAllowedTemplates(rawConfig json.RawMessage, allowed []string, r *render.Renderer) error {
	if len(rawConfig) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(rawConfig, &m); err != nil {
		return nil // factory will fail later with the real error
	}
	for _, field := range allowed {
		v, ok := m[field]
		if !ok {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		if err := r.Validate(s); err != nil {
			return fmt.Errorf("%s: %w", field, err)
		}
	}
	return nil
}
