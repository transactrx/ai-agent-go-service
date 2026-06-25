package loader

import (
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// RawConn is the loader-stage representation of a connection. Used by
// ValidateConnections / TopoOrder. Mirrors rawConnection but exported so the
// orchestration code in loader.go and tests can build them.
type RawConn struct {
	From Endpoint
	To   Endpoint
}

// Endpoint identifies one side of a connection.
type Endpoint struct{ Node, Port string }

// ValidateConnections enforces port cardinality, required-ness, and peer-role
// type matching. Each node's input ports come from its NodeSpec.InputPorts.
//
// The role assertions use go type-asserts:
//   - LLM     → node.LLMProvider
//   - Memory  → node.Memory
//   - Tool    → node.Tool
//   - Trigger → node.Trigger
//   - Agent   → node.Agent
//   - Policy  → node.Policy
func ValidateConnections(nodes map[string]node.Node, conns []RawConn) []error {
	var errs []error

	// Build incoming map: dest node id → port name → []source nodes.
	incoming := map[string]map[string][]node.Node{}
	for _, c := range conns {
		dst, ok := nodes[c.To.Node]
		if !ok {
			errs = append(errs, fmt.Errorf("connection: unknown destination node %q", c.To.Node))
			continue
		}
		src, ok := nodes[c.From.Node]
		if !ok {
			errs = append(errs, fmt.Errorf("connection: unknown source node %q", c.From.Node))
			continue
		}
		// Verify the port exists on dst.
		if !portExists(dst.Spec().InputPorts, c.To.Port) {
			errs = append(errs, fmt.Errorf("connection: node %q has no input port %q", c.To.Node, c.To.Port))
			continue
		}
		if incoming[c.To.Node] == nil {
			incoming[c.To.Node] = map[string][]node.Node{}
		}
		incoming[c.To.Node][c.To.Port] = append(incoming[c.To.Node][c.To.Port], src)
	}

	// For each node, check each input port against cardinality/required/peer-role.
	for id, n := range nodes {
		for _, p := range n.Spec().InputPorts {
			peers := incoming[id][p.Name]
			cnt := len(peers)
			switch p.Cardinality {
			case node.CardOne:
				if cnt != 1 {
					if p.Required || cnt > 1 {
						errs = append(errs, fmt.Errorf("node %q port %q: expected exactly 1 connection, got %d", id, p.Name, cnt))
					}
				}
			case node.CardZeroOrOne:
				if cnt > 1 {
					errs = append(errs, fmt.Errorf("node %q port %q: expected at most 1 connection, got %d", id, p.Name, cnt))
				}
			case node.CardZeroOrMany:
				// any count is fine
			}
			if p.Required && cnt == 0 {
				errs = append(errs, fmt.Errorf("node %q port %q: required but unwired", id, p.Name))
			}
			// Peer-role assertion.
			if p.PeerRole != "" {
				for _, peer := range peers {
					if !peerImplements(peer, p.PeerRole) {
						errs = append(errs, fmt.Errorf("node %q port %q: peer %q does not implement role %q",
							id, p.Name, peerID(nodes, peer), p.PeerRole))
					}
				}
			}
		}
	}
	return errs
}

func portExists(ports []node.PortSpec, name string) bool {
	for _, p := range ports {
		if p.Name == name {
			return true
		}
	}
	return false
}

func peerImplements(n node.Node, role node.Role) bool {
	// Primary check: declared Role in the node's spec. This is the authoritative
	// signal and also the only signal a stub/test double reliably sets.
	if n.Spec().Role == role {
		return true
	}
	// Secondary check: interface implementation (for nodes that predate Role or
	// omit it). A node passes if it satisfies the role-specific interface AND
	// declares no conflicting role.
	declaredRole := n.Spec().Role
	if declaredRole != "" {
		// Role is declared but does not match — reject regardless of interfaces.
		return false
	}
	switch role {
	case node.RoleTrigger:
		_, ok := n.(node.Trigger)
		return ok
	case node.RoleLLM:
		_, ok := n.(node.LLMProvider)
		return ok
	case node.RoleMemory:
		_, ok := n.(node.Memory)
		return ok
	case node.RoleTool:
		_, ok := n.(node.Tool)
		return ok
	case node.RoleAgent:
		_, ok := n.(node.Agent)
		return ok
	case node.RolePolicy:
		_, ok := n.(node.Policy)
		return ok
	}
	return false
}

func peerID(nodes map[string]node.Node, target node.Node) string {
	targetSpec := target.Spec()
	for id, n := range nodes {
		s := n.Spec()
		if s.Type == targetSpec.Type && s.Role == targetSpec.Role {
			return id
		}
	}
	return "?"
}

// TopoOrder returns nodes in execution order (sources before destinations).
// Returns an error if the graph contains a cycle. Cycle 1 only supports DAGs.
func TopoOrder(nodes map[string]node.Node, conns []RawConn) ([]string, error) {
	indeg := map[string]int{}
	out := map[string][]string{}
	for id := range nodes {
		indeg[id] = 0
	}
	for _, c := range conns {
		out[c.From.Node] = append(out[c.From.Node], c.To.Node)
		indeg[c.To.Node]++
	}
	var queue []string
	for id, d := range indeg {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	var order []string
	for len(queue) > 0 {
		// Pop front.
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		for _, m := range out[n] {
			indeg[m]--
			if indeg[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	if len(order) != len(nodes) {
		return nil, fmt.Errorf("workflow graph contains a cycle (cycle 1 requires a DAG)")
	}
	return order, nil
}
