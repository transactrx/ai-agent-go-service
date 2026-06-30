package executor

import (
	"encoding/json"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Workflow holds the fully-wired, immutable description of one loaded workflow.
// Defined here so the executor package does not import the engine package
// (which would create a cycle).
type Workflow struct {
	ID          string
	Description string
	Trigger     node.Trigger
	Nodes       map[string]node.Node
	TopoOrder   []string
	Connections map[ConnKey][]ConnRef
	AllowedTpl  map[string][]string
	RawConfigs  map[string]json.RawMessage
}

// ConnKey identifies a connection's destination.
type ConnKey struct{ DestNode, DestPort string }

// ConnRef identifies a connection's source.
type ConnRef struct{ SrcNode, SrcPort string }
