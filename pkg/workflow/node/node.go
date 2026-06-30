// Package node defines the framework's load-bearing abstractions: the universal
// Node contract, role-specific interfaces (Trigger/LLMProvider/Memory/Tool/
// Agent/Policy), connection ports, and the node factory registry.
//
// Engine and builtins both depend on this package; this package depends on
// nothing engine-specific. See spec §3 and §13.4 for design.
package node

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
)

// Node is the universal black-box contract every node type implements.
type Node interface {
	Spec() NodeSpec
	Init(ctx context.Context, env NodeEnv) error
	Close(ctx context.Context) error
}

// NodeSpec is the immutable metadata a node type publishes about itself.
type NodeSpec struct {
	Type             string
	Role             Role
	Description      string
	InputPorts       []PortSpec
	OutputPorts      []PortSpec
	InputDataSchema  json.RawMessage
	OutputDataSchema json.RawMessage
	ConfigSchema     json.RawMessage
	AllowedTemplates []string
	// OverridableFields lists top-level config fields whose values may be
	// replaced at workflow load (and hot-reconfigured later) from an external
	// prompt store. Mirrors the AllowedTemplates declarative pattern.
	OverridableFields []string
}

// Role categorizes a node's primary capability. Each role corresponds to a
// role-specific interface (e.g., RoleLLM ↔ LLMProvider).
type Role string

const (
	RoleTrigger Role = "trigger"
	RoleLLM     Role = "llm"
	RoleMemory  Role = "memory"
	RoleTool    Role = "tool"
	RoleAgent   Role = "agent"
	RolePolicy  Role = "policy"
)

// NodeEnv is the only window through which a Node touches engine/transport
// state. Black-box principle: nodes never reach into engine internals or other
// nodes' internals.
type NodeEnv interface {
	Logger() *log.Logger
	NodeID() string
	WorkflowID() string

	// Secret resolves an env-var NAME (from a *Env config field) into a
	// redacting wrapper. The raw value never appears in logs or errors.
	Secret(envVarName string) (secret.String, error)

	// Render expands {{ }} placeholders in a string. Catalog defined by the
	// engine renderer (see spec §8.4).
	Render(template string, ctx RenderCtx) (string, error)

	// Host returns a transport handle by kind (cycle 1: "nats").
	Host(kind string) (any, bool)

	// Peer returns the connected nodes on a typed input port.
	Peer(portName string) ([]Node, error)

	// AwaitClientToolResult blocks until a `tool_result_from_client` arrives
	// for the given toolCallID, or until timeout/ctx fires. Used only by tools
	// implementing ClientUITool. Returns the raw JSON payload the client sent.
	// Most nodes never call this; it lives on the universal NodeEnv interface so client-only tools can rely on it being present in any streaming context.
	AwaitClientToolResult(ctx context.Context, toolCallID string, timeout time.Duration) ([]byte, error)

	// RetryPolicy returns the parsed retry block for THIS node, or nil if
	// the workflow JSON had no `retry` sibling-of-`config` on this node.
	// Nodes consult it during Init to capture their own policy; the agent
	// loop reads tool.RetryPolicy() (a separate method on Tool) rather
	// than this directly.
	RetryPolicy() RetryPolicy
}

// Reconfigurable is an optional interface for nodes that can apply a new
// value for one of their OverridableFields at runtime (hot prompt update)
// without a workflow reload. Engine calls it from the prompt-change
// broadcast path; value arrives env-substituted.
type Reconfigurable interface {
	Reconfigure(field, value string) error
}

// MappingProvider is implemented by any node able to describe the index schema
// for the current request. The engine discovers it by interface assertion (like
// node.Agent) and never depends on a concrete node type.
type MappingProvider interface {
	IndexMapping(ctx context.Context) (string, error)
}

// RetryPolicy is the abstract per-node retry config exposed to nodes.
// The concrete shape lives in pkg/workflow/engine/retry.Policy; this
// interface lets the node package reference it without import cycles.
type RetryPolicy interface {
	IsRetryEnabled() bool
}

// RenderCtx pins the per-request values used by template placeholders.
type RenderCtx struct {
	Now          time.Time
	SessionID    string
	UserID       string
	RequestID    string
	TriggerEvent any
	NodeOutputs  map[string]any
	UserName     string // X-User-Name — human display name of the asker
	TimeZone     string // X-Time-Zone — IANA tz of the asker (e.g. America/New_York)
	IndexMapping string // live index field mapping, pre-fetched per request
}
