package node

import (
	"context"
	"encoding/json"
)

// Tool is invoked by the agent when the LLM emits a tool_use block whose
// name matches the tool's ToolSpec. Agent decides whether to terminate or
// surface tool errors per the tool's failure-policy config.
//
// Idempotency contract (engine retry v1): Tools whose workflow JSON includes
// a `retry` block MUST be idempotent — the engine may call Invoke up to
// maxAttempts times with the same args within one user turn. Tools that
// mutate state, send notifications, or otherwise have observable side
// effects SHOULD either be omitted from retry config or carry an
// idempotency key in their args / config.
//
// Audit of current builtin tools (informational, may evolve):
//   - tool/opensearch — GET search; safe to retry.
//   - tool/quickchart — POST /chart/create; server-side dedupes by body
//     hash, so retries with the same chart spec produce the same URL; safe.
//   - tool/serpapi    — GET search; safe.
//   - tool/ui-*       — short-circuit on AwaitClientToolResult; the agent
//     loop does not run the retry helper on these (clientui branch).
type Tool interface {
	Node
	ToolSpec() ToolSpec
	Invoke(ctx context.Context, args json.RawMessage) (json.RawMessage, error)
	FailurePolicy() FailurePolicy
	// RetryPolicy returns this tool's retry config, or nil if not configured.
	// Tools capture it during Init from env.RetryPolicy() and return it here.
	// The concrete type lives in pkg/workflow/engine/retry.Policy; callers
	// who need the concrete type assert through node.RetryPolicy.
	RetryPolicy() RetryPolicy
}

// ToolSpec is the LLM-facing contract: name, description, JSON-schema for input.
type ToolSpec struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

// FailurePolicy controls what the agent does when a tool returns a Go error.
type FailurePolicy string

const (
	FailureSurfaceToLLM FailurePolicy = "surface-to-llm"
	FailureTerminate    FailurePolicy = "terminate"
)

// ToolError lets a Tool author classify its own failures so the retry
// helper can decide whether to retry without parsing error messages.
// Tools may return a plain error too; the helper falls back to context
// + status-code inference. The Code value matches retry.Class strings:
// "transient" | "timeout" | "upstream" | "validation" | "permission".
type ToolError struct {
	Code      string
	Cause     error
	Retryable bool
}

func (e *ToolError) Error() string {
	if e == nil || e.Cause == nil {
		return ""
	}
	return e.Cause.Error()
}

func (e *ToolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// BaseTool is an optional mixin that captures the per-node retry policy so
// concrete tools don't have to. Embed it in concrete tool structs and call
// BaseTool.InitRetry(env) from your tool's Init.
//
//	type fooTool struct {
//	    node.BaseTool
//	    cfg Config
//	}
//
//	func (t *fooTool) Init(ctx context.Context, env node.NodeEnv) error {
//	    // ... your setup ...
//	    t.InitRetry(env)
//	    return nil
//	}
type BaseTool struct {
	policy RetryPolicy
}

// InitRetry captures the per-node retry policy from env. Idempotent: safe
// to call multiple times — last call wins.
func (b *BaseTool) InitRetry(env NodeEnv) { b.policy = env.RetryPolicy() }

// RetryPolicy returns the captured policy, or nil if InitRetry was never
// called (or was called with an env that had no policy).
func (b *BaseTool) RetryPolicy() RetryPolicy { return b.policy }
