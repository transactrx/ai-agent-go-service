package node

import "context"

type runClientToolsKey struct{}

// WithRunClientTools returns a ctx that offers extra client-only tools to the
// agent loops of one run (e.g. the RSAssistant bridge's request_user_input).
// The loop lists them after its connected tools (a connected tool of the
// same name wins) and answers their calls like ClientUITool calls: emit
// tool_call, then AwaitClientToolResult. No specs: ctx is returned as is.
func WithRunClientTools(ctx context.Context, specs ...ToolSpec) context.Context {
	if len(specs) == 0 {
		return ctx
	}
	return context.WithValue(ctx, runClientToolsKey{}, append([]ToolSpec(nil), specs...))
}

// RunClientTools returns the run's extra client-only tools (nil = none).
func RunClientTools(ctx context.Context) []ToolSpec {
	s, _ := ctx.Value(runClientToolsKey{}).([]ToolSpec)
	return s
}
