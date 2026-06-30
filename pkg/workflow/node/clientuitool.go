package node

// ClientUITool is an optional capability a Tool may implement. When the agent
// loop sees ClientOnly()==true, it skips Invoke entirely and instead emits a
// tool_call event then awaits a tool_result-from-client over the transport
// (see NodeEnv.AwaitClientToolResult). Resumes the loop with the received
// payload as the tool result.
type ClientUITool interface {
	Tool
	ClientOnly() bool
}
