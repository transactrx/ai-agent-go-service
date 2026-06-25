// Package errcode holds the stable error-code catalog returned in stream error
// terminators and pre-stream single responses. Codes are append-only across
// cycles; never reassign meaning.
package errcode

const (
	EndpointNotFound    = "endpoint-not-found"
	BadBody             = "bad-body"
	BadRequest          = "bad-request"
	StreamInitFailed    = "stream-init-failed"
	AuthError           = "auth-error"
	AccountIDMissing    = "account-id-missing"
	UserIDMissing       = "USER_ID_MISSING"
	ExecutorFailed      = "executor-failed"
	RenderError         = "render-error"
	MemoryLoadError     = "memory-load-error"
	MemoryAppendError   = "memory-append-error"
	LLMError            = "llm-error"
	ToolError           = "tool-error"
	UnknownTool         = "unknown-tool"
	MaxIterations       = "max-iterations"
	Timeout             = "timeout"
	Cancelled           = "cancelled"
	Shutdown            = "shutdown"
	Panic               = "panic"
	PolicyDeny          = "policy-deny"
	PolicyError         = "policy-error"
	IndexNotAllowed     = "index-not-allowed"
	DSLInjectionBlocked = "dsl-injection-blocked"
)
