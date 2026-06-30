package webbridge

// ChatStreamRequest is the body POSTed to /aichatviewer/stream.
type ChatStreamRequest struct {
	Message    string `json:"message"`
	SessionId  string `json:"sessionId,omitempty"`
	WorkflowId string `json:"workflowId,omitempty"`
}

// ChatCancelRequest is the body POSTed to /aichatviewer/cancel.
type ChatCancelRequest struct {
	RequestId string `json:"requestId"`
}

// streamRequestEnvelope is what the bridge publishes over NATS as the request body.
// Mirrors the docs/api-reference.md request shape for the chat workflow.
type streamRequestEnvelope struct {
	Message     string             `json:"message"`
	SessionId   string             `json:"sessionId,omitempty"`
	Attachments []ClientAttachment `json:"attachments,omitempty"`
}

// startEventOut is what the bridge writes in the SSE `data:` line for the start event.
// We replace the agent's start payload with one that carries our bridge-generated requestId,
// so the browser cancel path is decoupled from internal stream ids.
type startEventOut struct {
	SessionId  string `json:"sessionId"`
	WorkflowId string `json:"workflowId"`
	RequestId  string `json:"requestId"`
}

func getReplacerMap() map[string]string {
	return map[string]string{
		"provider-disabled": "AI provider is temporarily disabled",
		"policy-deny":       "Request denied by policy",
		"max-iterations":    "The agent gave up after too many steps",
	}
}
