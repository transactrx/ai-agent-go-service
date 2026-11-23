package natschat

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/transactrx/nats-agent/pkg/ask"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// askUserTool is nats-agent's request_user_input, offered to the run's
// agent loop(s) as a client-only tool when the request sets askUser (the
// RSAssistant bridge, for a caller that can show forms). The caller
// answers each call on the stream's tool-result subject.
var askUserTool = func() node.ToolSpec {
	schema, _ := json.Marshal(ask.ToolInputSchema())
	return node.ToolSpec{
		Name:        ask.ToolName,
		Description: ask.ToolDescription() + "\n\n" + ask.Guidance,
		InputSchema: schema,
	}
}()

// EnableAskUser lets this endpoint honor a request's askUser flag.
// rsassistant.Start calls it only for workflows published with
// rsassistant.askUser: true. Other endpoints ignore the flag and strip it
// from the body, so they behave as before askUser existed.
func (t *natsChatTrigger) EnableAskUser() { t.askUserEnabled.Store(true) }

// streamRunContext adds the run-scoped request_user_input tool when the
// endpoint is enabled and the streaming request asked for it; otherwise ctx
// is unchanged.
//
// askUser is not authenticated: any caller of an enabled endpoint may set
// it. That is harmless: the tool is fixed (no caller-supplied spec), runs
// nothing on the server, and its result comes only from whoever holds this
// stream's tool-result subject, i.e. the same caller. A caller that sets it
// and never answers gets the loop's usual client-tool timeout error.
func streamRunContext(ctx context.Context, body chatRequestBody, enabled bool) context.Context {
	if !enabled || !body.AskUser {
		return ctx
	}
	return node.WithRunClientTools(ctx, askUserTool)
}

// askUserFlag reads the askUser flag from a raw body: only the exact
// lowercase key with the literal true counts. The typed body never decodes
// it, so no askUser value can fail a request (as before the flag existed).
func askUserFlag(raw []byte) bool {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return false
	}
	return string(m["askUser"]) == "true"
}

// stripAskUser removes every top-level key Go's case-insensitive JSON
// matching would map to askUser (askUser, AskUser, ASKUSER, ...) from a body
// (what templates render as the trigger event). Any other body is returned
// as is.
func stripAskUser(raw []byte) []byte {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return raw
	}
	found := false
	for k := range m {
		if strings.EqualFold(k, "askUser") {
			delete(m, k)
			found = true
		}
	}
	if !found {
		return raw
	}
	out, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return out
}
