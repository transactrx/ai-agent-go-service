package rsassistant

import (
	"strings"

	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// agentAccess decides the identity pair a workflow's agent card declares (and
// that nats-agent validates tokens against). A non-empty skip means the
// workflow must not be published.
//
//   - none (or zero value) -> the service-wide APP_FUNCTION_ID, as before per-index functions
//   - bound                -> the workflow's index function (skip if it carries no function id)
//   - unbound              -> skip (fail closed)
//   - anything else        -> skip (fail closed)
func agentAccess(cfg Config, ia node.IndexAccess) (access wire.AgentAccess, skip string) {
	switch ia.Kind() {
	case node.IndexAccessNone:
		return wire.AgentAccess{AppID: cfg.AppID, FunctionID: cfg.FunctionID}, ""
	case node.IndexAccessBound:
		if strings.TrimSpace(ia.FunctionID) == "" {
			return wire.AgentAccess{}, "index function unbound: bound access has no function id"
		}
		return wire.AgentAccess{AppID: cfg.AppID, FunctionID: ia.FunctionID}, ""
	case node.IndexAccessUnbound:
		cause := ia.Cause
		if cause == "" {
			cause = "no usable function"
		}
		return wire.AgentAccess{}, "index function unbound: " + cause
	default:
		return wire.AgentAccess{}, "index function unbound: unknown index access status"
	}
}
