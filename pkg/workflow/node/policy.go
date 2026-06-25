package node

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
)

// Policy resolves authorization constraints for a given identity + target.
// Tools call their connected policy peer to derive injectable constraints
// before executing requests against backends. See spec §13.4.
type Policy interface {
	Node
	Resolve(ctx context.Context, req PolicyRequest) (PolicyResult, error)
}

// PolicyRequest carries the inputs to a policy decision.
type PolicyRequest struct {
	Identity   identity.Identity
	Target     string         // e.g., "opensearch", "snowflake"
	TargetMeta map[string]any // tool-specific (e.g., indexPath)
}

// PolicyResult is the policy's decision for one request.
type PolicyResult struct {
	InjectMustClauses []json.RawMessage // OpenSearch DSL clauses to merge into bool.must
	AllowedIndices    []string          // index patterns the identity may query
	Reason            string            // human-readable, for logs
	Deny              bool
	DenyReason        string
}

// PolicyDeniedError is returned by tools when their policy peer denies the
// request. The agent's failurePolicy logic distinguishes this from generic
// transport errors.
type PolicyDeniedError struct {
	Reason         string
	AllowedIndices []string
}

func (e *PolicyDeniedError) Error() string {
	return fmt.Sprintf("policy denied: %s (allowed indices: %v)", e.Reason, e.AllowedIndices)
}
