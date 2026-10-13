package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// indexAccessTimeout bounds one FunctionForIndex call so a slow or hung lookup
// cannot hang start-up. A timeout resolves the workflow as unbound.
var indexAccessTimeout = 15 * time.Second

// resolveIndexAccess works out which identity function guards the workflow's
// index. It finds every node implementing node.IndexScoped, takes its policy
// peer (the node wired to its node.PortPolicy input) and asks it, once.
//
//   - no tool whose policy peer implements node.IndexFunctionProvider -> none
//     (a policy node that did not opt in keeps today's behavior)
//   - every such tool resolves to the same non-blank function          -> bound
//   - anything else (no row, error, blank, timeout, several functions,
//     a tool without exactly one policy peer)                          -> unbound
func resolveIndexAccess(ctx context.Context, nodes map[string]node.Node, conns map[ConnKey][]ConnRef) node.IndexAccess {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var (
		patterns []string
		fnID     string
	)
	for _, id := range ids {
		scoped, ok := nodes[id].(node.IndexScoped)
		if !ok {
			continue
		}
		provider, cause := policyProvider(id, nodes, conns)
		if cause != "" {
			return node.IndexAccess{Status: node.IndexAccessUnbound, Index: scoped.AllowedIndexPattern(), Cause: cause}
		}
		if provider == nil {
			continue // the policy peer has not opted in
		}
		pattern := scoped.AllowedIndexPattern()
		patterns = append(patterns, pattern)
		fn, cause := lookupFunction(ctx, provider, pattern)
		if cause != "" {
			return node.IndexAccess{Status: node.IndexAccessUnbound, Index: strings.Join(patterns, ","), Cause: cause}
		}
		if fnID != "" && fn != fnID {
			return node.IndexAccess{Status: node.IndexAccessUnbound, Index: strings.Join(patterns, ","),
				Cause: "workflow spans indexes guarded by different functions"}
		}
		fnID = fn
	}
	if len(patterns) == 0 {
		return node.IndexAccess{Status: node.IndexAccessNone}
	}
	return node.IndexAccess{Status: node.IndexAccessBound, Index: strings.Join(patterns, ","), FunctionID: fnID}
}

// policyProvider returns the tool's policy peer as an IndexFunctionProvider.
// A nil provider with an empty cause means the peer has not opted in.
func policyProvider(toolID string, nodes map[string]node.Node, conns map[ConnKey][]ConnRef) (node.IndexFunctionProvider, string) {
	refs := conns[ConnKey{DestNode: toolID, DestPort: node.PortPolicy}]
	if len(refs) != 1 {
		return nil, fmt.Sprintf("node %q needs exactly one policy peer, has %d", toolID, len(refs))
	}
	provider, _ := nodes[refs[0].SrcNode].(node.IndexFunctionProvider)
	return provider, ""
}

func lookupFunction(ctx context.Context, p node.IndexFunctionProvider, pattern string) (fn, cause string) {
	cctx, cancel := context.WithTimeout(ctx, indexAccessTimeout)
	defer cancel()
	fn, err := p.FunctionForIndex(cctx, pattern)
	if err != nil {
		return "", err.Error()
	}
	if fn = strings.TrimSpace(fn); fn == "" {
		return "", "empty function id"
	}
	return fn, ""
}
