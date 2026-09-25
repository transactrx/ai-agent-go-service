package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Invoke parses the LLM input, asks the policy peer for constraints, and
// executes the final HTTP request.
func (t *opensearchTool) Invoke(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	var in struct {
		IndexPath    string                     `json:"indexPath"`
		QueryBody    map[string]json.RawMessage `json:"queryBody"`
		Size         *int                       `json:"size,omitempty"`
		Aggregations json.RawMessage            `json:"aggregations,omitempty"`
		Sort         json.RawMessage            `json:"sort,omitempty"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("tool/opensearch: bad input: %w", err)
	}
	if in.IndexPath == "" {
		return nil, fmt.Errorf("tool/opensearch: indexPath is required")
	}

	// 1. DSL injection guard.
	for k := range in.QueryBody {
		if k != "must" && k != "must_not" && k != "filter" {
			return nil, fmt.Errorf("tool/opensearch: queryBody has disallowed key %q (only must/must_not/filter permitted)", k)
		}
	}

	// 1a. Clause shape: a single clause object is wrapped in a list, so a
	// model's clause is never dropped; anything else is an error.
	for k, raw := range in.QueryBody {
		clauses, err := normalizeClauseList(raw)
		if err != nil {
			return nil, fmt.Errorf("tool/opensearch: queryBody.%s must be a list of query clauses", k)
		}
		in.QueryBody[k] = clauses
	}

	// 1b. Aggregation allowlist: only types computed over the security-scoped
	// query (see aggallowlist.go). Rejects "global" and other scope escapes.
	aggs, err := normalizeAggregations(in.Aggregations)
	if err != nil {
		return nil, err
	}

	// 1c. Document-read guard (see docread.go): no terms lookup or
	// more_like_this document in the clauses, aggregations or sort.
	parts := []json.RawMessage{aggs, in.Sort}
	for _, clause := range in.QueryBody {
		parts = append(parts, clause)
	}
	if err := rejectDocumentReads(parts...); err != nil {
		return nil, err
	}

	// 2. Index pattern guard — element-wise validation against the
	// workflow-configured allowedIndexPattern. Closes Risk A
	// (e.g., "prod.cpe-2026-04-30,*" or cross-family lists).
	if err := validateIndexPath(in.IndexPath, t.allowedRegexp); err != nil {
		return nil, err
	}

	// 3. Identity.
	id, ok := identity.FromContext(ctx)
	if !ok || id.AccountID == "" {
		return nil, fmt.Errorf("tool/opensearch: missing account id in context")
	}

	// 4. Policy.
	pr, err := t.policy.Resolve(ctx, node.PolicyRequest{
		Identity:   id,
		Target:     "opensearch",
		TargetMeta: map[string]any{"indexPath": in.IndexPath},
	})
	if err != nil {
		return nil, fmt.Errorf("tool/opensearch: policy resolve: %w", err)
	}
	if pr.Deny {
		return nil, &node.PolicyDeniedError{Reason: pr.DenyReason, AllowedIndices: pr.AllowedIndices}
	}

	// 5. Final query.
	finalQuery := buildFinalQuery(in.QueryBody, pr.InjectMustClauses)
	body := map[string]any{"query": finalQuery}
	if in.Size != nil {
		body["size"] = *in.Size
	} else {
		body["size"] = t.cfg.MaxResultSize
	}
	if len(aggs) > 0 {
		body["aggs"] = aggs // validated tree only; never the raw model input
	}
	if len(in.Sort) > 0 {
		body["sort"] = json.RawMessage(in.Sort)
	}
	bodyBytes, _ := json.Marshal(body)

	// 6. HTTP.
	url := buildURL(t.cfg.Host, in.IndexPath)
	if t.logger != nil {
		acct := id.AccountID
		t.logger.Printf("opensearch query: wf=%s node=%s acct=%s url=%s body=%s", t.wfID, t.nodeID, acct, url, string(bodyBytes))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(t.user.Reveal(), t.pass.Reveal())

	resp, err := t.http.Do(req)
	if err != nil {
		if t.logger != nil {
			t.logger.Printf("opensearch query failed: wf=%s node=%s url=%s err=%v", t.wfID, t.nodeID, url, err)
		}
		return nil, err
	}
	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		if t.logger != nil {
			t.logger.Printf("opensearch query rejected: wf=%s node=%s url=%s status=%d body=%s", t.wfID, t.nodeID, url, resp.StatusCode, string(out))
		}
		return nil, fmt.Errorf("tool/opensearch: %d: %s", resp.StatusCode, string(out))
	}
	if t.logger != nil {
		t.logger.Printf("opensearch query ok: wf=%s node=%s url=%s status=%d bytes=%d", t.wfID, t.nodeID, url, resp.StatusCode, len(out))
	}
	return json.RawMessage(out), nil
}

// buildURL composes the cluster URL from host + indexPath. Ensures /_search
// suffix so the LLM doesn't have to. ignore_unavailable=true makes
// multi-index queries skip indices that don't exist yet (e.g. today's daily
// index right after UTC midnight, before first ingestion) instead of
// failing the whole request with index_not_found.
func buildURL(host, indexPath string) string {
	url := strings.TrimRight(host, "/") + "/" + strings.TrimLeft(indexPath, "/")
	if !strings.HasSuffix(url, "/_search") {
		url = strings.TrimRight(url, "/") + "/_search"
	}
	return url + "?ignore_unavailable=true"
}

// normalizeClauseList returns raw as a JSON list of clause objects. A single
// object becomes a one-element list; null/empty stays empty.
func normalizeClauseList(raw json.RawMessage) (json.RawMessage, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return nil, nil
	}
	if strings.HasPrefix(s, "{") {
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("invalid clause")
		}
		return json.RawMessage("[" + s + "]"), nil
	}
	var list []json.RawMessage
	if err := json.Unmarshal([]byte(s), &list); err != nil {
		return nil, err
	}
	for _, c := range list {
		if !strings.HasPrefix(strings.TrimSpace(string(c)), "{") {
			return nil, fmt.Errorf("clause is not an object")
		}
	}
	return json.RawMessage(s), nil
}

// buildFinalQuery merges LLM-supplied bool sub-clauses with server-injected
// policy must clauses. Result is wrapped in a top-level bool.
func buildFinalQuery(llmClauses map[string]json.RawMessage, policyMust []json.RawMessage) map[string]any {
	final := map[string]any{}
	if raw, ok := llmClauses["must"]; ok && len(raw) > 0 && string(raw) != "null" {
		var arr []json.RawMessage
		_ = json.Unmarshal(raw, &arr)
		final["must"] = append(arr, policyMust...)
	} else if len(policyMust) > 0 {
		final["must"] = policyMust
	}
	if raw, ok := llmClauses["must_not"]; ok && len(raw) > 0 && string(raw) != "null" {
		final["must_not"] = json.RawMessage(raw)
	}
	if raw, ok := llmClauses["filter"]; ok && len(raw) > 0 && string(raw) != "null" {
		final["filter"] = json.RawMessage(raw)
	}
	return map[string]any{"bool": final}
}
