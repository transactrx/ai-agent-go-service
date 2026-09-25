package opensearch

import (
	"encoding/json"
	"fmt"
)

// Document-read guard.
//
// A terms lookup ({"terms":{"field":{"index","id","path"}}}) and a
// more_like_this whose like/unlike names a document make OpenSearch read a
// stored document to build the query. That read is not limited by the
// security scope, so it could reveal something about another account's
// document. They are rejected wherever they appear: in the query clauses, in
// filter/filters aggregations and in sort (nested sort filters are queries).
// Plain value lists and text keep working.

var errDocumentRead = fmt.Errorf("tool/opensearch: terms lookup (index/id/path) and more_like_this with a document are not allowed: " +
	"they read documents outside the security scope. Use a terms list of values or plain text instead. " +
	"Do not try to remove or work around the security scope.")

// rejectDocumentReads checks every model-supplied part of the request.
func rejectDocumentReads(parts ...json.RawMessage) error {
	for _, raw := range parts {
		if len(raw) == 0 {
			continue
		}
		var tree any
		if err := json.Unmarshal(raw, &tree); err != nil {
			continue // shape errors are reported by the owning validator
		}
		if readsOtherDocument(tree) {
			return errDocumentRead
		}
	}
	return nil
}

// readsOtherDocument reports a terms lookup or a more_like_this document
// reference anywhere in the tree.
func readsOtherDocument(node any) bool {
	switch v := node.(type) {
	case map[string]any:
		if terms, ok := v["terms"].(map[string]any); ok {
			for _, value := range terms {
				if isTermsLookup(value) {
					return true
				}
			}
		}
		if mlt, ok := v["more_like_this"].(map[string]any); ok {
			for _, key := range []string{"like", "unlike"} {
				if namesDocument(mlt[key]) {
					return true
				}
			}
		}
		for _, child := range v {
			if readsOtherDocument(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if readsOtherDocument(child) {
				return true
			}
		}
	}
	return false
}

// isTermsLookup reports a terms value that points at a stored document
// ({"index","id","path"}) instead of listing values. Other objects under a
// terms key (for example a terms aggregation's "order") are not lookups.
func isTermsLookup(value any) bool {
	m, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for _, key := range []string{"index", "id", "path"} {
		if _, has := m[key]; has {
			return true
		}
	}
	return false
}

// namesDocument reports a like/unlike value holding a document reference.
func namesDocument(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		return true
	case []any:
		for _, item := range v {
			if _, isDoc := item.(map[string]any); isDoc {
				return true
			}
		}
	}
	return false
}
