package opensearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
)

func TestIndexPrefix(t *testing.T) {
	if got := indexPrefix("dev.cpe-*"); got != "dev.cpe-" {
		t.Errorf("indexPrefix = %q, want dev.cpe-", got)
	}
	if got := indexPrefix("exact-index"); got != "exact-index" {
		t.Errorf("no-wildcard prefix = %q", got)
	}
}

func TestLatestIndexBefore(t *testing.T) {
	today := "dev.cpe-2026-06-18"
	names := []string{
		"dev.cpe-2026-06-18", // today — excluded
		"dev.cpe-2026-06-15", // gap
		"dev.cpe-2026-06-17", // latest before today
		"dev.cpe-2026-06-10",
	}
	if got := latestIndexBefore(names, today); got != "dev.cpe-2026-06-17" {
		t.Errorf("latestIndexBefore = %q, want dev.cpe-2026-06-17", got)
	}
	if got := latestIndexBefore([]string{today}, today); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
	if got := latestIndexBefore(nil, today); got != "" {
		t.Errorf("expected empty for nil, got %q", got)
	}
}

func TestIndexMappingFetchesAndCaches(t *testing.T) {
	var catCalls, mapCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_cat/indices/dev.cpe-*":
			catCalls++
			// Includes a prefix-sharing sibling that is NOT a clean daily index;
			// it must be filtered out so the clean -17 index is chosen.
			_, _ = w.Write([]byte(`[{"index":"dev.cpe-2026-06-17-reindex"},{"index":"dev.cpe-2026-06-17"},{"index":"dev.cpe-2026-06-15"}]`))
		case r.URL.Path == "/dev.cpe-2026-06-17/_mapping":
			mapCalls++
			_, _ = w.Write([]byte(`{"dev.cpe-2026-06-17":{"mappings":{}}}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	old := nowUTC
	nowUTC = func() time.Time { return time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC) }
	defer func() { nowUTC = old }()

	tool := &opensearchTool{http: srv.Client(), user: secret.New("u"), pass: secret.New("p")}
	tool.cfg.Host = srv.URL
	tool.cfg.AllowedIndexPattern = "dev.cpe-*"

	got, err := tool.IndexMapping(context.Background())
	if err != nil || got != `{"dev.cpe-2026-06-17":{"mappings":{}}}` {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := tool.IndexMapping(context.Background()); err != nil {
		t.Fatalf("second call err %v", err)
	}
	// Same-day second call must skip BOTH _cat and _mapping (per-day cache).
	if catCalls != 1 || mapCalls != 1 {
		t.Errorf("expected per-day cache to skip both calls (cat=%d map=%d)", catCalls, mapCalls)
	}
}
