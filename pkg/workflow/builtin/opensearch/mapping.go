package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// dailyIndexSuffix matches the trailing YYYY-MM-DD of a daily index name, used
// to reject prefix-sharing siblings (e.g. "<prefix>-2026-06-17-reindex").
var dailyIndexSuffix = regexp.MustCompile(`\d{4}-\d{2}-\d{2}$`)

// indexPrefix strips a trailing wildcard from the allowed index pattern,
// yielding the literal date-name prefix (e.g. "dev.cpe-*" -> "dev.cpe-").
func indexPrefix(pattern string) string {
	return strings.TrimSuffix(pattern, "*")
}

// latestIndexBefore returns the lexicographically greatest index name strictly
// less than todayIndexName. Zero-padded date suffixes make lexical order ==
// chronological order, so this is the most recent existing index before today.
// Returns "" when none qualifies.
func latestIndexBefore(names []string, todayIndexName string) string {
	best := ""
	for _, n := range names {
		if n < todayIndexName && n > best {
			best = n
		}
	}
	return best
}

// mappingCache memoizes the resolved mapping body keyed by today's index name,
// so once a day's mapping is fetched, BOTH the _cat/indices and _mapping calls
// are skipped for every subsequent request that same UTC day.
type mappingCache struct {
	mu    sync.Mutex
	byDay map[string]string
}

func (c *mappingCache) get(day string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.byDay[day]
	return v, ok
}

func (c *mappingCache) put(day, body string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byDay == nil {
		c.byDay = map[string]string{}
	}
	c.byDay[day] = body
}

// nowUTC is overridable in tests.
var nowUTC = func() time.Time { return time.Now().UTC() }

// IndexMapping satisfies node.MappingProvider. It finds the most recent index
// before today via _cat/indices, then GETs that index's _mapping. Schema read
// only — no per-account policy. Any error returns ("", err); the caller treats
// a non-nil error as "unavailable" and never blocks the chat.
func (t *opensearchTool) IndexMapping(ctx context.Context) (string, error) {
	prefix := indexPrefix(t.cfg.AllowedIndexPattern)
	today := prefix + nowUTC().Format("2006-01-02")

	// Same-day hit: skip both the _cat and _mapping round trips.
	if cached, ok := t.mappingCache.get(today); ok {
		return cached, nil
	}

	names, err := t.catIndices(ctx)
	if err != nil {
		return "", err
	}
	idx := latestIndexBefore(names, today)
	if idx == "" {
		// Don't cache the miss — a late-created index should be picked up on a
		// later request the same day.
		return "", fmt.Errorf("opensearch: no index before %s", today)
	}
	body, err := t.getMapping(ctx, idx)
	if err != nil {
		return "", err
	}
	t.mappingCache.put(today, body)
	return body, nil
}

// catIndices returns existing index names matching the allowed pattern.
func (t *opensearchTool) catIndices(ctx context.Context) ([]string, error) {
	url := strings.TrimRight(t.cfg.Host, "/") + "/_cat/indices/" +
		t.cfg.AllowedIndexPattern + "?h=index&format=json&s=index:desc"
	raw, err := t.doGet(ctx, url)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Index string `json:"index"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, fmt.Errorf("opensearch: parse _cat/indices: %w", err)
	}
	// Keep only clean daily indices (<prefix>-YYYY-MM-DD); reject prefix-sharing
	// siblings like "<prefix>-2026-06-17-reindex". latestIndexBefore is
	// order-independent, so no sort is needed.
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if dailyIndexSuffix.MatchString(r.Index) {
			out = append(out, r.Index)
		}
	}
	return out, nil
}

func (t *opensearchTool) getMapping(ctx context.Context, index string) (string, error) {
	url := strings.TrimRight(t.cfg.Host, "/") + "/" + index + "/_mapping"
	raw, err := t.doGet(ctx, url)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (t *opensearchTool) doGet(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(t.user.Reveal(), t.pass.Reveal())
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("opensearch: %d: %s", resp.StatusCode, string(out))
	}
	return out, nil
}
