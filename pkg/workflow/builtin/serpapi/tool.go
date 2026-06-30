package serpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// Invoke runs one SerpAPI search and returns the trimmed organic results.
func (t *serpapiTool) Invoke(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Query string `json:"query"`
		Num   int    `json:"num,omitempty"`
		HL    string `json:"hl,omitempty"`
		GL    string `json:"gl,omitempty"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("tool/serpapi: bad input: %w", err)
	}
	if in.Query == "" {
		return nil, fmt.Errorf("tool/serpapi: query is required")
	}

	q := url.Values{}
	q.Set("q", in.Query)
	q.Set("api_key", t.apiKey.Reveal())
	q.Set("engine", t.cfg.DefaultEngine)
	if in.Num > 0 {
		q.Set("num", strconv.Itoa(in.Num))
	}
	if in.HL != "" {
		q.Set("hl", in.HL)
	}
	if in.GL != "" {
		q.Set("gl", in.GL)
	}

	endpoint := t.cfg.Host + "/search.json?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("tool/serpapi: %d: %s", resp.StatusCode, truncate(body, 200))
	}

	var parsed struct {
		Organic []struct {
			Title   string `json:"title"`
			Link    string `json:"link"`
			Snippet string `json:"snippet"`
		} `json:"organic_results"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("tool/serpapi: parse response: %w", err)
	}

	limit := t.cfg.MaxResults
	if len(parsed.Organic) < limit {
		limit = len(parsed.Organic)
	}
	trimmed := make([]map[string]string, 0, limit)
	for i := 0; i < limit; i++ {
		trimmed = append(trimmed, map[string]string{
			"title":   parsed.Organic[i].Title,
			"link":    parsed.Organic[i].Link,
			"snippet": parsed.Organic[i].Snippet,
		})
	}
	return json.Marshal(trimmed)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
