package serpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
)

func TestInvokeHappyPathTrimsResults(t *testing.T) {
	rawResp := `{
	  "organic_results": [
	    {"title": "A", "link": "https://a", "snippet": "snippet A", "position": 1},
	    {"title": "B", "link": "https://b", "snippet": "snippet B", "position": 2},
	    {"title": "C", "link": "https://c", "snippet": "snippet C", "position": 3}
	  ],
	  "ads": [{"title": "ad"}]
	}`
	var seenQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(rawResp))
	}))
	defer srv.Close()

	tool := &serpapiTool{
		cfg: Config{
			Host:          srv.URL,
			DefaultEngine: "google",
			MaxResults:    5,
		},
		apiKey: secret.New("K"),
		http:   srv.Client(),
	}
	out, err := tool.Invoke(context.Background(), json.RawMessage(`{"query":"drug recall"}`))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var results []map[string]string
	if err := json.Unmarshal(out, &results); err != nil {
		t.Fatalf("unmarshal: %v (raw=%s)", err, out)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if results[0]["title"] != "A" || results[0]["link"] != "https://a" || results[0]["snippet"] != "snippet A" {
		t.Fatalf("first result wrong: %+v", results[0])
	}
	if _, ok := results[0]["position"]; ok {
		t.Fatalf("position should be stripped")
	}
	if !strings.Contains(seenQuery, "q=drug+recall") && !strings.Contains(seenQuery, "q=drug%20recall") {
		t.Fatalf("query not in URL params: %s", seenQuery)
	}
	if !strings.Contains(seenQuery, "api_key=K") {
		t.Fatalf("api_key not in URL params: %s", seenQuery)
	}
	if !strings.Contains(seenQuery, "engine=google") {
		t.Fatalf("engine not in URL params: %s", seenQuery)
	}
}

func TestInvokeRespectsMaxResults(t *testing.T) {
	rawResp := `{"organic_results":[
	  {"title":"A","link":"a","snippet":"A"},
	  {"title":"B","link":"b","snippet":"B"},
	  {"title":"C","link":"c","snippet":"C"},
	  {"title":"D","link":"d","snippet":"D"}
	]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(rawResp))
	}))
	defer srv.Close()
	tool := &serpapiTool{cfg: Config{Host: srv.URL, DefaultEngine: "google", MaxResults: 2}, apiKey: secret.New("K"), http: srv.Client()}
	out, _ := tool.Invoke(context.Background(), json.RawMessage(`{"query":"x"}`))
	var r []any
	_ = json.Unmarshal(out, &r)
	if len(r) != 2 {
		t.Fatalf("expected 2 trimmed results, got %d", len(r))
	}
}

func TestInvokeMissingQuery(t *testing.T) {
	tool := &serpapiTool{cfg: Config{Host: "http://unused", DefaultEngine: "google", MaxResults: 5}, apiKey: secret.New("K"), http: &http.Client{}}
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "query") {
		t.Fatalf("expected query-required error, got %v", err)
	}
}

func TestInvokeHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	tool := &serpapiTool{cfg: Config{Host: srv.URL, DefaultEngine: "google", MaxResults: 5}, apiKey: secret.New("K"), http: srv.Client()}
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"query":"x"}`))
	if err == nil || !strings.Contains(err.Error(), "429") {
		t.Fatalf("expected 429 error, got %v", err)
	}
}
