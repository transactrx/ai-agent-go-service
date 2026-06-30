package quickchart

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// stubUploader returns a canned URL and records the bytes it received.
type stubUploader struct {
	gotPNG []byte
	url    string
	err    error
}

func (s *stubUploader) UploadChart(_ context.Context, png []byte) (string, string, error) {
	s.gotPNG = append([]byte(nil), png...)
	if s.err != nil {
		return "", "", s.err
	}
	return "chart/abc.png", s.url, nil
}

func (s *stubUploader) UploadAttachment(_ context.Context, _, _, _ string, _ []byte) (string, string, error) {
	return "", "", errors.New("not used in tests")
}

func newTool(host string, uploader *stubUploader) *quickchartTool {
	return &quickchartTool{
		cfg: Config{
			Host: host, Width: 600, Height: 400,
			BackgroundColor: "white", Format: "png",
			PhiScrubFields: defaultPhiScrubFields,
		},
		http:     &http.Client{},
		logger:   log.New(io.Discard, "", 0),
		uploader: uploader,
	}
}

// pngFixture returns a payload above chartMinBytes with a real PNG magic prefix.
func pngFixture(size int) []byte {
	if size < 8 {
		size = 8
	}
	out := make([]byte, size)
	copy(out, []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A})
	for i := 8; i < size; i++ {
		out[i] = byte(i % 256)
	}
	return out
}

func TestInvokeHappyPath_PostsAndUploadsPNG(t *testing.T) {
	var (
		seenPath string
		seenBody string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		seenBody = string(b)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngFixture(1024))
	}))
	defer srv.Close()

	up := &stubUploader{url: "https://signed.example/chart/abc.png"}
	tool := newTool(srv.URL, up)
	tool.http = srv.Client()

	out, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"bar","data":{"labels":["a","b"],"datasets":[{"label":"x","data":[1,2]}]}}`))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var resp map[string]string
	_ = json.Unmarshal(out, &resp)
	if resp["imageUrl"] != up.url {
		t.Errorf("imageUrl = %q, want %q", resp["imageUrl"], up.url)
	}
	if resp["type"] != "bar" {
		t.Errorf("type = %q", resp["type"])
	}
	if seenPath != "/chart" {
		t.Errorf("expected POST /chart, got %s", seenPath)
	}
	if !strings.Contains(seenBody, `"type":"bar"`) {
		t.Errorf("body missing type: %s", seenBody)
	}
	if len(up.gotPNG) != 1024 {
		t.Errorf("uploader saw %d bytes, expected 1024", len(up.gotPNG))
	}
}

func TestInvoke_RejectsPHI(t *testing.T) {
	tool := newTool("http://unused", &stubUploader{})
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"bar","data":{"labels":["firstName"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err == nil || !strings.Contains(err.Error(), "PHI") {
		t.Fatalf("expected PHI rejection, got %v", err)
	}
}

func TestInvoke_HTTPNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	tool := newTool(srv.URL, &stubUploader{})
	tool.http = srv.Client()
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"bar","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected 500 in error, got %v", err)
	}
}

func TestInvoke_NonImageContentType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>error page</html>"))
	}))
	defer srv.Close()
	tool := newTool(srv.URL, &stubUploader{})
	tool.http = srv.Client()
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"bar","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err == nil || !strings.Contains(err.Error(), "did not return an image") {
		t.Fatalf("expected content-type error, got %v", err)
	}
}

func TestInvoke_TinyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte{0x89, 0x50}) // 2 bytes — well under chartMinBytes
	}))
	defer srv.Close()
	tool := newTool(srv.URL, &stubUploader{})
	tool.http = srv.Client()
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"bar","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err == nil || !strings.Contains(err.Error(), "suspiciously small") {
		t.Fatalf("expected min-bytes error, got %v", err)
	}
}

func TestInvoke_BinaryErrorBody_NotLeakedToModel(t *testing.T) {
	// QuickChart renders config errors as a PNG (format=png) and returns 400
	// WITH that image as the body. The tool must NOT splash raw image bytes
	// into the model-facing error — the model can't read them and gives up.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(pngFixture(2048))
	}))
	defer srv.Close()
	tool := newTool(srv.URL, &stubUploader{})
	tool.http = srv.Client()
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"line","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "400") {
		t.Errorf("error should name the HTTP status: %q", msg)
	}
	// No raw PNG bytes (magic header / nulls) may reach the model.
	if strings.Contains(msg, "PNG") || strings.ContainsRune(msg, '\x89') || strings.ContainsRune(msg, '\x00') {
		t.Errorf("error leaked binary PNG bytes to the model: %q", msg)
	}
	// Must give the model actionable guidance so it can fix the config.
	if !strings.Contains(msg, "datasets") {
		t.Errorf("error should give actionable chart-config guidance: %q", msg)
	}
}

func TestInvoke_TextErrorBody_PassedThrough(t *testing.T) {
	// A JSON/text error body IS useful to the model — keep surfacing it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"unsupported chart type xyz"}`))
	}))
	defer srv.Close()
	tool := newTool(srv.URL, &stubUploader{})
	tool.http = srv.Client()
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"xyz","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err == nil || !strings.Contains(err.Error(), "unsupported chart type xyz") {
		t.Fatalf("text error body should be surfaced to the model, got %v", err)
	}
}

func TestErrorBodyDetail(t *testing.T) {
	cases := []struct {
		name, ct, body string
		wantSub        string
		noBinary       bool
	}{
		{"png image", "image/png", "\x89PNG\x00\x01", "datasets", true},
		{"jpeg image", "image/jpeg", "\xff\xd8\xff\x00", "datasets", true},
		{"octet-stream", "application/octet-stream", "\x00\x01\x02", "datasets", true},
		{"empty body image", "image/png", "", "datasets", true},
		{"unknown content-type", "", "\x00\x01", "datasets", true},
		{"json text", "application/json", `{"error":"bad chart type"}`, "bad chart type", false},
		{"plain text", "text/plain; charset=utf-8", "boom", "boom", false},
		{"html text", "text/html", "<b>err</b>", "err", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := errorBodyDetail(tc.ct, []byte(tc.body))
			if !strings.Contains(got, tc.wantSub) {
				t.Errorf("errorBodyDetail(%q) = %q, want substring %q", tc.ct, got, tc.wantSub)
			}
			if tc.noBinary && (strings.ContainsRune(got, '\x00') || strings.Contains(got, "PNG") ||
				strings.ContainsRune(got, '\xff') || strings.ContainsRune(got, '\x89')) {
				t.Errorf("errorBodyDetail(%q) leaked binary bytes: %q", tc.ct, got)
			}
		})
	}
}

func TestIsTextual(t *testing.T) {
	textual := []string{"text/plain", "text/html", "application/json", "application/xml; charset=utf-8", "TEXT/PLAIN"}
	binary := []string{"image/png", "image/jpeg", "application/octet-stream", "application/pdf", ""}
	for _, ct := range textual {
		if !isTextual(ct) {
			t.Errorf("isTextual(%q) = false, want true", ct)
		}
	}
	for _, ct := range binary {
		if isTextual(ct) {
			t.Errorf("isTextual(%q) = true, want false", ct)
		}
	}
}

func TestInvoke_JPEGErrorBody_NotLeakedToModel(t *testing.T) {
	// Same guarantee as the PNG case but for a JPEG error body.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/jpeg")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46})
	}))
	defer srv.Close()
	tool := newTool(srv.URL, &stubUploader{})
	tool.http = srv.Client()
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"bar","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if msg := err.Error(); strings.ContainsRune(msg, '\xff') || strings.Contains(msg, "JFIF") || !strings.Contains(msg, "datasets") {
		t.Errorf("jpeg error not handled cleanly: %q", msg)
	}
}

func TestInvoke_UploaderError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngFixture(1024))
	}))
	defer srv.Close()
	up := &stubUploader{err: errors.New("kms unavailable")}
	tool := newTool(srv.URL, up)
	tool.http = srv.Client()
	_, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"bar","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err == nil || !strings.Contains(err.Error(), "kms unavailable") {
		t.Fatalf("expected uploader error surfaced, got %v", err)
	}
}

func TestInvoke_ChartURLPrefix_ReturnsShortURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngFixture(1024))
	}))
	defer srv.Close()

	up := &stubUploader{url: "https://signed.example/chart/abc.png"}
	tool := newTool(srv.URL, up)
	tool.http = srv.Client()
	tool.cfg.ChartURLPrefix = "aichatviewer"

	out, err := tool.Invoke(context.Background(), json.RawMessage(`{"type":"bar","data":{"labels":["a"],"datasets":[{"label":"x","data":[1]}]}}`))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var resp map[string]string
	_ = json.Unmarshal(out, &resp)
	const stubKey = "chart/abc.png"
	want := "aichatviewer/" + stubKey
	if resp["imageUrl"] != want {
		t.Errorf("imageUrl = %q, want %q", resp["imageUrl"], want)
	}
}

func TestChartImageURL(t *testing.T) {
	const presigned = "https://bucket.s3.amazonaws.com/chart/abc.png?X-Amz-Signature=zzz"
	cases := []struct {
		name, prefix, key, presigned, want string
	}{
		{"prefix set", "aichatviewer", "chart/abc-123.png", presigned, "aichatviewer/chart/abc-123.png"},
		{"prefix trailing slash trimmed", "aichatviewer/", "chart/abc.png", presigned, "aichatviewer/chart/abc.png"},
		{"empty prefix falls back to presigned", "", "chart/abc.png", presigned, presigned},
		{"blank prefix falls back to presigned", "   ", "chart/abc.png", presigned, presigned},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := chartImageURL(c.prefix, c.key, c.presigned)
			if got != c.want {
				t.Fatalf("chartImageURL(%q,%q,...) = %q, want %q", c.prefix, c.key, got, c.want)
			}
		})
	}
}
