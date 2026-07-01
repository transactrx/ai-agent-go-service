// pkg/aichatviewer/upload_local_get_test.go
package webbridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestUploadLocalGetServesPreviouslyStoredFile(t *testing.T) {
	dir := t.TempDir()
	local := NewLocalStorage(dir, "/aichatviewer/upload-local")
	res, err := local.Put(context.Background(), AttachmentInput{
		AccountID: "acct-test", Filename: "claim.txt", MediaType: "text/plain", Data: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// URL is /aichatviewer/upload-local/<id>/claim.txt — pull out the id.
	parts := strings.Split(strings.TrimPrefix(res.URL, "/aichatviewer/upload-local/"), "/")
	if len(parts) != 2 {
		t.Fatalf("URL: %s", res.URL)
	}
	id := parts[0]

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals(localAccountID, "acct-test")
		return c.Next()
	})
	app.Get("/aichatviewer/upload-local/:id/:filename", uploadLocalGet(local))

	req := httptest.NewRequest("GET", "/aichatviewer/upload-local/"+id+"/claim.txt", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	// SendFile should produce the bytes.
	buf := make([]byte, 32)
	n, _ := resp.Body.Read(buf)
	if string(buf[:n]) != "hello" {
		t.Fatalf("body: %q", buf[:n])
	}
}

// serveStored puts a file then GETs it back through uploadLocalGet, returning
// the response for header assertions.
func serveStored(t *testing.T, filename, mediaType string, data []byte) *http.Response {
	t.Helper()
	dir := t.TempDir()
	local := NewLocalStorage(dir, "/aichatviewer/upload-local")
	res, err := local.Put(context.Background(), AttachmentInput{
		AccountID: "acct-test", Filename: filename, MediaType: mediaType, Data: data,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	id := strings.Split(strings.TrimPrefix(res.URL, "/aichatviewer/upload-local/"), "/")[0]

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error { c.Locals(localAccountID, "acct-test"); return c.Next() })
	app.Get("/aichatviewer/upload-local/:id/:filename", uploadLocalGet(local))

	resp, err := app.Test(httptest.NewRequest("GET", "/aichatviewer/upload-local/"+id+"/"+filename, nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return resp
}

func TestUploadLocalGetForcesDownloadForActiveContent(t *testing.T) {
	// A stored HTML document must NOT be served as text/html (would run script
	// same-origin). It must be downgraded to a download, with nosniff set.
	resp := serveStored(t, "evil.html", "text/html", []byte("<script>alert(1)</script>"))
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/octet-stream") {
		t.Fatalf("Content-Type = %q, want application/octet-stream (must not render as html)", ct)
	}
}

func TestUploadLocalGetPreservesInlineImages(t *testing.T) {
	// Inline images must keep their real type (this is why we don't blanket
	// Content-Disposition: attachment).
	resp := serveStored(t, "pic.png", "image/png", []byte("\x89PNG\r\n\x1a\n"))
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "image/png") {
		t.Fatalf("Content-Type = %q, want image/png (inline preserved)", ct)
	}
}

func TestSafeInlineContentType(t *testing.T) {
	safe := []string{"image/png", "image/jpeg", "image/gif; charset=binary", "application/pdf", "text/plain; charset=utf-8", "text/csv", "application/json"}
	unsafe := []string{"text/html", "text/html; charset=utf-8", "image/svg+xml", "application/xhtml+xml", "application/javascript", ""}
	for _, ct := range safe {
		if !safeInlineContentType(ct) {
			t.Errorf("safeInlineContentType(%q) = false, want true", ct)
		}
	}
	for _, ct := range unsafe {
		if safeInlineContentType(ct) {
			t.Errorf("safeInlineContentType(%q) = true, want false", ct)
		}
	}
}

func TestUploadLocalGetRejectsTraversal(t *testing.T) {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals(localAccountID, "acct-test")
		return c.Next()
	})
	app.Get("/aichatviewer/upload-local/:id/:filename", uploadLocalGet(NewLocalStorage(t.TempDir(), "/aichatviewer/upload-local")))

	// Fiber routes are exact; the param values themselves must contain the traversal.
	// Use URL-encoded path separators so the params match but contain dangerous chars.
	resp, _ := app.Test(httptest.NewRequest("GET", "/aichatviewer/upload-local/..%2F..%2Fetc/passwd", nil))
	if resp.StatusCode != 400 && resp.StatusCode != 404 {
		t.Fatalf("expected 400 or 404, got %d", resp.StatusCode)
	}
}
