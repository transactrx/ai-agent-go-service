// pkg/aichatviewer/upload_local_get_test.go
package webbridge

import (
	"context"
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
