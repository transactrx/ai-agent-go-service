// pkg/aichatviewer/upload_test.go
package webbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

type stubStorage struct {
	lastInput AttachmentInput
	res       AttachmentResult
	err       error
}

func (s *stubStorage) Put(_ context.Context, in AttachmentInput) (AttachmentResult, error) {
	s.lastInput = in
	return s.res, s.err
}

func newTestApp(stub AttachmentStorage, cfg UploadConfig) *fiber.App {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals(localAccountID, "acct-test")
		c.Locals(localUserID, "user-test")
		return c.Next()
	})
	app.Post("/aichatviewer/upload", UploadAttachment(stub, cfg))
	return app
}

func multipartBody(t *testing.T, filename, mime, content string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	h := map[string][]string{
		"Content-Disposition": {`form-data; name="file"; filename="` + filename + `"`},
		"Content-Type":        {mime},
	}
	fw, err := mw.CreatePart(h)
	if err != nil {
		t.Fatalf("CreatePart: %v", err)
	}
	_, _ = io.Copy(fw, strings.NewReader(content))
	if err := mw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return &buf, mw.FormDataContentType()
}

func httptestNew(method, url string, body *bytes.Buffer, contentType string) *http.Request {
	r := httptest.NewRequest(method, url, body)
	r.Header.Set("Content-Type", contentType)
	return r
}

func TestUploadHandlerStoresAndReturnsURL(t *testing.T) {
	stub := &stubStorage{res: AttachmentResult{URL: "https://x/y", MediaType: "image/png", Filename: "a.png", Size: 4}}
	app := newTestApp(stub, UploadConfig{MaxBytes: 1 << 20, AllowedMime: []string{"image/png"}})

	body, ct := multipartBody(t, "a.png", "image/png", "PNG!")
	resp, err := app.Test(httptestNew("POST", "/aichatviewer/upload", body, ct))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var out AttachmentResult
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.URL != "https://x/y" {
		t.Fatalf("url: %s", out.URL)
	}
	if stub.lastInput.Filename != "a.png" || stub.lastInput.MediaType != "image/png" {
		t.Fatalf("storage input: %+v", stub.lastInput)
	}
	if stub.lastInput.AccountID != "acct-test" {
		t.Fatalf("expected accountID from middleware Locals, got %q", stub.lastInput.AccountID)
	}
}

func TestUploadHandlerRejectsTooLarge(t *testing.T) {
	stub := &stubStorage{}
	app := newTestApp(stub, UploadConfig{MaxBytes: 5, AllowedMime: []string{"image/png"}})
	body, ct := multipartBody(t, "a.png", "image/png", "0123456789")
	resp, _ := app.Test(httptestNew("POST", "/aichatviewer/upload", body, ct))
	if resp.StatusCode != 413 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestUploadHandlerRejectsDisallowedMime(t *testing.T) {
	stub := &stubStorage{}
	app := newTestApp(stub, UploadConfig{MaxBytes: 1 << 20, AllowedMime: []string{"image/png"}})
	body, ct := multipartBody(t, "x.exe", "application/octet-stream", "MZ")
	resp, _ := app.Test(httptestNew("POST", "/aichatviewer/upload", body, ct))
	if resp.StatusCode != 415 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}
