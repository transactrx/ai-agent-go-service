// pkg/aichatviewer/upload_local_get.go
package webbridge

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// uploadLocalGet serves files previously stored by LocalStorage.Put. The URL
// shape is `/aichatviewer/upload-local/:id/:filename`. The account is read
// from the session (via c.Locals), so users can't read other accounts' files
// even if they guess an `id`.
func uploadLocalGet(local *LocalStorage) fiber.Handler {
	return func(c *fiber.Ctx) error {
		accountID, _ := c.Locals(localAccountID).(string)
		if accountID == "" {
			return c.Status(401).JSON(fiber.Map{"code": "auth-error"})
		}
		id := c.Params("id")
		filename := c.Params("filename")
		if id == "" || filename == "" {
			return c.Status(400).JSON(fiber.Map{"code": "bad-request"})
		}
		// Sanitize: no directory traversal.
		if filepath.Base(filename) != filename || filepath.Base(id) != id {
			return c.Status(400).JSON(fiber.Map{"code": "bad-request"})
		}
		path := filepath.Join(local.root, accountID, id, filename)
		// Statting first lets us return a clean 404 instead of an internal error.
		if _, err := os.Stat(path); err != nil {
			return c.Status(404).JSON(fiber.Map{"code": "not-found"})
		}
		// XSS hardening: these files are served same-origin, so a stored HTML/SVG
		// document would otherwise render (and run script) in the app's origin.
		// nosniff stops content-type sniffing; we then constrain the declared
		// Content-Type to a safe inline allowlist and force everything else to
		// application/octet-stream (download, never render). Inline images/PDF —
		// the reason we don't use Content-Disposition: attachment — still render.
		c.Set(fiber.HeaderXContentTypeOptions, "nosniff")
		if err := c.SendFile(path, true); err != nil {
			return err
		}
		if !safeInlineContentType(c.GetRespHeader(fiber.HeaderContentType)) {
			c.Set(fiber.HeaderContentType, "application/octet-stream")
		}
		return nil
	}
}

// safeInlineContentType reports whether a Content-Type is safe to render inline
// from a same-origin upload. Allowed: non-SVG images, PDF, and plain text-ish
// data (matching the upload MIME allowlist). Everything else — notably
// text/html, image/svg+xml, and unknown types — is not, and is downgraded to a
// download by the caller.
func safeInlineContentType(ct string) bool {
	if i := strings.IndexByte(ct, ';'); i >= 0 { // drop "; charset=..."
		ct = ct[:i]
	}
	ct = strings.ToLower(strings.TrimSpace(ct))
	switch {
	case strings.HasPrefix(ct, "image/") && ct != "image/svg+xml":
		return true
	case ct == "application/pdf", ct == "text/plain", ct == "text/csv", ct == "application/json":
		return true
	default:
		return false
	}
}
