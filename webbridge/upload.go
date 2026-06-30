// pkg/aichatviewer/upload.go
package webbridge

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// UploadConfig is the upload route's tunables.
type UploadConfig struct {
	MaxBytes    int64    // 0 = unlimited
	AllowedMime []string // exact strings or "image/*"-style glob
}

// UploadAttachment is the POST /aichatviewer/upload handler. Cookie-auth is
// performed by upstream middleware; this handler reads accountID from
// c.Locals (same convention as StreamChat).
func UploadAttachment(storage AttachmentStorage, cfg UploadConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		accountID, _ := c.Locals(localAccountID).(string)
		if accountID == "" {
			return c.Status(401).JSON(fiber.Map{"code": "auth-error", "message": "no account"})
		}

		fh, err := c.FormFile("file")
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"code": "bad-request", "message": "missing file"})
		}
		if cfg.MaxBytes > 0 && fh.Size > cfg.MaxBytes {
			return c.Status(413).JSON(fiber.Map{"code": "too-large", "message": fmt.Sprintf("max %d bytes", cfg.MaxBytes)})
		}
		mt := fh.Header.Get("Content-Type")
		if !mimeAllowed(mt, cfg.AllowedMime) {
			return c.Status(415).JSON(fiber.Map{"code": "bad-mime", "message": mt})
		}

		f, err := fh.Open()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"code": "io", "message": err.Error()})
		}
		defer func() { _ = f.Close() }()
		data, err := io.ReadAll(f)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"code": "io", "message": err.Error()})
		}

		res, err := storage.Put(context.Background(), AttachmentInput{
			AccountID: accountID,
			Filename:  safeName(fh.Filename),
			MediaType: mt,
			Data:      data,
		})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"code": "storage", "message": err.Error()})
		}
		return c.JSON(res)
	}
}

func mimeAllowed(got string, allowed []string) bool {
	for _, a := range allowed {
		if a == got {
			return true
		}
		if strings.HasSuffix(a, "/*") && strings.HasPrefix(got, strings.TrimSuffix(a, "*")) {
			return true
		}
	}
	return false
}

func safeName(s string) string {
	s = strings.ReplaceAll(s, "\\", "/")
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	if s == "" {
		s = "upload"
	}
	return s
}
