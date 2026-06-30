// pkg/aichatviewer/upload_local_get.go
package webbridge

import (
	"os"
	"path/filepath"

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
		return c.SendFile(path, true)
	}
}
