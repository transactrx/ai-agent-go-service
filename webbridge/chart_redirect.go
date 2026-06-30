// pkg/aichatviewer/chart_redirect.go
package webbridge

import (
	"context"
	"net/http"
	"regexp"

	"github.com/gofiber/fiber/v2"
)

// ChartPresigner re-signs a chart object for direct browser GET from S3. key is
// the full object key ("chart/<uuid>.png").
type ChartPresigner interface {
	// ChartExists reports whether the object exists in S3. A clean "not found"
	// (HeadObject 404) returns (false, nil); real errors return (false, err).
	ChartExists(ctx context.Context, key string) (bool, error)
	// PresignChart presigns a GET URL for the given key. Only called when
	// ChartExists returned (true, nil).
	PresignChart(ctx context.Context, key string) (string, error)
}

// chartNameRe is the ONLY accepted shape for the :name path segment: the
// canonical UUID form the QuickChart tool produces (xxxxxxxx-xxxx-xxxx-xxxx-
// xxxxxxxxxxxx) followed by ".png" (security control #2). Go RE2 `$` matches
// end-of-text only — NOT before a trailing newline — and no multiline flag is
// set, so a name like "uuid.png\n" is rejected. The pattern also rejects path
// traversal ("../"), other extensions, and any attempt to reach a different S3
// key prefix such as uploads/ (which holds user PHI). Combined with the IAM
// grant scoped to chart/* (control #1), a crafted request cannot reach uploads/.
var chartNameRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\.png$`)

// chartRedirectHandler validates the uuid, checks S3 existence, presigns
// chart/<name>, and 302s to S3. Missing objects (model-fabricated ids or
// expired objects) return 200 with a small SVG placeholder so the browser
// never renders a broken image. Any real error returns 404 (no detail leaked).
func chartRedirectHandler(p ChartPresigner) fiber.Handler {
	return func(c *fiber.Ctx) error {
		name := c.Params("name")
		if !chartNameRe.MatchString(name) {
			return c.SendStatus(http.StatusNotFound)
		}
		key := "chart/" + name
		exists, err := p.ChartExists(c.UserContext(), key)
		if err != nil {
			if logger != nil {
				logger.Printf("aichatviewer: chart existence check failed name=%q err=%v", name, err)
			}
			return c.SendStatus(http.StatusNotFound)
		}
		if !exists {
			// A real chart was never produced for this id (model fabricated it, or
			// the object expired). Serve a clean placeholder so the model's inline
			// ![chart] never renders as a broken image.
			if logger != nil {
				logger.Printf("aichatviewer: chart not found, serving placeholder name=%q", name)
			}
			c.Set(fiber.HeaderContentType, "image/svg+xml")
			return c.Status(http.StatusOK).SendString(chartUnavailableSVG)
		}
		url, err := p.PresignChart(c.UserContext(), key)
		if err != nil || url == "" {
			if logger != nil {
				logger.Printf("aichatviewer: chart presign failed name=%q err=%v", name, err)
			}
			return c.SendStatus(http.StatusNotFound)
		}
		return c.Redirect(url, http.StatusFound)
	}
}

// chartUnavailableSVG is a small inline placeholder shown when a referenced
// chart object does not exist in S3 (fabricated/expired id). Inline SVG avoids
// committing a binary asset and renders fine in an <img>.
const chartUnavailableSVG = `<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180" viewBox="0 0 320 180" role="img" aria-label="Chart unavailable"><rect width="320" height="180" rx="8" fill="#f3f4f6" stroke="#d1d5db"/><text x="160" y="92" font-family="sans-serif" font-size="15" fill="#6b7280" text-anchor="middle">Chart unavailable</text></svg>`
