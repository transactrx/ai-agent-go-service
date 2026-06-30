package agent

import (
	"log"
	"os"
	"strings"
)

// resolveFilesBucket picks the S3 bucket name for the chart/upload cache.
// Precedence: explicit S3_FILES_BUCKET wins; otherwise the default is built
// from APP_NAME + ENVIRONMENT. If neither is usable, returns "" and lets the
// s3files initializer fail loudly.
func resolveFilesBucket(logger *log.Logger) string {
	if b := strings.TrimSpace(os.Getenv("S3_FILES_BUCKET")); b != "" {
		return b
	}
	app := strings.TrimSpace(os.Getenv("APP_NAME"))
	env := strings.TrimSpace(os.Getenv("ENVIRONMENT"))
	if app == "" || env == "" {
		return ""
	}
	bucket := strings.ToLower(app) + "-assistant-files-" + strings.ToLower(env)
	logger.Printf("boot: S3_FILES_BUCKET unset, defaulting to %q (APP_NAME + ENVIRONMENT)", bucket)
	return bucket
}
