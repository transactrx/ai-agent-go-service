// pkg/aichatviewer/upload_backend.go
package webbridge

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// newAttachmentStorage picks a backend from env vars:
//   - POWERLINE_AICHAT_UPLOAD_DIR=<path>     → LocalStorage at that path.
//     When local storage is selected, a background janitor sweeps the dir
//     every hour and deletes files older than the TTL configured by
//     POWERLINE_AICHAT_UPLOAD_TTL_HOURS (default 24). The chat backend
//     uploads bytes to S3 the moment the agent processes a message, so
//     the local copy is transient by design.
//   - POWERLINE_AICHAT_UPLOAD_BUCKET=<name>  → S3Storage (region from
//     POWERLINE_AICHAT_UPLOAD_REGION; required when bucket is set)
//
// Returns nil when neither is set — the upload route is not registered in
// that case (uploads are an opt-in feature).
func newAttachmentStorage(ctx context.Context, logger *log.Logger) AttachmentStorage {
	if dir := os.Getenv("POWERLINE_AICHAT_UPLOAD_DIR"); dir != "" {
		ttl := localTTLFromEnv()
		logger.Printf("aichatviewer: upload storage = local-fs at %s (janitor ttl=%s)", dir, ttl)
		go NewLocalJanitor(dir, ttl, time.Hour, logger).Start(ctx)
		return NewLocalStorage(dir, "/aichatviewer/upload-local")
	}
	bucket := os.Getenv("POWERLINE_AICHAT_UPLOAD_BUCKET")
	if bucket == "" {
		logger.Print("aichatviewer: upload storage = none (neither POWERLINE_AICHAT_UPLOAD_DIR nor POWERLINE_AICHAT_UPLOAD_BUCKET set); upload route disabled")
		return nil
	}
	region := os.Getenv("POWERLINE_AICHAT_UPLOAD_REGION")
	if region == "" {
		logger.Print("aichatviewer: POWERLINE_AICHAT_UPLOAD_REGION required when bucket is set; upload route disabled")
		return nil
	}
	client, err := NewAwsS3Client(region)
	if err != nil {
		logger.Printf("aichatviewer: AWS S3 client init failed: %v; upload route disabled", err)
		return nil
	}
	logger.Printf("aichatviewer: upload storage = S3 bucket=%s region=%s", bucket, region)
	return NewS3StorageWithClient(client, bucket, region, 24*time.Hour)
}

// NewAwsS3Client is a placeholder for the production AWS-SDK-backed client.
// Wiring is intentionally deferred: aws-sdk-go-v2 isn't yet a dep of this
// repo, so for now the upload route only works against POWERLINE_AICHAT_UPLOAD_DIR.
// To enable S3: add aws-sdk-go-v2 + s3 + presign-v4 packages to go.mod and
// flesh out PutObject/PresignGet against them.
func NewAwsS3Client(_ string) (S3Client, error) {
	return nil, fmt.Errorf("aichatviewer: AWS S3 client not yet wired; set POWERLINE_AICHAT_UPLOAD_DIR for local FS or add aws-sdk-go-v2 to go.mod")
}

// localTTLFromEnv reads POWERLINE_AICHAT_UPLOAD_TTL_HOURS and converts to a
// Duration. Empty/invalid values fall back to DefaultLocalTTL.
func localTTLFromEnv() time.Duration {
	raw := os.Getenv("POWERLINE_AICHAT_UPLOAD_TTL_HOURS")
	if raw == "" {
		return DefaultLocalTTL
	}
	h, err := strconv.Atoi(raw)
	if err != nil || h <= 0 {
		return DefaultLocalTTL
	}
	return time.Duration(h) * time.Hour
}
