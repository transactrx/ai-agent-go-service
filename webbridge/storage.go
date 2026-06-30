// pkg/aichatviewer/storage.go
package webbridge

import (
	"context"
	"time"
)

// AttachmentInput is what UploadAttachment hands to the storage backend.
type AttachmentInput struct {
	AccountID string
	Filename  string
	MediaType string
	Data      []byte
}

// AttachmentResult is what UploadAttachment returns to the client. The URL
// is what the React island then references in subsequent user_message
// attachments[].url (over the WS).
type AttachmentResult struct {
	URL       string    `json:"url"`
	MediaType string    `json:"mediaType"`
	Filename  string    `json:"filename"`
	Size      int64     `json:"size"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// AttachmentStorage is the backend abstraction. Implementations: local FS
// for dev, S3 for prod (added in B4).
type AttachmentStorage interface {
	// Put stores the bytes and returns a URL the client can fetch. For S3 the
	// URL is presigned and points directly at the bucket. For local FS the
	// URL is same-origin and served by UploadLocalGet.
	Put(ctx context.Context, in AttachmentInput) (AttachmentResult, error)
}
