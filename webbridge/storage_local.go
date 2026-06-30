// pkg/aichatviewer/storage_local.go
package webbridge

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// LocalStorage writes to a directory tree on disk and returns same-origin
// URLs served by a separate handler (UploadLocalGet, added when wiring routes
// in B6). Intended for dev only.
type LocalStorage struct {
	root      string
	urlPrefix string // e.g. "/aichatviewer/upload-local"
}

// NewLocalStorage builds a LocalStorage rooted at the given dir; URLs are
// emitted with the given prefix.
func NewLocalStorage(root, urlPrefix string) *LocalStorage {
	return &LocalStorage{root: root, urlPrefix: urlPrefix}
}

// Put writes bytes to <root>/<accountId>/<uuid>/<filename> and returns a URL
// of shape <urlPrefix>/<uuid>/<filename>. AccountID is NOT in the URL to
// avoid leaking account ids to the client; the GET handler (UploadLocalGet)
// re-derives the account from the session.
func (s *LocalStorage) Put(_ context.Context, in AttachmentInput) (AttachmentResult, error) {
	id := uuid.NewString()
	dir := filepath.Join(s.root, in.AccountID, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return AttachmentResult{}, fmt.Errorf("local storage mkdir: %w", err)
	}
	path := filepath.Join(dir, in.Filename)
	if err := os.WriteFile(path, in.Data, 0o644); err != nil {
		return AttachmentResult{}, fmt.Errorf("local storage write: %w", err)
	}
	return AttachmentResult{
		URL:       fmt.Sprintf("%s/%s/%s", s.urlPrefix, id, in.Filename),
		MediaType: in.MediaType,
		Filename:  in.Filename,
		Size:      int64(len(in.Data)),
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}, nil
}
