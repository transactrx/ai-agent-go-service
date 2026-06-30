// pkg/aichatviewer/storage_test.go
package webbridge

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalStoragePutReturnsServableURL(t *testing.T) {
	dir := t.TempDir()
	s := NewLocalStorage(dir, "/aichatviewer/upload-local")
	res, err := s.Put(context.Background(), AttachmentInput{
		AccountID: "acct1",
		Filename:  "claim.png",
		MediaType: "image/png",
		Data:      []byte("PNGDATA"),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !strings.HasPrefix(res.URL, "/aichatviewer/upload-local/") {
		t.Fatalf("URL: %s", res.URL)
	}
	if res.MediaType != "image/png" || res.Filename != "claim.png" || res.Size != int64(len("PNGDATA")) {
		t.Fatalf("unexpected metadata: %+v", res)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "acct1", "*", "claim.png"))
	if len(matches) != 1 {
		t.Fatalf("file not written: %v", matches)
	}
}

func TestS3StoragePutUsesPresignedGet(t *testing.T) {
	fakeS3 := &fakeS3Client{presignURL: "https://bucket.s3.amazonaws.com/path?sig=xyz"}
	s := NewS3StorageWithClient(fakeS3, "powerline-aichat-uploads", "us-east-1", 24*time.Hour)
	res, err := s.Put(context.Background(), AttachmentInput{
		AccountID: "acct1",
		Filename:  "x.pdf",
		MediaType: "application/pdf",
		Data:      []byte("PDF"),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.URL != fakeS3.presignURL {
		t.Fatalf("URL: %s", res.URL)
	}
	if fakeS3.lastPutKey == "" || !strings.HasPrefix(fakeS3.lastPutKey, "acct1/") {
		t.Fatalf("S3 key: %s", fakeS3.lastPutKey)
	}
	if fakeS3.lastPutMediaType != "application/pdf" || string(fakeS3.lastPutData) != "PDF" {
		t.Fatalf("captured put args: mediaType=%q data=%q", fakeS3.lastPutMediaType, fakeS3.lastPutData)
	}
}

type fakeS3Client struct {
	lastPutKey       string
	lastPutMediaType string
	lastPutData      []byte
	putObjectErr     error
	presignURL       string
}

func (f *fakeS3Client) PutObject(_ context.Context, _, key, mediaType string, data []byte) error {
	f.lastPutKey = key
	f.lastPutMediaType = mediaType
	f.lastPutData = data
	return f.putObjectErr
}

func (f *fakeS3Client) PresignGet(_ context.Context, _, _ string, _ time.Duration) (string, error) {
	return f.presignURL, nil
}
