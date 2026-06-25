package s3files

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestSanitizeFilename(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"claim.png", "claim.png"},
		{"../etc/passwd", "passwd"},
		{"", "file"},
		{"a b.png", "a_b.png"},
		{"foo.json", "foo.json"},
		{"日本語.png", "_.png"},
		{"_.png", "_.png"},
		{"path/to/file.pdf", "file.pdf"},
		{"...", "file"},
	}
	for _, tc := range cases {
		got := sanitizeFilename(tc.in)
		if got != tc.want {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// stubS3 captures PutObject calls.
type stubS3 struct {
	putCalls []struct {
		Bucket, Key, ContentType string
		Body                     []byte
	}
	putErr error
}

func (s *stubS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, _ := io.ReadAll(in.Body)
	s.putCalls = append(s.putCalls, struct {
		Bucket, Key, ContentType string
		Body                     []byte
	}{
		Bucket:      aws.ToString(in.Bucket),
		Key:         aws.ToString(in.Key),
		ContentType: aws.ToString(in.ContentType),
		Body:        body,
	})
	return &s3.PutObjectOutput{}, s.putErr
}

type stubPresign struct {
	calls   []struct{ Bucket, Key string }
	url     string
	presErr error
}

func (p *stubPresign) PresignGetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.PresignOptions)) (*presignedRequest, error) {
	p.calls = append(p.calls, struct{ Bucket, Key string }{aws.ToString(in.Bucket), aws.ToString(in.Key)})
	return &presignedRequest{URL: p.url}, p.presErr
}

func TestUploadChart_HappyPath(t *testing.T) {
	s := &stubS3{}
	p := &stubPresign{url: "https://signed.example/chart/abc.png?X-Amz-Expires=604800"}
	u := &S3Uploader{
		bucket:  "test-bucket",
		client:  s,
		presign: p,
		newUUID: func() string { return "abc" },
	}
	key, url, err := u.UploadChart(context.Background(), []byte{0x89, 0x50, 0x4E, 0x47})
	if err != nil {
		t.Fatalf("UploadChart: %v", err)
	}
	if key != "chart/abc.png" {
		t.Errorf("key = %q, want chart/abc.png", key)
	}
	if url != p.url {
		t.Errorf("url mismatch: %q", url)
	}
	if len(s.putCalls) != 1 || s.putCalls[0].ContentType != "image/png" {
		t.Fatalf("PutObject calls = %+v", s.putCalls)
	}
	if !strings.HasSuffix(p.calls[0].Key, "chart/abc.png") {
		t.Errorf("presign key = %q", p.calls[0].Key)
	}
}

func TestUploadChart_PutError(t *testing.T) {
	u := &S3Uploader{
		bucket:  "test-bucket",
		client:  &stubS3{putErr: errors.New("access denied")},
		presign: &stubPresign{},
		newUUID: func() string { return "abc" },
	}
	_, _, err := u.UploadChart(context.Background(), []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("expected wrapped put error, got %v", err)
	}
	if !strings.Contains(err.Error(), "chart/abc.png") {
		t.Fatalf("error must include key for ops: %v", err)
	}
}

func TestUploadChart_PresignError(t *testing.T) {
	u := &S3Uploader{
		bucket:  "test-bucket",
		client:  &stubS3{},
		presign: &stubPresign{presErr: errors.New("kms boom")},
		newUUID: func() string { return "abc" },
	}
	_, _, err := u.UploadChart(context.Background(), []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "kms boom") {
		t.Fatalf("expected wrapped presign error, got %v", err)
	}
}

func TestUploadAttachment_HappyPath(t *testing.T) {
	s := &stubS3{}
	p := &stubPresign{url: "https://signed.example/uploads/x.png?X-Amz-Expires=604800"}
	u := &S3Uploader{
		bucket:  "test-bucket",
		client:  s,
		presign: p,
		newUUID: func() string { return "u1" },
	}
	key, url, err := u.UploadAttachment(context.Background(), "sess123", "claim.png", "image/png", []byte("PNGBYTES"))
	if err != nil {
		t.Fatalf("UploadAttachment: %v", err)
	}
	if key != "uploads/sess123/u1-claim.png" {
		t.Errorf("key = %q, want uploads/sess123/u1-claim.png", key)
	}
	if url != p.url {
		t.Errorf("url mismatch: %q", url)
	}
	if s.putCalls[0].ContentType != "image/png" {
		t.Errorf("content-type = %q", s.putCalls[0].ContentType)
	}
	if string(s.putCalls[0].Body) != "PNGBYTES" {
		t.Errorf("body not passed through")
	}
}

func TestUploadAttachment_DefaultsForEmpties(t *testing.T) {
	s := &stubS3{}
	p := &stubPresign{url: "https://x"}
	u := &S3Uploader{bucket: "b", client: s, presign: p, newUUID: func() string { return "u" }}
	key, _, err := u.UploadAttachment(context.Background(), "", "", "", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if key != "uploads/anon/u-file" {
		t.Errorf("expected anon prefix + file fallback, got %q", key)
	}
	if s.putCalls[0].ContentType != "application/octet-stream" {
		t.Errorf("default content-type wrong: %q", s.putCalls[0].ContentType)
	}
}

func TestUploadAttachment_SanitizesFilename(t *testing.T) {
	s := &stubS3{}
	p := &stubPresign{url: "https://x"}
	u := &S3Uploader{bucket: "b", client: s, presign: p, newUUID: func() string { return "u" }}
	key, _, _ := u.UploadAttachment(context.Background(), "sid", "../../../etc/passwd", "text/plain", []byte("x"))
	if key != "uploads/sid/u-passwd" {
		t.Errorf("traversal not blocked: %q", key)
	}
}
