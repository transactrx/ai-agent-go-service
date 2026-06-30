// Package s3files provides a thin AWS S3 wrapper used by the QuickChart tool
// and the executor's attachment pipeline. PutObject + presigned GET, scoped
// to chart/ and uploads/ key prefixes. Constructed once at boot and injected
// via the engine Hosts map under the name "s3files".
package s3files

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// PresignTTL is the lifetime of every presigned GET URL we hand out. 7 days
// is the AWS sigv4 maximum.
const PresignTTL = 7 * 24 * time.Hour

// Uploader is what the QuickChart tool and the executor call. Both methods
// return a presigned GET URL (TTL = PresignTTL) along with the S3 key for
// observability/logging.
type Uploader interface {
	UploadChart(ctx context.Context, png []byte) (key, url string, err error)
	UploadAttachment(ctx context.Context, sessionID, filename, mediaType string, data []byte) (key, url string, err error)
}

// s3API is the subset of the SDK v2 S3 client we use. Lets tests substitute
// a stub without pulling in AWS plumbing.
type s3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// presignAPI is the subset of the v2 presign client we use.
type presignAPI interface {
	PresignGetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.PresignOptions)) (*presignedRequest, error)
}

// presignedRequest mirrors *v4.PresignedHTTPRequest so the stub doesn't need
// to import the SDK presign type. The real client returns *v4.PresignedHTTPRequest;
// we adapt in New() via presignAdapter below.
type presignedRequest struct{ URL string }

// S3Uploader is the real Uploader backed by AWS SDK v2.
type S3Uploader struct {
	bucket  string
	client  s3API
	presign presignAPI
	newUUID func() string // injectable for tests; defaults to uuid.NewString
}

// New constructs an S3Uploader from the ambient AWS config and the
// S3_FILES_BUCKET env var. Returns an error (not a panic) if the env var is
// empty so the caller can decide whether to fail-fast or proceed without S3.
func New(ctx context.Context, bucket string) (*S3Uploader, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, errors.New("s3files: bucket name is empty (set S3_FILES_BUCKET)")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("s3files: load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	presign := s3.NewPresignClient(client, s3.WithPresignExpires(PresignTTL))
	return &S3Uploader{
		bucket:  bucket,
		client:  client,
		presign: presignAdapter{p: presign},
		newUUID: uuid.NewString,
	}, nil
}

// presignAdapter bridges the real v2 presign client (which returns
// *v4.PresignedHTTPRequest) to our presignAPI interface (which returns the
// minimal presignedRequest the tests use).
type presignAdapter struct{ p *s3.PresignClient }

func (a presignAdapter) PresignGetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.PresignOptions)) (*presignedRequest, error) {
	req, err := a.p.PresignGetObject(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return &presignedRequest{URL: req.URL}, nil
}

// Bucket returns the configured bucket name. Useful for logging.
func (u *S3Uploader) Bucket() string { return u.bucket }

// UploadChart writes png to chart/<uuid>.png and returns a presigned GET URL.
func (u *S3Uploader) UploadChart(ctx context.Context, png []byte) (string, string, error) {
	key := "chart/" + u.newUUID() + ".png"
	return u.putAndPresign(ctx, key, "image/png", png)
}

// UploadAttachment writes data to uploads/<sessionID>/<uuid>-<sanitized>.
// mediaType becomes the object's Content-Type. sessionID may be empty
// (becomes "anon/") — the executor always passes a value but we don't crash.
func (u *S3Uploader) UploadAttachment(ctx context.Context, sessionID, filename, mediaType string, data []byte) (string, string, error) {
	sid := strings.TrimSpace(sessionID)
	if sid == "" {
		sid = "anon"
	}
	if strings.TrimSpace(mediaType) == "" {
		mediaType = "application/octet-stream"
	}
	key := "uploads/" + sid + "/" + u.newUUID() + "-" + sanitizeFilename(filename)
	return u.putAndPresign(ctx, key, mediaType, data)
}

func (u *S3Uploader) putAndPresign(ctx context.Context, key, contentType string, body []byte) (string, string, error) {
	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(u.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
		Body:        bytes.NewReader(body),
	})
	if err != nil {
		return "", "", fmt.Errorf("s3files: put %s/%s: %w", u.bucket, key, err)
	}
	req, err := u.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", "", fmt.Errorf("s3files: presign %s/%s: %w", u.bucket, key, err)
	}
	return key, req.URL, nil
}

// filenameAllowed is the safe character set after sanitization: ASCII letters,
// digits, dot, underscore, dash. Anything else collapses to '_'.
var filenameAllowed = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// sanitizeFilename strips path components, replaces disallowed runs with a
// single '_', and falls back to "file" if nothing usable remains.
func sanitizeFilename(s string) string {
	s = filepath.Base(s)
	if s == "." || s == ".." || s == "/" {
		return "file"
	}
	s = filenameAllowed.ReplaceAllString(s, "_")
	s = strings.Trim(s, ".")
	if s == "" || s == "_" {
		return "file"
	}
	return s
}
