// pkg/aichatviewer/storage_s3.go
package webbridge

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// S3Client is the narrow interface S3Storage needs. The concrete production
// implementation wraps aws-sdk-go-v2's s3 + presign clients; tests use a fake.
// Kept minimal so tests don't drag in the AWS SDK.
type S3Client interface {
	PutObject(ctx context.Context, bucket, key, mediaType string, data []byte) error
	PresignGet(ctx context.Context, bucket, key string, ttl time.Duration) (string, error)
}

// S3Storage uploads via PutObject and returns presigned-GET URLs the client
// fetches directly (no WebApp passthrough). 24h default TTL.
type S3Storage struct {
	client     S3Client
	bucket     string
	region     string
	presignTTL time.Duration
}

// NewS3StorageWithClient is the test-friendly constructor (accepts any
// S3Client). Production code calls NewAwsS3Client(region) to get a real
// S3Client (added in B6 alongside route wiring).
func NewS3StorageWithClient(c S3Client, bucket, region string, ttl time.Duration) *S3Storage {
	return &S3Storage{client: c, bucket: bucket, region: region, presignTTL: ttl}
}

func (s *S3Storage) Put(ctx context.Context, in AttachmentInput) (AttachmentResult, error) {
	id := uuid.NewString()
	key := fmt.Sprintf("%s/%s/%s", in.AccountID, id, in.Filename)
	if err := s.client.PutObject(ctx, s.bucket, key, in.MediaType, in.Data); err != nil {
		return AttachmentResult{}, fmt.Errorf("s3 put: %w", err)
	}
	url, err := s.client.PresignGet(ctx, s.bucket, key, s.presignTTL)
	if err != nil {
		return AttachmentResult{}, fmt.Errorf("s3 presign: %w", err)
	}
	return AttachmentResult{
		URL:       url,
		MediaType: in.MediaType,
		Filename:  in.Filename,
		Size:      int64(len(in.Data)),
		ExpiresAt: time.Now().Add(s.presignTTL),
	}, nil
}
