// pkg/aichatviewer/chart_presigner_s3.go
package webbridge

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// chartPresignTTL is the lifetime of a presigned chart GET URL. Short on
// purpose: the browser follows the 302 immediately, so a leaked URL is only
// usable for a few minutes (security control #4).
const chartPresignTTL = 15 * time.Minute

// s3ChartPresigner is the real ChartPresigner backed by aws-sdk-go-v2.
type s3ChartPresigner struct {
	bucket  string
	client  *s3.Client
	presign *s3.PresignClient
}

// newS3ChartPresigner builds a presigner from ambient AWS credentials (the ECS
// task role). region is where the chart bucket lives (us-east-1).
func newS3ChartPresigner(ctx context.Context, bucket, region string) (*s3ChartPresigner, error) {
	if strings.TrimSpace(bucket) == "" {
		return nil, fmt.Errorf("aichatviewer: chart bucket name is empty")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("aichatviewer: load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	return &s3ChartPresigner{
		bucket:  bucket,
		client:  client,
		presign: s3.NewPresignClient(client, s3.WithPresignExpires(chartPresignTTL)),
	}, nil
}

// ChartExists reports whether the object exists in S3. A HeadObject 404
// (types.NotFound) is the clean "fabricated or expired id" case and is returned
// as (false, nil) — not an error. Any other failure returns (false, err).
func (p *s3ChartPresigner) ChartExists(ctx context.Context, key string) (bool, error) {
	_, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// PresignChart presigns GET on chart/<uuid>.png. key is the fully-built object
// key ("chart/<uuid>.png"); the caller has already validated the uuid and
// confirmed the object exists.
func (p *s3ChartPresigner) PresignChart(ctx context.Context, key string) (string, error) {
	req, err := p.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("aichatviewer: presign %s/%s: %w", p.bucket, key, err)
	}
	return req.URL, nil
}
