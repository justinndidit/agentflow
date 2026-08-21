package blob

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog"
)

// S3Config addresses any S3-compatible service.
type S3Config struct {
	// Endpoint is host:port. Empty means AWS S3 itself.
	Endpoint string

	AccessKey string
	SecretKey string
	Bucket    string
	Region    string

	// UseSSL is false only for a local MinIO on plain HTTP. Anything reachable
	// off the host should have it on.
	UseSSL bool
}

// S3Store is an S3-compatible blob store.
type S3Store struct {
	client *minio.Client
	bucket string
	logger *zerolog.Logger
}

// NewS3Store connects and verifies the bucket exists.
//
// The check happens at startup rather than on first use, because a
// misconfigured bucket should stop a node from booting rather than surface as
// every task failing once it is already in the rotation.
func NewS3Store(ctx context.Context, cfg S3Config, logger *zerolog.Logger) (*S3Store, error) {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to blob storage at %s: %w", endpoint, err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket %s: %w", cfg.Bucket, err)
	}
	if !exists {
		// Not created automatically: bucket creation is a deployment decision
		// with lifecycle, retention and access-policy consequences that an
		// engine node has no business making on its own.
		return nil, fmt.Errorf("bucket %s does not exist at %s", cfg.Bucket, endpoint)
	}

	logger.Info().
		Str("func", "NewS3Store").
		Str("endpoint", endpoint).
		Str("bucket", cfg.Bucket).
		Msg("blob storage connected")

	return &S3Store{client: client, bucket: cfg.Bucket, logger: logger}, nil
}

func (s *S3Store) Name() string { return "s3" }

// PresignPut returns a URL the worker can upload to without any credentials of
// its own.
//
// ttl should not outlive the attempt's lease. A URL that stays valid longer
// lets a worker whose lease was reclaimed keep writing, and while the fence
// stops its result being recorded, it would still be spending storage under a
// key nobody will read.
func (s *S3Store) PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error) {
	presigned, err := s.client.PresignedPutObject(ctx, s.bucket, key, ttl)
	if err != nil {
		return "", fmt.Errorf("presign upload for %s: %w", key, err)
	}
	return presigned.String(), nil
}

// Stat reports what is at key, returning nil when nothing is there.
//
// A missing object is the normal case: most tasks return their output inline
// and never touch the presigned URL at all, so absence is an answer rather than
// an error.
func (s *S3Store) Stat(ctx context.Context, key string) (*Object, error) {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		var response minio.ErrorResponse
		if errors.As(err, &response) &&
			(response.Code == "NoSuchKey" || response.StatusCode == 404) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %s: %w", key, err)
	}

	return &Object{
		Key: key,
		// A durable s3:// reference rather than the presigned URL, which would
		// stop resolving the moment it expired.
		URI:  "s3://" + s.bucket + "/" + key,
		Size: info.Size,
	}, nil
}

// ParseURI splits an s3://bucket/key reference back into its parts, for tools
// that need to fetch an artifact the engine recorded.
func ParseURI(uri string) (bucket, key string, err error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("parse artifact uri %q: %w", uri, err)
	}
	if parsed.Scheme != "s3" {
		return "", "", fmt.Errorf("artifact uri %q is not an s3:// reference", uri)
	}
	if parsed.Host == "" || len(parsed.Path) <= 1 {
		return "", "", fmt.Errorf("artifact uri %q has no bucket or key", uri)
	}
	return parsed.Host, parsed.Path[1:], nil
}
