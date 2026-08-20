//go:build integration

package dbtest

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/justinndidit/agentflow/internal/blob"
)

// A throwaway MinIO, for the same reason the tests use a throwaway Postgres:
// presigned URLs, multipart semantics and the exact shape of a not-found error
// are properties of an S3 implementation, and a fake would only assert that the
// code calls the methods it was written to call.

const (
	blobImage     = "minio/minio:latest"
	blobUser      = "minioadmin"
	blobPassword  = "minioadmin"
	defaultBucket = "agentflow-artifacts"
)

var (
	blobOnce     sync.Once
	blobInstance *blobService
	blobStartErr error
)

type blobService struct {
	endpoint string
}

// BlobEndpoint is the host:port of the shared test MinIO.
func BlobEndpoint(t *testing.T) string {
	t.Helper()
	return startBlob(t).endpoint
}

// BlobConfig points at the shared MinIO with a bucket created for this test.
//
// Each caller gets its own bucket, so tests cannot see one another's objects
// without paying for a container per test.
func BlobConfig(t *testing.T) blob.S3Config {
	t.Helper()

	service := startBlob(t)
	bucket := fmt.Sprintf("test-%d", time.Now().UnixNano())

	client, err := minio.New(service.endpoint, &minio.Options{
		Creds:  miniocreds.NewStaticV4(blobUser, blobPassword, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("failed to connect to test MinIO: %v", err)
	}
	if err := client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("failed to create bucket %s: %v", bucket, err)
	}

	return blob.S3Config{
		Endpoint:  service.endpoint,
		AccessKey: blobUser,
		SecretKey: blobPassword,
		Bucket:    bucket,
		Region:    "us-east-1",
		UseSSL:    false,
	}
}

// BlobStore returns a store backed by a fresh bucket on the shared MinIO.
func BlobStore(t *testing.T) *blob.S3Store {
	t.Helper()

	logger := zerolog.Nop()
	store, err := blob.NewS3Store(context.Background(), BlobConfig(t), &logger)
	if err != nil {
		t.Fatalf("failed to build the test blob store: %v", err)
	}
	return store
}

func startBlob(t *testing.T) *blobService {
	t.Helper()

	blobOnce.Do(func() {
		blobInstance, blobStartErr = launchBlob()
	})
	if blobStartErr != nil {
		t.Fatalf("failed to start test MinIO: %v", blobStartErr)
	}
	return blobInstance
}

func launchBlob() (*blobService, error) {
	// Not a test's context: the container outlives whichever test happened to
	// start it, and Ryuk tears it down when the run ends.
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        blobImage,
			Cmd:          []string{"server", "/data"},
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     blobUser,
				"MINIO_ROOT_PASSWORD": blobPassword,
			},
			WaitingFor: wait.ForHTTP("/minio/health/live").
				WithPort("9000/tcp").
				WithStartupTimeout(60 * time.Second),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("run minio container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("container host: %w", err)
	}
	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return nil, fmt.Errorf("container port: %w", err)
	}
	number, err := strconv.Atoi(port.Port())
	if err != nil {
		return nil, fmt.Errorf("parse container port %q: %w", port.Port(), err)
	}

	return &blobService{endpoint: fmt.Sprintf("%s:%d", host, number)}, nil
}
