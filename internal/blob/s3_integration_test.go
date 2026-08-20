//go:build integration

package blob_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/justinndidit/agentflow/internal/blob"
	"github.com/justinndidit/agentflow/internal/dbtest"
)

func nopLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

// upload does what a worker does: PUT to the presigned URL with no credentials
// of its own.
func upload(t *testing.T, presigned string, body []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodPut, presigned, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to build the upload request: %v", err)
	}
	req.ContentLength = int64(len(body))

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response
}

// The whole point of presigning: a worker uploads directly, holding no
// credentials, and the engine never sees the bytes.
func TestS3Store_PresignedUploadNeedsNoCredentials(t *testing.T) {
	ctx := context.Background()
	store := dbtest.BlobStore(t)

	key := blob.ArtifactKey(uuid.New(), uuid.New(), 1)
	presigned, err := store.PresignPut(ctx, key, 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignPut failed: %v", err)
	}
	if presigned == "" {
		t.Fatal("PresignPut returned an empty URL")
	}

	payload := []byte(strings.Repeat("artifact bytes ", 100))
	if response := upload(t, presigned, payload); response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("upload returned %d: %s", response.StatusCode, strings.ReplaceAll(string(body), "\n", " "))
	}

	object, err := store.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if object == nil {
		t.Fatal("Stat found nothing after a successful upload")
	}
	if object.Size != int64(len(payload)) {
		t.Errorf("Size = %d, want %d", object.Size, len(payload))
	}
	// A durable reference, not the presigned URL, which would stop resolving.
	if !strings.HasPrefix(object.URI, "s3://") {
		t.Errorf("URI = %q, want an s3:// reference", object.URI)
	}
	if strings.Contains(object.URI, "X-Amz-Signature") {
		t.Errorf("URI = %q; a presigned URL was recorded instead of a durable reference", object.URI)
	}
}

// Most tasks return their output inline and never touch the presigned URL, so
// absence has to be an answer rather than an error.
func TestS3Store_StatMissingObject(t *testing.T) {
	ctx := context.Background()
	store := dbtest.BlobStore(t)

	object, err := store.Stat(ctx, blob.ArtifactKey(uuid.New(), uuid.New(), 1))
	if err != nil {
		t.Fatalf("Stat of a missing object returned an error: %v", err)
	}
	if object != nil {
		t.Errorf("Stat = %+v, want nil for an object that was never uploaded", object)
	}
}

// A URL that outlives its lease lets a reclaimed worker keep writing, spending
// storage under a key nobody will read.
func TestS3Store_PresignedURLExpires(t *testing.T) {
	ctx := context.Background()
	store := dbtest.BlobStore(t)

	key := blob.ArtifactKey(uuid.New(), uuid.New(), 1)
	presigned, err := store.PresignPut(ctx, key, 1*time.Second)
	if err != nil {
		t.Fatalf("PresignPut failed: %v", err)
	}

	time.Sleep(3 * time.Second)

	response := upload(t, presigned, []byte("too late"))
	if response.StatusCode == http.StatusOK {
		t.Error("an expired presigned URL still accepted an upload")
	}
}

// Keyed per attempt, matching task_results' (task_id, attempt) primary key, so
// a retry writes alongside the attempt that failed rather than over it.
func TestArtifactKey_IsPerAttempt(t *testing.T) {
	ctx := context.Background()
	store := dbtest.BlobStore(t)

	workflowID, taskID := uuid.New(), uuid.New()
	first := blob.ArtifactKey(workflowID, taskID, 1)
	second := blob.ArtifactKey(workflowID, taskID, 2)

	if first == second {
		t.Fatal("two attempts of the same task share an artifact key")
	}

	for key, payload := range map[string]string{first: "attempt one", second: "attempt two"} {
		presigned, err := store.PresignPut(ctx, key, 5*time.Minute)
		if err != nil {
			t.Fatalf("PresignPut failed: %v", err)
		}
		if response := upload(t, presigned, []byte(payload)); response.StatusCode != http.StatusOK {
			t.Fatalf("upload of %s returned %d", key, response.StatusCode)
		}
	}

	// Both survive: the failed attempt's artifact is often the only evidence of
	// what went wrong.
	for _, key := range []string{first, second} {
		object, err := store.Stat(ctx, key)
		if err != nil {
			t.Fatalf("Stat failed: %v", err)
		}
		if object == nil {
			t.Errorf("attempt artifact %s is missing", key)
		}
	}
}

// A misconfigured bucket should stop a node booting rather than surface as
// every task failing once it is already taking work.
func TestNewS3Store_MissingBucketFailsAtStartup(t *testing.T) {
	cfg := dbtest.BlobConfig(t)
	cfg.Bucket = "definitely-not-created-" + uuid.New().String()[:8]

	if _, err := blob.NewS3Store(context.Background(), cfg, nopLogger()); err == nil {
		t.Fatal("expected a missing bucket to fail at construction")
	}
}

func TestDisabled_PresignsNothing(t *testing.T) {
	ctx := context.Background()
	var store blob.Store = blob.Disabled{}

	url, err := store.PresignPut(ctx, "any/key", time.Minute)
	if err != nil {
		t.Fatalf("Disabled.PresignPut returned an error: %v", err)
	}
	// Empty, so the worker's environment carries no AGENTFLOW_ARTIFACT_URI and
	// a well-behaved agent returns its output inline.
	if url != "" {
		t.Errorf("Disabled.PresignPut = %q, want empty", url)
	}

	object, err := store.Stat(ctx, "any/key")
	if err != nil || object != nil {
		t.Errorf("Disabled.Stat = %v, %v; want nil, nil", object, err)
	}
}
