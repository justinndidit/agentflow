//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/justinndidit/agentflow/internal/blob"
	"github.com/justinndidit/agentflow/internal/dbtest"
	"github.com/justinndidit/agentflow/internal/engine"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
	"github.com/justinndidit/agentflow/internal/runtime"
)

// uploadingRuntime is a worker that does what a real large-output agent does:
// PUT its bulk to the presigned destination, and return a small descriptor for
// its dependents to read.
func uploadingRuntime(t *testing.T, payload []byte) runtime.Runtime {
	t.Helper()

	return runtime.RuntimeFunc(func(ctx context.Context, req runtime.Request) (*runtime.Response, error) {
		if req.ArtifactURI == "" {
			t.Error("the worker was given no artifact destination")
			return &runtime.Response{Output: []byte(`{}`)}, nil
		}

		request, err := http.NewRequestWithContext(ctx, http.MethodPut, req.ArtifactURI, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		request.ContentLength = int64(len(payload))

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			t.Errorf("artifact upload returned %d", response.StatusCode)
		}

		// Small enough to sit inline, and what downstream template references
		// actually resolve against — the artifact itself is opaque to them.
		return &runtime.Response{
			Output: []byte(`{"bytes":` + strconv.Itoa(len(payload)) + `}`),
		}, nil
	})
}

func poolWithBlobs(f *commitFixture, rt runtime.Runtime, blobs blob.Store) *engine.Pool {
	return engine.NewPool(2, rt,
		engine.NewCommitter(repositories.NewTxManager(f.pool, nopLogger()), nopLogger(), noBackoff),
		engine.NewTemplateResolver(f.stores.TaskResultStore),
		engine.NewCachedAgents(f.stores.AgentStore),
		blobs, testLeaseTTL, nopLogger())
}

// The whole artifact path: the engine presigns a destination, the worker
// uploads directly to it, and the engine records the durable reference without
// ever having handled the bytes.
func TestPool_RecordsAnUploadedArtifact(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"produces-artifact": nil}, noBackoff)

	// Comfortably larger than anything that belongs inline.
	payload := bytes.Repeat([]byte("report bytes "), 40_000)
	store := dbtest.BlobStore(t)

	pool := poolWithBlobs(f, uploadingRuntime(t, payload), store)
	if err := pool.Handle(ctx, f.claimedNow(t)); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	task := f.task(t, "produces-artifact")
	result, err := f.stores.TaskResultStore.GetByAttempt(ctx, task.ID, task.Attempt)
	if err != nil {
		t.Fatalf("GetByAttempt failed: %v", err)
	}

	if result.ArtifactURI == nil {
		t.Fatal("no artifact_uri recorded after the worker uploaded one")
	}
	if !strings.HasPrefix(*result.ArtifactURI, "s3://") {
		t.Errorf("artifact_uri = %q, want a durable s3:// reference", *result.ArtifactURI)
	}
	// A presigned URL would stop resolving the moment it expired.
	if strings.Contains(*result.ArtifactURI, "X-Amz-Signature") {
		t.Errorf("artifact_uri = %q; a presigned URL was stored", *result.ArtifactURI)
	}

	// The inline output stayed small; the bulk went around the engine.
	if len(result.Output) > 1024 {
		t.Errorf("inline output is %d bytes; the payload went through Postgres", len(result.Output))
	}

	// And the object is really there, at the size the worker sent.
	bucket, key, err := blob.ParseURI(*result.ArtifactURI)
	if err != nil {
		t.Fatalf("ParseURI failed: %v", err)
	}
	if bucket == "" || key == "" {
		t.Fatalf("artifact_uri %q did not parse into a bucket and key", *result.ArtifactURI)
	}
	object, err := store.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if object == nil {
		t.Fatal("the recorded artifact does not exist in storage")
	}
	if object.Size != int64(len(payload)) {
		t.Errorf("stored artifact is %d bytes, want %d", object.Size, len(payload))
	}
}

// Most tasks return their output inline and never touch the presigned URL, so
// no artifact reference should be invented for them.
func TestPool_NoArtifactWhenNothingUploaded(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"inline-only": nil}, noBackoff)

	pool := poolWithBlobs(f, runtime.NewEcho(0), dbtest.BlobStore(t))
	if err := pool.Handle(ctx, f.claimedNow(t)); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	task := f.task(t, "inline-only")
	result, err := f.stores.TaskResultStore.GetByAttempt(ctx, task.ID, task.Attempt)
	if err != nil {
		t.Fatalf("GetByAttempt failed: %v", err)
	}
	if result.ArtifactURI != nil {
		t.Errorf("artifact_uri = %q, want null for a task that uploaded nothing", *result.ArtifactURI)
	}
}

// An artifact a failing attempt managed to upload is often the only evidence of
// what it was doing, so it is recorded alongside the failure rather than
// discarded with it.
func TestPool_RecordsArtifactsFromFailedAttempts(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"fails-after-upload": nil}, noBackoff)

	payload := []byte("partial work before the failure")
	store := dbtest.BlobStore(t)

	failing := runtime.RuntimeFunc(func(ctx context.Context, req runtime.Request) (*runtime.Response, error) {
		uploader := uploadingRuntime(t, payload)
		if _, err := uploader.Execute(ctx, req); err != nil {
			return nil, err
		}
		return nil, context.DeadlineExceeded
	})

	pool := poolWithBlobs(f, failing, store)
	if err := pool.Handle(ctx, f.claimedNow(t)); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	task := f.task(t, "fails-after-upload")
	results, err := f.stores.TaskResultStore.ListByTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("ListByTask failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("stored %d results, want 1", len(results))
	}
	if results[0].ArtifactURI == nil {
		t.Error("the failed attempt's artifact was discarded")
	}
}

// With storage disabled the worker gets no destination, and nothing is
// recorded — a deployment without blob storage still runs normally.
func TestPool_NoArtifactWhenStorageDisabled(t *testing.T) {
	ctx := context.Background()
	f := newCommitFixture(t, map[string][]string{"no-storage": nil}, noBackoff)

	checked := runtime.RuntimeFunc(func(_ context.Context, req runtime.Request) (*runtime.Response, error) {
		if req.ArtifactURI != "" {
			t.Errorf("ArtifactURI = %q, want empty with storage disabled", req.ArtifactURI)
		}
		return &runtime.Response{Output: []byte(`{}`)}, nil
	})

	pool := poolWithBlobs(f, checked, blob.Disabled{})
	if err := pool.Handle(ctx, f.claimedNow(t)); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if err := pool.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	task := f.task(t, "no-storage")
	result, err := f.stores.TaskResultStore.GetByAttempt(ctx, task.ID, task.Attempt)
	if err != nil {
		t.Fatalf("GetByAttempt failed: %v", err)
	}
	if result.ArtifactURI != nil {
		t.Errorf("artifact_uri = %q, want null", *result.ArtifactURI)
	}
}
