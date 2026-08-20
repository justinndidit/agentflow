// Package blob stores task outputs too large for Postgres.
//
// Postgres is the right bus for small JSON and the wrong one for the PDFs,
// embeddings and reports these workflows actually produce. Keeping large
// payloads in task_results means every claim scan drags TOAST pointers and
// inflated rows through shared buffers, so the hot scheduling path pays for
// cold data it never reads.
//
// The interface is the S3 API rather than any particular product. MinIO speaks
// it, which makes it the natural local and self-hosted option, and so do S3,
// R2, Ceph and Backblaze — moving between them is an endpoint and a credential,
// not a code change.
package blob

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Object describes a stored artifact.
type Object struct {
	// Key is the path within the bucket.
	Key string

	// URI is the durable reference recorded on task_results.artifact_uri. It is
	// deliberately not a presigned URL: those expire, and a reference that stops
	// resolving is worse than no reference at all.
	URI string

	Size int64
}

// Store hands out somewhere for a worker to write, and reports what landed.
//
// The engine never moves the bytes itself. It presigns a destination, passes it
// to the worker, and afterwards asks whether anything arrived — so a 400 MB
// model artifact never passes through the engine's memory or its database
// connection.
type Store interface {
	// PresignPut returns a URL the worker can PUT to, valid for ttl.
	PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error)

	// Stat reports what is at key, or nil if nothing is.
	Stat(ctx context.Context, key string) (*Object, error)

	// Name identifies the backing store in logs.
	Name() string
}

// ArtifactKey is where one attempt's artifact lives.
//
// Keyed per attempt, matching task_results' (task_id, attempt) primary key: a
// retry writes alongside the attempt that failed rather than over it, so the
// artifact that explains a failure survives the retry that follows it.
func ArtifactKey(workflowID, taskID uuid.UUID, attempt int) string {
	return fmt.Sprintf("workflows/%s/tasks/%s/attempts/%d/output",
		workflowID, taskID, attempt)
}

// Disabled is a Store for deployments with no blob storage configured.
//
// It presigns nothing, so AGENTFLOW_ARTIFACT_URI is absent from the worker's
// environment and a well-behaved agent returns its output inline. Tasks that
// genuinely need to write large artifacts fail on the inline size limit, which
// is the honest outcome when there is nowhere to put them.
type Disabled struct{}

func (Disabled) PresignPut(context.Context, string, time.Duration) (string, error) {
	return "", nil
}

func (Disabled) Stat(context.Context, string) (*Object, error) { return nil, nil }

func (Disabled) Name() string { return "disabled" }
