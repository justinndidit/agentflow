package blob

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestArtifactKey_IsStableAndScoped(t *testing.T) {
	workflowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	taskID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	key := ArtifactKey(workflowID, taskID, 3)

	if key != ArtifactKey(workflowID, taskID, 3) {
		t.Error("ArtifactKey is not deterministic")
	}
	// Scoped by workflow then task, so an operator can list everything one run
	// produced with a prefix query.
	if !strings.HasPrefix(key, "workflows/"+workflowID.String()+"/") {
		t.Errorf("key = %q, want it prefixed by its workflow", key)
	}
	if !strings.Contains(key, taskID.String()) {
		t.Errorf("key = %q, want it to name its task", key)
	}
	// Per attempt, matching task_results' (task_id, attempt) key.
	if ArtifactKey(workflowID, taskID, 1) == ArtifactKey(workflowID, taskID, 2) {
		t.Error("two attempts share a key; a retry would overwrite the evidence of the failure")
	}
}

func TestParseURI(t *testing.T) {
	bucket, key, err := ParseURI("s3://agentflow-artifacts/workflows/a/tasks/b/attempts/1/output")
	if err != nil {
		t.Fatalf("ParseURI failed: %v", err)
	}
	if bucket != "agentflow-artifacts" {
		t.Errorf("bucket = %q, want agentflow-artifacts", bucket)
	}
	if key != "workflows/a/tasks/b/attempts/1/output" {
		t.Errorf("key = %q", key)
	}
}

func TestParseURI_Rejects(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{"wrong scheme", "https://example.com/thing"},
		{"no scheme", "agentflow-artifacts/key"},
		{"no key", "s3://bucket"},
		{"no bucket", "s3:///key"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := ParseURI(test.uri); err == nil {
				t.Errorf("ParseURI(%q) succeeded, want an error", test.uri)
			}
		})
	}
}

// A round trip through ParseURI has to recover exactly what the key was, or an
// artifact recorded on a task cannot be fetched again.
func TestParseURI_RoundTripsAnArtifactKey(t *testing.T) {
	workflowID, taskID := uuid.New(), uuid.New()
	key := ArtifactKey(workflowID, taskID, 2)

	gotBucket, gotKey, err := ParseURI("s3://my-bucket/" + key)
	if err != nil {
		t.Fatalf("ParseURI failed: %v", err)
	}
	if gotBucket != "my-bucket" {
		t.Errorf("bucket = %q", gotBucket)
	}
	if gotKey != key {
		t.Errorf("key = %q, want %q", gotKey, key)
	}
}
