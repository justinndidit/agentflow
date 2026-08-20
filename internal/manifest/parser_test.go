package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// validManifest is the baseline every parser test starts from: a two task
// workflow that exercises a dependency edge and a template reference across it.
// Tests mutate a copy of it rather than each spelling out a full manifest, so a
// failure points at the one rule under test.
const validManifest = `
name: test-workflow
namespace: default
workflow_version: 1
workers: 4
timeout: 2m
max_tokens: 1000

tasks:
  - task_key: fetch
    agent: research-agent
    priority: 4
    max_retries: 3
    timeout: 300
    input:
      roles: ["backend engineer"]

  - task_key: rank
    agent: matching-agent
    priority: 2
    max_retries: 1
    timeout: 120
    depends_on:
      - fetch
    input:
      jobs: "{{ tasks.fetch.output.jobs }}"
`

func TestParseBytes_Valid(t *testing.T) {
	workflow, err := ParseBytes([]byte(validManifest))
	if err != nil {
		t.Fatalf("expected valid manifest to parse, got: %v", err)
	}

	if workflow.WorkflowName != "test-workflow" {
		t.Errorf("WorkflowName = %q, want %q", workflow.WorkflowName, "test-workflow")
	}
	if workflow.WorkflowNameSpace != "default" {
		t.Errorf("WorkflowNameSpace = %q, want %q", workflow.WorkflowNameSpace, "default")
	}
	if workflow.Version != 1 {
		t.Errorf("Version = %d, want 1", workflow.Version)
	}
	if workflow.DefaultWorkerCount != 4 {
		t.Errorf("DefaultWorkerCount = %d, want 4", workflow.DefaultWorkerCount)
	}
	if workflow.DefaultTimeout != "2m" {
		t.Errorf("DefaultTimeout = %q, want %q", workflow.DefaultTimeout, "2m")
	}
	if workflow.MaxTokensPerRun != 1000 {
		t.Errorf("MaxTokensPerRun = %d, want 1000", workflow.MaxTokensPerRun)
	}
	if len(workflow.Tasks) != 2 {
		t.Fatalf("len(Tasks) = %d, want 2", len(workflow.Tasks))
	}

	// Task ordering matters: the submit path converts tasks positionally, so a
	// reordering decode would silently mispair keys and agents.
	fetch := workflow.Tasks[0]
	if fetch.TaskKey != "fetch" {
		t.Errorf("Tasks[0].TaskKey = %q, want %q", fetch.TaskKey, "fetch")
	}
	if fetch.AgentName != "research-agent" {
		t.Errorf("Tasks[0].AgentName = %q, want %q", fetch.AgentName, "research-agent")
	}
	if fetch.Priority != 4 {
		t.Errorf("Tasks[0].Priority = %d, want 4", fetch.Priority)
	}
	if fetch.MaxRetries != 3 {
		t.Errorf("Tasks[0].MaxRetries = %d, want 3", fetch.MaxRetries)
	}
	if fetch.TimeoutInSeconds != 300 {
		t.Errorf("Tasks[0].TimeoutInSeconds = %d, want 300", fetch.TimeoutInSeconds)
	}
	if len(fetch.DependsOn) != 0 {
		t.Errorf("Tasks[0].DependsOn = %v, want empty", fetch.DependsOn)
	}

	rank := workflow.Tasks[1]
	if len(rank.DependsOn) != 1 || rank.DependsOn[0] != "fetch" {
		t.Errorf("Tasks[1].DependsOn = %v, want [fetch]", rank.DependsOn)
	}

	// The template must survive parsing unresolved — upstream output does not
	// exist until dispatch.
	jobs, ok := rank.Input["jobs"].(string)
	if !ok {
		t.Fatalf("Tasks[1].Input[jobs] = %T, want string", rank.Input["jobs"])
	}
	if jobs != "{{ tasks.fetch.output.jobs }}" {
		t.Errorf("Tasks[1].Input[jobs] = %q, want the template unresolved", jobs)
	}
}

// A dependency may be declared before the task it points at appears in the
// file. The unknown-dependency check runs in a second pass for this reason.
func TestParseBytes_ForwardDependency(t *testing.T) {
	manifest := `
name: forward
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: second
    agent: a
    max_retries: 0
    timeout: 10
    depends_on:
      - first
  - task_key: first
    agent: a
    max_retries: 0
    timeout: 10
`
	if _, err := ParseBytes([]byte(manifest)); err != nil {
		t.Fatalf("a dependency on a later-declared task should parse, got: %v", err)
	}
}

func TestParseBytes_Invalid(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
		wantErr  string
	}{
		{
			name:     "malformed yaml",
			manifest: "name: [unclosed",
			wantErr:  "did not find expected",
		},
		{
			name: "missing name",
			manifest: `
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    agent: agent-a
    max_retries: 0
    timeout: 10
`,
			wantErr: "WorkflowName",
		},
		{
			name: "missing namespace",
			manifest: `
name: w
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    agent: agent-a
    max_retries: 0
    timeout: 10
`,
			wantErr: "WorkflowNameSpace",
		},
		{
			name: "no tasks",
			manifest: `
name: w
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
`,
			wantErr: "Tasks",
		},
		{
			name: "task missing agent",
			manifest: `
name: w
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    max_retries: 0
    timeout: 10
`,
			wantErr: "AgentName",
		},
		{
			name: "task missing timeout",
			manifest: `
name: w
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    agent: agent-a
    max_retries: 0
`,
			wantErr: "TimeoutInSeconds",
		},
		{
			name: "negative max_retries",
			manifest: `
name: w
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    agent: agent-a
    max_retries: -1
    timeout: 10
`,
			wantErr: "MaxRetries",
		},
		{
			name: "duplicate task key",
			manifest: `
name: w
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    agent: agent-a
    max_retries: 0
    timeout: 10
  - task_key: a
    agent: agent-b
    max_retries: 0
    timeout: 10
`,
			wantErr: "duplicate task id: a",
		},
		{
			name: "unknown dependency",
			manifest: `
name: w
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: a
    agent: agent-a
    max_retries: 0
    timeout: 10
    depends_on:
      - ghost
`,
			wantErr: "depends on unknown task with task key ghost",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workflow, err := ParseBytes([]byte(test.manifest))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", test.wantErr)
			}
			if workflow != nil {
				t.Errorf("expected nil workflow on error, got %+v", workflow)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, test.wantErr)
			}
		})
	}
}

// Parse is a thin wrapper over ParseBytes; these cover only what it adds, which
// is locating and reading the file.
func TestParse_ReadsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow.yml")
	if err := os.WriteFile(path, []byte(validManifest), 0o600); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	workflow, data, err := Parse(path)
	if err != nil {
		t.Fatalf("expected manifest to parse, got: %v", err)
	}
	if workflow.WorkflowName != "test-workflow" {
		t.Errorf("WorkflowName = %q, want %q", workflow.WorkflowName, "test-workflow")
	}

	// The raw bytes are stored verbatim as the workflow definition, so they must
	// come back unmodified rather than re-encoded from the decoded struct.
	if string(data) != validManifest {
		t.Errorf("returned bytes do not match the file contents")
	}
}

func TestParse_MissingFile(t *testing.T) {
	workflow, data, err := Parse(filepath.Join(t.TempDir(), "does-not-exist.yml"))
	if err == nil {
		t.Fatal("expected an error for a missing file, got nil")
	}
	if workflow != nil || data != nil {
		t.Errorf("expected nil workflow and nil data, got %v and %v", workflow, data)
	}
}

// Parse resolves a relative path against the working directory.
func TestParse_RelativePath(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workflow.yml"), []byte(validManifest), 0o600); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}
	t.Chdir(dir)

	if _, _, err := Parse("workflow.yml"); err != nil {
		t.Fatalf("expected a relative path to resolve, got: %v", err)
	}
}

// The manifest shipped with the repo is the one the getting-started path runs,
// so a validation rule that rejects it is a breaking change.
func TestParse_ExampleWorkflowIsValid(t *testing.T) {
	workflow, _, err := Parse(filepath.Join("..", "..", "example-workflow.yml"))
	if err != nil {
		t.Fatalf("example-workflow.yml must stay valid, got: %v", err)
	}
	if len(workflow.Tasks) == 0 {
		t.Error("expected example-workflow.yml to declare tasks")
	}
}
