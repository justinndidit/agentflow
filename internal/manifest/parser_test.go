package manifest

import (
	"os"
	"strings"
	"testing"
)

func SetupTest(t *testing.T) (func(), *os.File) {
	tempFile, err := os.CreateTemp(".", "test_workflow.yml")
	if err != nil {
		t.Fatalf("expected temporary file but got error: %s", err)
	}

	return func() {
		if err := tempFile.Close(); err != nil {
			t.Fatalf("failed to close temp file: %v", err)
		}
		os.Remove(tempFile.Name())
	}, tempFile
}

func TestParse_Valid(t *testing.T) {
	data := strings.Join([]string{
		"name: \"test-workflow\"",
		"workers: 2",
		"timeout: \"30s\"",
		"max_tokens: 1000",
		"tasks:",
		"  - id: \"task1\"",
		"    agent: \"agent1\"",
		"    input: {}",
		"    depends_on: []",
	}, "\n")

	tearDown, tempFile := SetupTest(t)
	defer tearDown()

	if _, err := tempFile.WriteString(data); err != nil {
		t.Fatalf("failed to write YAML: %v", err)
	}

	wf, err := Parse(tempFile.Name())
	if err != nil {
		t.Fatalf("Parse failed unexpectedly: %v %s", err, tempFile.Name())
	}

	if wf.WorkflowName != "test-workflow" {
		t.Errorf("WorkflowName: expected 'test-workflow', got %q", wf.WorkflowName)
	}
	if wf.DefaultWorkerCount != 2 {
		t.Errorf("DefaultWorkerCount: expected 2, got %d", wf.DefaultWorkerCount)
	}
	if wf.DefaultTimeout != "30s" {
		t.Errorf("DefaultTimeout: expected '30s', got %q", wf.DefaultTimeout)
	}
	if wf.MaxTokensPerRun != 1000 {
		t.Errorf("MaxTokensPerRun: expected 1000, got %d", wf.MaxTokensPerRun)
	}
	if len(wf.Tasks) != 1 {
		t.Errorf("Tasks length: expected 1, got %d", len(wf.Tasks))
	}
	if wf.Tasks[0].TaskKey != "task1" {
		t.Errorf("Task ID: expected 'task1', got %q", wf.Tasks[0].TaskKey)
	}
	if wf.Tasks[0].AgentID != "agent1" {
		t.Errorf("AgentID: expected 'agent1', got %q", wf.Tasks[0].AgentID)
	}
}

func TestParse_InvalidMissingName(t *testing.T) {
	data := `workers: 2
									timeout: "30s"
									max_tokens: 1000
									tasks:
										- id: task1
	  									agent: agent1`

	tearDown, tempFile := SetupTest(t)
	defer tearDown()

	if _, err := tempFile.WriteString(data); err != nil {
		t.Fatalf("failed to write YAML: %v", err)
	}
	_, err := Parse(tempFile.Name())
	if err == nil {
		t.Fatalf("expected Parse to return error, got nil")
	}
}

func TestParse_InvalidMaxTokensType(t *testing.T) {
	data := `name: test
									workers: 2
									timeout: "30s"
									max_tokens: "invalid"
									tasks: []`

	tearDown, tempFile := SetupTest(t)
	defer tearDown()

	if _, err := tempFile.WriteString(data); err != nil {
		t.Fatalf("failed to write YAML: %v", err)
	}
	_, err := Parse(tempFile.Name())
	if err == nil {
		t.Fatalf("expected Parse to return error, got nil")
	}
}

func TestParse_InvalidYAMLStructure(t *testing.T) {
	// Provide deliberately invalid YAML
	data := `: invalid_yaml::`

	tearDown, tempFile := SetupTest(t)
	defer tearDown()

	if _, err := tempFile.WriteString(data); err != nil {
		t.Fatalf("failed to write YAML: %v", err)
	}

	_, err := Parse(tempFile.Name())
	if err == nil {
		t.Fatalf("expected Parse to return error for invalid YAML structure, got nil")
	}
}
