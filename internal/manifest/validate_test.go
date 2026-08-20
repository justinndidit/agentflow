package manifest

import (
	"strings"
	"testing"
)

// refManifest builds a two task workflow where the second task's input is
// supplied by the caller, so each case varies only the reference under test.
// depends is written verbatim into depends_on.
func refManifest(depends, input string) []byte {
	return []byte(`
name: refs
namespace: default
workers: 1
timeout: 1m
max_tokens: 10
tasks:
  - task_key: upstream
    agent: agent-a
    max_retries: 0
    timeout: 10
  - task_key: other
    agent: agent-a
    max_retries: 0
    timeout: 10
  - task_key: downstream
    agent: agent-b
    max_retries: 0
    timeout: 10
    depends_on:` + depends + `
    input:` + input + `
`)
}

func TestTemplateReferences_Allowed(t *testing.T) {
	tests := []struct {
		name    string
		depends string
		input   string
	}{
		{
			name:    "reference to declared dependency",
			depends: "\n      - upstream",
			input:   "\n      jobs: \"{{ tasks.upstream.output.jobs }}\"",
		},
		{
			name:    "no whitespace inside braces",
			depends: "\n      - upstream",
			input:   "\n      jobs: \"{{tasks.upstream.output.jobs}}\"",
		},
		{
			name:    "extra whitespace inside braces",
			depends: "\n      - upstream",
			input:   "\n      jobs: \"{{    tasks.upstream.output.jobs    }}\"",
		},
		{
			name:    "reference nested in a map",
			depends: "\n      - upstream",
			input:   "\n      config:\n        nested:\n          jobs: \"{{ tasks.upstream.output.jobs }}\"",
		},
		{
			name:    "reference nested in a slice",
			depends: "\n      - upstream",
			input:   "\n      items:\n        - \"{{ tasks.upstream.output.jobs }}\"",
		},
		{
			name:    "reference inside a slice of maps",
			depends: "\n      - upstream",
			input:   "\n      items:\n        - key: \"{{ tasks.upstream.output.jobs }}\"",
		},
		{
			name:    "two references to two declared dependencies",
			depends: "\n      - upstream\n      - other",
			input:   "\n      a: \"{{ tasks.upstream.output.x }}\"\n      b: \"{{ tasks.other.output.y }}\"",
		},
		{
			name:    "two references in a single string",
			depends: "\n      - upstream\n      - other",
			input:   "\n      combined: \"{{ tasks.upstream.output.x }} and {{ tasks.other.output.y }}\"",
		},
		{
			name:    "reference embedded in surrounding text",
			depends: "\n      - upstream",
			input:   "\n      prompt: \"summarise {{ tasks.upstream.output.jobs }} please\"",
		},
		{
			name:    "no references at all",
			depends: "\n      - upstream",
			input:   "\n      literal: \"nothing templated here\"",
		},
		{
			name:    "task key containing a hyphen and underscore",
			depends: "\n      - upstream",
			input:   "\n      jobs: \"{{ tasks.upstream.output.a_b-c }}\"",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseBytes(refManifest(test.depends, test.input)); err != nil {
				t.Fatalf("expected manifest to parse, got: %v", err)
			}
		})
	}
}

func TestTemplateReferences_Rejected(t *testing.T) {
	tests := []struct {
		name    string
		depends string
		input   string
		wantErr string
	}{
		{
			name:    "reference to a task not depended on",
			depends: "\n      - upstream",
			input:   "\n      jobs: \"{{ tasks.other.output.jobs }}\"",
			wantErr: "task downstream references output of task other but does not list it in depends_on",
		},
		{
			name:    "reference with no dependencies declared at all",
			depends: " []",
			input:   "\n      jobs: \"{{ tasks.upstream.output.jobs }}\"",
			wantErr: "does not list it in depends_on",
		},
		{
			name:    "reference to an unknown task",
			depends: "\n      - upstream",
			input:   "\n      jobs: \"{{ tasks.ghost.output.jobs }}\"",
			wantErr: "task downstream references output of unknown task ghost",
		},
		{
			name:    "self reference",
			depends: "\n      - upstream",
			input:   "\n      jobs: \"{{ tasks.downstream.output.jobs }}\"",
			wantErr: "task downstream references its own output",
		},
		{
			name:    "one valid and one invalid reference",
			depends: "\n      - upstream",
			input:   "\n      a: \"{{ tasks.upstream.output.x }}\"\n      b: \"{{ tasks.ghost.output.y }}\"",
			wantErr: "unknown task ghost",
		},
		{
			name:    "invalid reference nested deep in a map",
			depends: "\n      - upstream",
			input:   "\n      config:\n        nested:\n          jobs: \"{{ tasks.ghost.output.jobs }}\"",
			wantErr: "unknown task ghost",
		},
		{
			name:    "invalid reference nested in a slice",
			depends: "\n      - upstream",
			input:   "\n      items:\n        - \"{{ tasks.ghost.output.jobs }}\"",
			wantErr: "unknown task ghost",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseBytes(refManifest(test.depends, test.input))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", test.wantErr)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, test.wantErr)
			}
		})
	}
}

// Self reference is reported as such even though the task also declares itself
// in depends_on, which would otherwise satisfy the declared-dependency check. A
// task waiting on its own output can never become ready.
func TestTemplateReferences_SelfReferenceBeatsSelfDependency(t *testing.T) {
	_, err := ParseBytes(refManifest(
		"\n      - downstream",
		"\n      jobs: \"{{ tasks.downstream.output.jobs }}\"",
	))
	if err == nil {
		t.Fatal("expected a self reference to be rejected, got nil")
	}
	if !strings.Contains(err.Error(), "references its own output") {
		t.Errorf("error = %q, want it to report a self reference", err)
	}
}

// When a task carries several bad references the reported one is the
// alphabetically first, so the message does not vary between runs with map
// iteration order.
func TestTemplateReferences_ErrorIsDeterministic(t *testing.T) {
	manifest := refManifest(
		"\n      - upstream",
		"\n      a: \"{{ tasks.zzz.output.x }}\"\n      b: \"{{ tasks.aaa.output.y }}\"",
	)

	for range 20 {
		_, err := ParseBytes(manifest)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if !strings.Contains(err.Error(), "unknown task aaa") {
			t.Fatalf("error = %q, want the alphabetically first bad reference (aaa)", err)
		}
	}
}

// Documents a deliberate limit of the reference scan: it walks map values but
// never map keys, so a template used as a key is not validated. Nothing resolves
// keys at dispatch either, so this is consistent — but if that changes, this
// test should start failing.
func TestTemplateReferences_KeysAreNotScanned(t *testing.T) {
	_, err := ParseBytes(refManifest(
		"\n      - upstream",
		"\n      \"{{ tasks.ghost.output.jobs }}\": value",
	))
	if err != nil {
		t.Fatalf("references in map keys are not scanned, expected no error, got: %v", err)
	}
}

// collectTemplateRefs handles map[any]any as well as map[string]any. yaml.v3
// decodes into map[string]any, so that branch is unreachable through ParseBytes
// and is covered directly here to keep it honest.
func TestCollectTemplateRefs_MapAnyAny(t *testing.T) {
	refs := map[string]bool{}
	collectTemplateRefs(map[any]any{
		1: "{{ tasks.upstream.output.jobs }}",
	}, refs)

	if !refs["upstream"] {
		t.Errorf("refs = %v, want it to contain upstream", refs)
	}
}

func TestCollectTemplateRefs_IgnoresNonStrings(t *testing.T) {
	refs := map[string]bool{}
	collectTemplateRefs(map[string]any{
		"count":   42,
		"enabled": true,
		"ratio":   1.5,
		"empty":   nil,
	}, refs)

	if len(refs) != 0 {
		t.Errorf("refs = %v, want empty", refs)
	}
}
