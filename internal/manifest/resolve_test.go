package manifest

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// decode is a convenience for building the outputs map from JSON literals, so
// tests describe upstream results the way they are actually stored.
func decode(t *testing.T, raw string) any {
	t.Helper()

	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		t.Fatalf("failed to decode %s: %v", raw, err)
	}
	return value
}

func resolve(t *testing.T, template string, outputs map[string]any) string {
	t.Helper()

	resolved, err := ResolveInput([]byte(template), outputs)
	if err != nil {
		t.Fatalf("ResolveInput failed: %v", err)
	}
	return string(resolved)
}

// A value that is only a reference keeps the referenced type. A worker
// expecting a list must receive a list, not a string containing one.
func TestResolveInput_PreservesType(t *testing.T) {
	tests := []struct {
		name     string
		template string
		output   string
		want     string
	}{
		{
			name:     "array",
			template: `{"jobs":"{{ tasks.fetch.output.jobs }}"}`,
			output:   `{"jobs":["a","b"]}`,
			want:     `{"jobs":["a","b"]}`,
		},
		{
			name:     "object",
			template: `{"config":"{{ tasks.fetch.output.config }}"}`,
			output:   `{"config":{"depth":3}}`,
			want:     `{"config":{"depth":3}}`,
		},
		{
			name:     "number",
			template: `{"count":"{{ tasks.fetch.output.count }}"}`,
			output:   `{"count":42}`,
			want:     `{"count":42}`,
		},
		{
			name:     "boolean",
			template: `{"ready":"{{ tasks.fetch.output.ready }}"}`,
			output:   `{"ready":true}`,
			want:     `{"ready":true}`,
		},
		{
			name:     "null",
			template: `{"maybe":"{{ tasks.fetch.output.maybe }}"}`,
			output:   `{"maybe":null}`,
			want:     `{"maybe":null}`,
		},
		{
			name:     "string stays a string",
			template: `{"name":"{{ tasks.fetch.output.name }}"}`,
			output:   `{"name":"ada"}`,
			want:     `{"name":"ada"}`,
		},
		{
			name:     "the whole output",
			template: `{"everything":"{{ tasks.fetch.output }}"}`,
			output:   `{"a":1,"b":2}`,
			want:     `{"everything":{"a":1,"b":2}}`,
		},
		{
			name:     "surrounding whitespace is ignored",
			template: `{"jobs":"{{    tasks.fetch.output.jobs    }}"}`,
			output:   `{"jobs":[1]}`,
			want:     `{"jobs":[1]}`,
		},
		{
			name:     "no whitespace at all",
			template: `{"jobs":"{{tasks.fetch.output.jobs}}"}`,
			output:   `{"jobs":[1]}`,
			want:     `{"jobs":[1]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := resolve(t, test.template, map[string]any{"fetch": decode(t, test.output)})
			if got != test.want {
				t.Errorf("ResolveInput = %s, want %s", got, test.want)
			}
		})
	}
}

// A reference inside surrounding text has to stay a string, so the value is
// rendered rather than substituted structurally.
func TestResolveInput_InterpolatesIntoText(t *testing.T) {
	tests := []struct {
		name     string
		template string
		output   string
		want     string
	}{
		{
			name:     "string value",
			template: `{"prompt":"summarise {{ tasks.fetch.output.name }} please"}`,
			output:   `{"name":"ada"}`,
			want:     `{"prompt":"summarise ada please"}`,
		},
		{
			// An integral number renders without a trailing ".0", which is what
			// an author writing "{{ ... }} items" expects to read.
			name:     "integral number",
			template: `{"prompt":"found {{ tasks.fetch.output.count }} items"}`,
			output:   `{"count":42}`,
			want:     `{"prompt":"found 42 items"}`,
		},
		{
			name:     "fractional number",
			template: `{"prompt":"score {{ tasks.fetch.output.score }}"}`,
			output:   `{"score":1.5}`,
			want:     `{"prompt":"score 1.5"}`,
		},
		{
			name:     "boolean",
			template: `{"prompt":"ready={{ tasks.fetch.output.ready }}"}`,
			output:   `{"ready":false}`,
			want:     `{"prompt":"ready=false"}`,
		},
		{
			name:     "structure renders as compact JSON",
			template: `{"prompt":"data: {{ tasks.fetch.output.jobs }}"}`,
			output:   `{"jobs":["a","b"]}`,
			want:     `{"prompt":"data: [\"a\",\"b\"]"}`,
		},
		{
			name:     "two references in one string",
			template: `{"prompt":"{{ tasks.fetch.output.a }} and {{ tasks.fetch.output.b }}"}`,
			output:   `{"a":"x","b":"y"}`,
			want:     `{"prompt":"x and y"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := resolve(t, test.template, map[string]any{"fetch": decode(t, test.output)})
			if got != test.want {
				t.Errorf("ResolveInput = %s, want %s", got, test.want)
			}
		})
	}
}

func TestResolveInput_WalksNestedPaths(t *testing.T) {
	outputs := map[string]any{
		"fetch": decode(t, `{"data":{"items":[{"id":7},{"id":9}]}}`),
	}

	got := resolve(t, `{"picked":"{{ tasks.fetch.output.data.items.1.id }}"}`, outputs)
	if got != `{"picked":9}` {
		t.Errorf("ResolveInput = %s, want the indexed element", got)
	}
}

func TestResolveInput_DescendsIntoStructures(t *testing.T) {
	outputs := map[string]any{"fetch": decode(t, `{"name":"ada"}`)}

	tests := []struct {
		name     string
		template string
		want     string
	}{
		{
			name:     "nested map",
			template: `{"a":{"b":{"c":"{{ tasks.fetch.output.name }}"}}}`,
			want:     `{"a":{"b":{"c":"ada"}}}`,
		},
		{
			name:     "list element",
			template: `{"items":["{{ tasks.fetch.output.name }}","literal"]}`,
			want:     `{"items":["ada","literal"]}`,
		},
		{
			name:     "list of maps",
			template: `{"items":[{"who":"{{ tasks.fetch.output.name }}"}]}`,
			want:     `{"items":[{"who":"ada"}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := resolve(t, test.template, outputs); got != test.want {
				t.Errorf("ResolveInput = %s, want %s", got, test.want)
			}
		})
	}
}

// A template with no references is returned byte for byte, without a decode and
// re-encode that would reorder keys and reformat what the author wrote.
func TestResolveInput_UntouchedWhenNoReferences(t *testing.T) {
	template := `{"zebra": 1, "alpha": 2, "note": "no templates here"}`

	resolved, err := ResolveInput([]byte(template), nil)
	if err != nil {
		t.Fatalf("ResolveInput failed: %v", err)
	}
	if string(resolved) != template {
		t.Errorf("ResolveInput = %s, want the input verbatim", resolved)
	}
}

func TestResolveInput_EmptyTemplate(t *testing.T) {
	resolved, err := ResolveInput(nil, nil)
	if err != nil {
		t.Fatalf("ResolveInput(nil) failed: %v", err)
	}
	if len(resolved) != 0 {
		t.Errorf("ResolveInput(nil) = %s, want empty", resolved)
	}
}

// Resolution failure means an upstream agent produced a shape the manifest did
// not expect. That is a workflow bug and has to surface as one rather than
// being papered over with a zero value.
func TestResolveInput_Failures(t *testing.T) {
	outputs := map[string]any{
		"fetch": decode(t, `{"name":"ada","jobs":["a"],"count":3}`),
	}

	tests := []struct {
		name     string
		template string
		wantErr  string
	}{
		{
			name:     "no output for the referenced task",
			template: `{"x":"{{ tasks.missing.output.name }}"}`,
			wantErr:  "no output available for task missing",
		},
		{
			name:     "key absent from the output",
			template: `{"x":"{{ tasks.fetch.output.nope }}"}`,
			wantErr:  `has no key "nope"`,
		},
		{
			name:     "path through a scalar",
			template: `{"x":"{{ tasks.fetch.output.name.deeper }}"}`,
			wantErr:  "is a string, which has no field",
		},
		{
			name:     "non-numeric index into a list",
			template: `{"x":"{{ tasks.fetch.output.jobs.first }}"}`,
			wantErr:  "is a list",
		},
		{
			name:     "index out of range",
			template: `{"x":"{{ tasks.fetch.output.jobs.5 }}"}`,
			wantErr:  "out of range",
		},
		{
			name:     "failure inside interpolated text",
			template: `{"x":"value is {{ tasks.fetch.output.nope }}"}`,
			wantErr:  `has no key "nope"`,
		},
		{
			name:     "failure nested deep in the template",
			template: `{"a":{"b":["{{ tasks.fetch.output.nope }}"]}}`,
			wantErr:  `has no key "nope"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := ResolveInput([]byte(test.template), outputs)
			if err == nil {
				t.Fatalf("expected an error containing %q, got %s", test.wantErr, resolved)
			}
			if resolved != nil {
				t.Errorf("expected nil output on failure, got %s", resolved)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, test.wantErr)
			}
		})
	}
}

// A stored template that is not valid JSON cannot be resolved. It only reaches
// this path if something wrote a malformed input_template, so failing loudly is
// the right response.
func TestResolveInput_MalformedTemplate(t *testing.T) {
	_, err := ResolveInput([]byte(`{"x": "{{ tasks.fetch.output.a }}"`), map[string]any{
		"fetch": decode(t, `{"a":1}`),
	})
	if err == nil {
		t.Fatal("expected malformed JSON to fail")
	}
}

// Keys are not scanned, matching the validator, which never checked them
// either. A templated key would produce an input whose shape depends on
// upstream data, which no worker contract could describe.
func TestResolveInput_KeysAreNotResolved(t *testing.T) {
	outputs := map[string]any{"fetch": decode(t, `{"name":"ada"}`)}

	got := resolve(t, `{"{{ tasks.fetch.output.name }}":"value"}`, outputs)
	if got != `{"{{ tasks.fetch.output.name }}":"value"}` {
		t.Errorf("ResolveInput = %s, want the key left alone", got)
	}
}

func TestTemplateRefs(t *testing.T) {
	tests := []struct {
		name     string
		template string
		want     []string
	}{
		{
			name:     "none",
			template: `{"a":"literal"}`,
			want:     []string{},
		},
		{
			name:     "one",
			template: `{"a":"{{ tasks.fetch.output.x }}"}`,
			want:     []string{"fetch"},
		},
		{
			name:     "several, deduplicated and in order",
			template: `{"a":"{{ tasks.fetch.output.x }}","b":"{{ tasks.rank.output.y }} {{ tasks.fetch.output.z }}"}`,
			want:     []string{"fetch", "rank"},
		},
		{
			name:     "nested",
			template: `{"a":{"b":["{{ tasks.deep.output.x }}"]}}`,
			want:     []string{"deep"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := TemplateRefs([]byte(test.template))
			if !slices.Equal(got, test.want) {
				t.Errorf("TemplateRefs = %v, want %v", got, test.want)
			}
		})
	}
}

// Every reference the validator accepts must also be resolvable. If the two
// patterns disagree, a manifest passes submit and then fails at dispatch — the
// worst possible time to discover it.
func TestResolveAndValidateAgreeOnSyntax(t *testing.T) {
	forms := []string{
		`{{ tasks.fetch.output }}`,
		`{{tasks.fetch.output}}`,
		`{{  tasks.fetch.output.a  }}`,
		`{{ tasks.fetch.output.a.b.c }}`,
		`{{ tasks.fetch-with-dash.output.a }}`,
		`{{ tasks.fetch_with_underscore.output.a }}`,
		`{{ tasks.fetch.output.0 }}`,
	}

	for _, form := range forms {
		t.Run(form, func(t *testing.T) {
			validatorRefs := map[string]bool{}
			collectTemplateRefs(form, validatorRefs)
			if len(validatorRefs) != 1 {
				t.Fatalf("the validator found %d refs in %s, want 1", len(validatorRefs), form)
			}

			resolverRefs := TemplateRefs([]byte(form))
			if len(resolverRefs) != 1 {
				t.Fatalf("the resolver found %d refs in %s, want 1", len(resolverRefs), form)
			}
			if !validatorRefs[resolverRefs[0]] {
				t.Errorf("the validator saw %v but the resolver saw %q", validatorRefs, resolverRefs[0])
			}
		})
	}
}
