package data

import (
	"encoding/json"
	"testing"
)

// The manifest expresses timeouts in seconds and pgtype.Interval stores
// microseconds, so this conversion sits directly between the two.
func TestConvertSecondsToMicroSeconds(t *testing.T) {
	tests := []struct {
		name    string
		seconds int64
		want    int64
	}{
		{"zero", 0, 0},
		{"one second", 1, 1_000_000},
		{"the example manifest's 300s", 300, 300_000_000},
		{"an hour", 3600, 3_600_000_000},
		{"negative", -1, -1_000_000},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ConvertSecondsToMicroSeconds(test.seconds); got != test.want {
				t.Errorf("ConvertSecondsToMicroSeconds(%d) = %d, want %d",
					test.seconds, got, test.want)
			}
		})
	}
}

// Task input is marshalled once at submit and stored as bytes. Go map iteration
// is randomised, so a marshaller that did not sort keys would produce a
// different input_template for the same manifest on every submission.
func TestMarshalData_IsDeterministic(t *testing.T) {
	input := map[string]any{
		"zebra": 1, "alpha": 2, "mike": 3, "bravo": 4, "yankee": 5,
	}

	first, err := MarshalData(input)
	if err != nil {
		t.Fatalf("MarshalData failed: %v", err)
	}

	for range 20 {
		next, err := MarshalData(input)
		if err != nil {
			t.Fatalf("MarshalData failed: %v", err)
		}
		if string(next) != string(first) {
			t.Fatalf("MarshalData is not deterministic:\n%s\n%s", first, next)
		}
	}
}

// Template expressions must reach the database intact — they are resolved at
// dispatch, so any escaping applied here would have to be undone there.
func TestMarshalData_PreservesTemplates(t *testing.T) {
	data, err := MarshalData(map[string]any{
		"jobs": "{{ tasks.fetch.output.jobs }}",
	})
	if err != nil {
		t.Fatalf("MarshalData failed: %v", err)
	}

	var round map[string]any
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if round["jobs"] != "{{ tasks.fetch.output.jobs }}" {
		t.Errorf("jobs = %v, want the template unchanged", round["jobs"])
	}
}

func TestMarshalData_NilInput(t *testing.T) {
	data, err := MarshalData(nil)
	if err != nil {
		t.Fatalf("MarshalData(nil) failed: %v", err)
	}
	// input_template is a NOT NULL column, so nil input must still produce
	// storable bytes rather than an empty slice.
	if string(data) != "null" {
		t.Errorf("MarshalData(nil) = %q, want %q", data, "null")
	}
}
