package engine

import (
	"slices"
	"strings"
	"testing"

	"github.com/justinndidit/agentflow/internal/manifest"
)

// task builds the only two fields TopologicalSort reads. Manifests reaching the
// graph have already passed parsing, so the rest is irrelevant here.
func task(key string, dependsOn ...string) *manifest.TaskDefinition {
	return &manifest.TaskDefinition{TaskKey: key, DependsOn: dependsOn}
}

// waveKeys flattens the sorted waves into comparable task-key slices. Order
// within a wave is not meaningful — tasks in the same wave are by definition
// independent, and the set backing the sort does not preserve insertion order —
// so each wave is sorted before comparison.
func waveKeys(waves [][]*manifest.TaskDefinition) [][]string {
	out := make([][]string, 0, len(waves))
	for _, wave := range waves {
		keys := make([]string, 0, len(wave))
		for _, t := range wave {
			keys = append(keys, t.TaskKey)
		}
		slices.Sort(keys)
		out = append(out, keys)
	}
	return out
}

func sortTasks(t *testing.T, tasks []*manifest.TaskDefinition) [][]string {
	t.Helper()

	graph, err := NewGraph(tasks)
	if err != nil {
		t.Fatalf("failed to build graph: %v", err)
	}
	waves, err := graph.TopologicalSort()
	if err != nil {
		t.Fatalf("expected an acyclic graph to sort, got: %v", err)
	}
	return waveKeys(waves)
}

func TestTopologicalSort_Acyclic(t *testing.T) {
	tests := []struct {
		name  string
		tasks []*manifest.TaskDefinition
		want  [][]string
	}{
		{
			name:  "single task",
			tasks: []*manifest.TaskDefinition{task("a")},
			want:  [][]string{{"a"}},
		},
		{
			name:  "independent tasks share one wave",
			tasks: []*manifest.TaskDefinition{task("a"), task("b"), task("c")},
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "linear chain is one task per wave",
			tasks: []*manifest.TaskDefinition{task("a"), task("b", "a"), task("c", "b")},
			want:  [][]string{{"a"}, {"b"}, {"c"}},
		},
		{
			// a -> b, a -> c, b -> d, c -> d
			name: "diamond",
			tasks: []*manifest.TaskDefinition{
				task("a"), task("b", "a"), task("c", "a"), task("d", "b", "c"),
			},
			want: [][]string{{"a"}, {"b", "c"}, {"d"}},
		},
		{
			name: "fan out",
			tasks: []*manifest.TaskDefinition{
				task("root"), task("x", "root"), task("y", "root"), task("z", "root"),
			},
			want: [][]string{{"root"}, {"x", "y", "z"}},
		},
		{
			name: "disconnected components advance together",
			tasks: []*manifest.TaskDefinition{
				task("a1"), task("a2", "a1"),
				task("b1"), task("b2", "b1"),
			},
			want: [][]string{{"a1", "b1"}, {"a2", "b2"}},
		},
		{
			// A task deeper in one branch must wait for its own chain, not for
			// the whole previous wave to be the same depth.
			name: "uneven branch depths",
			tasks: []*manifest.TaskDefinition{
				task("a"),
				task("b", "a"), task("c", "b"),
				task("d", "a"),
				task("end", "c", "d"),
			},
			want: [][]string{{"a"}, {"b", "d"}, {"c"}, {"end"}},
		},
		{
			// depends_on is not deduplicated, but the counter and the adjacency
			// list both double, so the extra decrement cancels the extra count.
			name:  "repeated dependency",
			tasks: []*manifest.TaskDefinition{task("a"), task("b", "a", "a")},
			want:  [][]string{{"a"}, {"b"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := sortTasks(t, test.tasks)
			if !slices.EqualFunc(got, test.want, slices.Equal) {
				t.Errorf("waves = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTopologicalSort_FanIn(t *testing.T) {
	got := sortTasks(t, []*manifest.TaskDefinition{
		task("x"), task("y"), task("z"), task("sink", "x", "y", "z"),
	})
	want := [][]string{{"x", "y", "z"}, {"sink"}}

	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("waves = %v, want %v", got, want)
	}
}

// depends_on is not deduplicated anywhere, and the sort survives that only
// because the same duplication lands on both sides: the dependency counter
// starts at 2 and the upstream task decrements it twice. Deduplicating one side
// alone would deadlock the task, so this pins the pairing.
//
// remaining_deps is persisted from the same unfiltered slice, so the dispatcher
// will inherit this and needs the same symmetry.
func TestTopologicalSort_RepeatedDependencyCancelsOut(t *testing.T) {
	got := sortTasks(t, []*manifest.TaskDefinition{task("a"), task("b", "a", "a")})
	want := [][]string{{"a"}, {"b"}}

	if !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("waves = %v, want %v", got, want)
	}
}

func TestTopologicalSort_Cycles(t *testing.T) {
	tests := []struct {
		name  string
		tasks []*manifest.TaskDefinition
	}{
		{
			name:  "self loop",
			tasks: []*manifest.TaskDefinition{task("a", "a")},
		},
		{
			name:  "two node cycle",
			tasks: []*manifest.TaskDefinition{task("a", "b"), task("b", "a")},
		},
		{
			name:  "three node cycle",
			tasks: []*manifest.TaskDefinition{task("a", "c"), task("b", "a"), task("c", "b")},
		},
		{
			// The sortable part must not mask the cycle: the count check has to
			// compare against every task, not just the ones it managed to place.
			name: "cycle alongside a sortable component",
			tasks: []*manifest.TaskDefinition{
				task("ok1"), task("ok2", "ok1"),
				task("x", "y"), task("y", "x"),
			},
		},
		{
			name: "cycle downstream of a valid root",
			tasks: []*manifest.TaskDefinition{
				task("root"),
				task("a", "root", "b"), task("b", "a"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			graph, err := NewGraph(test.tasks)
			if err != nil {
				t.Fatalf("failed to build graph: %v", err)
			}

			waves, err := graph.TopologicalSort()
			if err == nil {
				t.Fatalf("expected a cycle error, got waves %v", waveKeys(waves))
			}
			if waves != nil {
				t.Errorf("expected nil waves on error, got %v", waveKeys(waves))
			}
			if !strings.Contains(err.Error(), "cycle detected") {
				t.Errorf("error = %q, want it to report a cycle", err)
			}
		})
	}
}

func TestTopologicalSort_Empty(t *testing.T) {
	waves := sortTasks(t, []*manifest.TaskDefinition{})
	if len(waves) != 0 {
		t.Errorf("waves = %v, want none", waves)
	}
}

// Every task must appear exactly once across all waves; a task emitted twice
// would be dispatched twice.
func TestTopologicalSort_EmitsEachTaskOnce(t *testing.T) {
	tasks := []*manifest.TaskDefinition{
		task("a"), task("b", "a"), task("c", "a"), task("d", "b", "c"), task("e", "d"),
	}

	seen := map[string]int{}
	for _, wave := range sortTasks(t, tasks) {
		for _, key := range wave {
			seen[key]++
		}
	}

	if len(seen) != len(tasks) {
		t.Errorf("saw %d distinct tasks, want %d", len(seen), len(tasks))
	}
	for key, count := range seen {
		if count != 1 {
			t.Errorf("task %s appeared %d times, want 1", key, count)
		}
	}
}

// NewGraph keys tasks by task_key, so duplicates collapse. The parser rejects
// duplicates before the graph is built, which is what keeps this from losing a
// task in practice.
func TestNewGraph_DuplicateKeysCollapse(t *testing.T) {
	graph, err := NewGraph([]*manifest.TaskDefinition{task("a"), task("a")})
	if err != nil {
		t.Fatalf("failed to build graph: %v", err)
	}

	if len(graph.Tasks) != 1 {
		t.Errorf("len(graph.Tasks) = %d, want 1", len(graph.Tasks))
	}
}
