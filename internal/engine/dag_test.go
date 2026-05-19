package engine

import (
	"slices"
	"strings"
	"testing"

	"github.com/justinndidit/agentflow/internal/state"
)

func TestTopologicalSort_Parallel(t *testing.T) {
	//t1 -> t2
	//t1 -> t3
	//t2 -> t4
	//t3 -> t4
	tasks := []*state.Task{
		{ID: "t1", DependsOn: []string{}},
		{ID: "t2", DependsOn: []string{"t1"}},
		{ID: "t3", DependsOn: []string{"t1"}},
		{ID: "t4", DependsOn: []string{"t2", "t3"}},
	}

	solutionWaves := [][]string{
		{"t1"}, {"t2", "t3"}, {"t4"},
	}

	g, err := NewGraph(tasks)
	if err != nil {
		t.Fatalf("unexpected error creating graph: %s", err)
	}

	waves, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error with TopologicalSort(): %s", err)
	}

	if len(waves) != 3 {
		t.Fatalf("expected 3 waves, got %d", len(waves))
	}

	for n, wave := range waves {
		waveIDs := []string{}
		for _, task := range wave {
			waveIDs = append(waveIDs, task.ID)
		}
		slices.Sort(waveIDs)
		slices.Sort(solutionWaves[n])

		if slices.Compare(waveIDs, solutionWaves[n]) != 0 {
			t.Fatalf("expected wave to be %v but got %v", solutionWaves[n], waveIDs)
		}
	}

}

func TestTopologicalSort_Cycle(t *testing.T) {
	//t1 -> t2 -> t3 -> t1
	tasks := []*state.Task{
		{ID: "t1", DependsOn: []string{"t3"}},
		{ID: "t2", DependsOn: []string{"t1"}},
		{ID: "t3", DependsOn: []string{"t2"}},
	}
	g, err := NewGraph(tasks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = g.TopologicalSort()
	if err == nil {
		t.Fatalf("expected cycle detection error, got nil")
	}
}

func TestTopologicalSort_Linear(t *testing.T) {
	//t1 -> t2 -> t3
	tasks := []*state.Task{
		{ID: "t1", DependsOn: []string{}},
		{ID: "t2", DependsOn: []string{"t1"}},
		{ID: "t3", DependsOn: []string{"t2"}},
	}
	solutionWaves := []string{"t1", "t2", "t3"}

	g, err := NewGraph(tasks)

	if err != nil {
		t.Fatalf("expected graph got error: %s", err)
	}

	waves, err := g.TopologicalSort()

	if err != nil {
		t.Fatalf(
			"unexpected error: %s", err)
	}

	if len(waves) != 3 {
		t.Fatalf("expected 3 waves got %d", len(waves))
	}

	for i, wave := range waves {
		waveIDs := []string{}

		for _, task := range wave {
			waveIDs = append(waveIDs, task.ID)
		}

		slices.Sort(waveIDs)

		if strings.Compare(waveIDs[0], solutionWaves[i]) != 0 {
			t.Fatalf("expected wave to be %s but got %s", solutionWaves[i], waveIDs[0])
		}
	}
}

func TestTopologicalSort_Indpendent(t *testing.T) {
	//t1, t2, t3 all independent
	tasks := []*state.Task{
		{ID: "t1", DependsOn: []string{}},
		{ID: "t2", DependsOn: []string{}},
		{ID: "t3", DependsOn: []string{}},
	}
	solutionWave := []string{"t1", "t2", "t3"}

	g, err := NewGraph(tasks)
	if err != nil {
		t.Fatalf("unexpected error creating graph: %s", err)
	}

	waves, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("unexpected error with TopologicalSort(): %s", err)
	}

	if len(waves) != 1 {
		t.Fatalf("expected 1 wave, got %d", len(waves))
	}

	for _, wave := range waves {
		waveIDs := []string{}

		for _, task := range wave {
			waveIDs = append(waveIDs, task.ID)
		}

		slices.Sort(waveIDs)
		slices.Sort(solutionWave)
		if slices.Compare(waveIDs, solutionWave) != 0 {
			t.Fatalf("expected wave to be %v but got %v", solutionWave, waveIDs)
		}
	}
}
