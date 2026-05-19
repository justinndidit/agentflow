package engine

import (
	"fmt"

	"github.com/justinndidit/agentflow/internal/state"
	"github.com/justinndidit/agentflow/pkg/set"
)

type Graph struct {
	Tasks map[string]*state.Task
}

func NewGraph(tasks []*state.Task) (*Graph, error) {
	g := &Graph{
		Tasks: make(map[string]*state.Task),
	}

	for _, t := range tasks {
		g.Tasks[t.ID] = t
	}

	return g, nil
}

// TopologicalSort returns tasks grouped by execution wave
// wave 0 runs first, wave 1 runs after wave 0 completes, etc.
// tasks in the same wave run in parallel
func (g *Graph) TopologicalSort() ([][]*state.Task, error) {
	result := [][]*state.Task{}
	startNodes := set.NewSet[*state.Task]()

	//extract dependency count for each task  specified in graph
	//find adjacent tasks
	taskDependencyCount := map[string]int{}
	//represents a map of TaskID to tasks that TaskID unblocks
	adjacentTasks := map[string][]*state.Task{}

	for key, task := range g.Tasks {
		dependencyLength := len(task.DependsOn)
		if dependencyLength == 0 {
			startNodes.Add(task)
			continue
		}
		taskDependencyCount[key] = dependencyLength

		for _, dependency := range task.DependsOn {
			adjacentTasks[dependency] = append(adjacentTasks[dependency], task)
		}
	}

	for len(startNodes) > 0 {
		currentWave := startNodes.ToSlice()
		startNodes.Clear()

		//start nodes have no inbound edges
		for _, startNode := range currentWave {
			for _, task := range adjacentTasks[startNode.ID] {
				taskDependencyCount[task.ID]--

				if taskDependencyCount[task.ID] == 0 {
					startNodes.Add(task)
				}
			}
		}
		result = append(result, currentWave)

	}

	taskCount := 0
	for _, wave := range result {
		for range wave {
			taskCount++
		}
	}

	if taskCount < len(g.Tasks) {
		return nil, fmt.Errorf("cycle detected: %d and graph has %d tasks", taskCount, len(g.Tasks))
	}

	return result, nil
}
