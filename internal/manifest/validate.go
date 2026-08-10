package manifest

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
)

// templateRefPattern matches an upstream output reference of the form
// {{ tasks.<task_key>.output... }} and captures the task key.
var templateRefPattern = regexp.MustCompile(`\{\{\s*tasks\.([a-zA-Z0-9_-]+)\.output`)

// collectTemplateRefs walks a decoded YAML value and records every task key
// referenced by a template expression. Input is arbitrarily nested, so maps and
// slices are both descended into; only strings can carry a reference.
func collectTemplateRefs(value any, refs map[string]bool) {
	switch v := value.(type) {
	case string:
		for _, match := range templateRefPattern.FindAllStringSubmatch(v, -1) {
			refs[match[1]] = true
		}
	case map[string]any:
		for _, item := range v {
			collectTemplateRefs(item, refs)
		}
	case map[any]any:
		for _, item := range v {
			collectTemplateRefs(item, refs)
		}
	case []any:
		for _, item := range v {
			collectTemplateRefs(item, refs)
		}
	}
}

// validateTemplateReferences enforces that a task may only read the output of a
// task it explicitly depends on. Without this, a reference can resolve against a
// task that has not run yet — the ordering would hold only by accident of the
// rest of the graph. known is the set of task keys defined in this workflow.
func validateTemplateReferences(tasks []*TaskDefinition, known map[string]bool) error {
	for _, task := range tasks {
		refs := map[string]bool{}
		collectTemplateRefs(task.Input, refs)

		declared := make(map[string]bool, len(task.DependsOn))
		for _, dependency := range task.DependsOn {
			declared[dependency] = true
		}

		// sorted so the reported error is stable across runs
		for _, ref := range slices.Sorted(maps.Keys(refs)) {
			switch {
			case ref == task.TaskKey:
				return fmt.Errorf("task %s references its own output", task.TaskKey)
			case !known[ref]:
				return fmt.Errorf("task %s references output of unknown task %s", task.TaskKey, ref)
			case !declared[ref]:
				return fmt.Errorf(
					"task %s references output of task %s but does not list it in depends_on",
					task.TaskKey, ref)
			}
		}
	}

	return nil
}
