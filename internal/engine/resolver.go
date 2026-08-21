package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/justinndidit/agentflow/internal/manifest"
	"github.com/justinndidit/agentflow/internal/persistence/models"
	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

// InputResolver turns a task's stored template into the concrete input a worker
// receives.
type InputResolver interface {
	Resolve(ctx context.Context, task *models.TaskRow) ([]byte, error)
}

// TemplateResolver resolves `{{ tasks.<key>.output... }}` references against the
// outputs its dependencies committed.
//
// Resolution happens at dispatch rather than at submit because at submit the
// upstream output does not exist yet. What is stored is the manifest as
// authored; what a worker receives is computed the moment before it runs.
type TemplateResolver struct {
	results repositories.TaskResultStore
}

func NewTemplateResolver(results repositories.TaskResultStore) *TemplateResolver {
	return &TemplateResolver{results: results}
}

// Resolve loads the referenced upstream outputs and substitutes them.
//
// A task with no references skips the database entirely, which is the common
// case: only tasks that actually consume upstream data pay for a query.
//
// Every error here is a task failure rather than an engine error. A missing or
// mis-shaped upstream output means an agent produced something the manifest did
// not expect, which is a workflow bug and has to surface as one — retrying the
// downstream task against the same output would fail identically.
func (r *TemplateResolver) Resolve(ctx context.Context, task *models.TaskRow) ([]byte, error) {
	refs := manifest.TemplateRefs(task.InputTemplate)
	if len(refs) == 0 {
		return task.InputTemplate, nil
	}

	raw, err := r.results.OutputsByTaskKey(ctx, task.WorkflowID, refs)
	if err != nil {
		return nil, fmt.Errorf("load upstream outputs for task %s: %w", task.TaskKey, err)
	}

	outputs := make(map[string]any, len(raw))
	for key, encoded := range raw {
		// A null output is legal — a worker may legitimately return nothing —
		// and decodes to nil, which the resolver reports as a missing field if
		// anything tries to read through it.
		var decoded any
		if len(encoded) > 0 {
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				return nil, fmt.Errorf(
					"output of task %s is not valid JSON: %w", key, err)
			}
		}
		outputs[key] = decoded
	}

	// A reference the parser accepted but that has no output means the upstream
	// task has not committed. The claim only releases a task once
	// remaining_deps reaches zero, so this should be unreachable — and if it is
	// reached, something is wrong with the counter rather than the manifest.
	for _, ref := range refs {
		if _, ok := outputs[ref]; !ok {
			return nil, fmt.Errorf(
				"task %s references the output of %s, which has not completed",
				task.TaskKey, ref)
		}
	}

	resolved, err := manifest.ResolveInput(task.InputTemplate, outputs)
	if err != nil {
		return nil, fmt.Errorf("resolve input for task %s: %w", task.TaskKey, err)
	}
	return resolved, nil
}

// StaticResolver hands back the stored template unchanged. It exists for tests
// and for paths that deliberately bypass resolution.
type StaticResolver struct{}

func (StaticResolver) Resolve(_ context.Context, task *models.TaskRow) ([]byte, error) {
	return task.InputTemplate, nil
}
