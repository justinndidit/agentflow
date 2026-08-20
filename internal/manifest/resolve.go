package manifest

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// templateExprPattern matches one complete upstream reference and captures the
// task key and the path beneath `output`.
//
// It is deliberately stricter than templateRefPattern, which only has to spot a
// reference well enough to validate it. Resolution has to read the whole
// expression, so anything malformed must fail here rather than be silently
// treated as literal text.
var templateExprPattern = regexp.MustCompile(
	`\{\{\s*tasks\.([a-zA-Z0-9_-]+)\.output((?:\.[a-zA-Z0-9_-]+)*)\s*\}\}`)

// wholeExprPattern matches a string that is nothing but a single reference.
var wholeExprPattern = regexp.MustCompile(`^\s*` + templateExprPattern.String() + `\s*$`)

// ResolveInput substitutes upstream outputs into a task's stored input template.
//
// outputs maps a task key to that task's decoded output. Only the dependencies
// a task declared are supplied; the parser has already rejected any manifest
// that references anything else, so a missing key here means the upstream
// produced nothing rather than that the reference was illegal.
//
// Two substitution modes, and the difference is load-bearing:
//
//   - A value that is *only* a reference is replaced by the referenced value
//     with its type intact. `jobs: "{{ tasks.fetch.output.jobs }}"` yields an
//     array, not a string containing one, so a worker expecting a list gets a
//     list.
//   - A reference embedded in surrounding text is interpolated as text.
//     `prompt: "summarise {{ ... }} please"` has to stay a string, so the value
//     is stringified — scalars as themselves, structures as compact JSON.
//
// A reference that cannot be resolved is an error. That is a task failure, not
// an engine error: it means an upstream agent produced a shape the manifest did
// not expect, which is a workflow bug and should surface as one.
func ResolveInput(template []byte, outputs map[string]any) ([]byte, error) {
	if len(template) == 0 {
		return template, nil
	}
	// Nothing to do, and no reason to pay for a decode/encode round trip that
	// would also reformat input the author wrote by hand.
	if !templateExprPattern.Match(template) {
		return template, nil
	}

	var decoded any
	if err := json.Unmarshal(template, &decoded); err != nil {
		return nil, fmt.Errorf("input template is not valid JSON: %w", err)
	}

	resolved, err := resolveValue(decoded, outputs)
	if err != nil {
		return nil, err
	}

	encoded, err := json.Marshal(resolved)
	if err != nil {
		return nil, fmt.Errorf("failed to encode resolved input: %w", err)
	}
	return encoded, nil
}

// resolveValue walks the decoded template. Maps and slices are descended into;
// only strings can carry a reference.
func resolveValue(value any, outputs map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		return resolveString(typed, outputs)

	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			// Keys are not scanned. Nothing resolves them, and a templated key
			// would produce an input whose shape depends on upstream data —
			// which no worker contract could describe.
			resolvedItem, err := resolveValue(item, outputs)
			if err != nil {
				return nil, err
			}
			out[key] = resolvedItem
		}
		return out, nil

	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			resolvedItem, err := resolveValue(item, outputs)
			if err != nil {
				return nil, err
			}
			out = append(out, resolvedItem)
		}
		return out, nil

	default:
		return value, nil
	}
}

func resolveString(value string, outputs map[string]any) (any, error) {
	// Whole-value reference: preserve the referenced value's type.
	if match := wholeExprPattern.FindStringSubmatch(value); match != nil {
		return lookup(match[1], match[2], outputs)
	}

	if !templateExprPattern.MatchString(value) {
		return value, nil
	}

	// Embedded reference: the result has to remain a string, so every match is
	// stringified in place.
	var failure error
	interpolated := templateExprPattern.ReplaceAllStringFunc(value, func(expr string) string {
		if failure != nil {
			return expr
		}
		match := templateExprPattern.FindStringSubmatch(expr)
		resolved, err := lookup(match[1], match[2], outputs)
		if err != nil {
			failure = err
			return expr
		}
		text, err := stringify(resolved)
		if err != nil {
			failure = err
			return expr
		}
		return text
	})
	if failure != nil {
		return nil, failure
	}
	return interpolated, nil
}

// lookup walks path beneath the named task's output.
func lookup(taskKey, path string, outputs map[string]any) (any, error) {
	output, ok := outputs[taskKey]
	if !ok {
		return nil, fmt.Errorf("no output available for task %s", taskKey)
	}

	current := output
	walked := "tasks." + taskKey + ".output"

	for _, segment := range strings.Split(strings.TrimPrefix(path, "."), ".") {
		if segment == "" {
			continue
		}

		switch container := current.(type) {
		case map[string]any:
			next, ok := container[segment]
			if !ok {
				return nil, fmt.Errorf("%s has no key %q", walked, segment)
			}
			current = next

		case []any:
			// Numeric segments index into arrays, so a manifest can pull one
			// element out of an upstream list.
			index, err := strconv.Atoi(segment)
			if err != nil {
				return nil, fmt.Errorf("%s is a list; %q is not an index", walked, segment)
			}
			if index < 0 || index >= len(container) {
				return nil, fmt.Errorf("%s has %d elements; index %d is out of range",
					walked, len(container), index)
			}
			current = container[index]

		default:
			return nil, fmt.Errorf("%s is %s, which has no field %q",
				walked, describe(current), segment)
		}

		walked += "." + segment
	}

	return current, nil
}

// stringify renders a value for interpolation into surrounding text. Scalars
// render as themselves so a number does not arrive quoted; structures render as
// compact JSON, which is the only faithful option inside a string.
func stringify(value any) (string, error) {
	switch typed := value.(type) {
	case nil:
		return "", nil
	case string:
		return typed, nil
	case bool:
		return strconv.FormatBool(typed), nil
	case float64:
		// json.Unmarshal produces float64 for every number. Render integral
		// values without a trailing ".0", which is what an author writing
		// "{{ ... }} items" expects to see.
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10), nil
		}
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("failed to render value as text: %w", err)
		}
		return string(encoded), nil
	}
}

// describe names a value's JSON type for error messages, so a failure says what
// the upstream actually produced rather than only what was expected.
func describe(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case string:
		return "a string"
	case float64:
		return "a number"
	case bool:
		return "a boolean"
	case []any:
		return "a list"
	case map[string]any:
		return "an object"
	default:
		return "an unexpected type"
	}
}

// TemplateRefs returns the task keys referenced by a stored input template, so
// a caller knows which upstream outputs it has to load before resolving.
func TemplateRefs(template []byte) []string {
	seen := map[string]bool{}
	keys := []string{}

	for _, match := range templateExprPattern.FindAllSubmatch(template, -1) {
		key := string(match[1])
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys
}
