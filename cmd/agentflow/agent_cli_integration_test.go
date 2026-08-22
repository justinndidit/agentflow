//go:build integration

package main_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/justinndidit/agentflow/internal/dbtest"
)

// The registry CLI is the first useful thing anyone does with Agentflow, so it
// is exercised as a real binary against a real database rather than by calling
// the store directly.
func TestAgentCLI_RegisterListRemove(t *testing.T) {
	binary := buildBinary(t)
	host, port := dbtest.HostPort(t)
	env := engineEnv(t, host, port)

	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(binary, args...)
		cmd.Env = env
		cmd.Dir = dbtest.RepoRoot()

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v failed: %v\n%s", args, err, output)
		}
		return string(output)
	}

	if out := run("agent", "list"); !strings.Contains(out, "no agents registered") {
		t.Errorf("list on an empty registry said: %s", out)
	}

	run("agent", "register", "-name", "research-agent", "-image", "agentflow/research:v1")
	run("agent", "register", "-name", "writer-agent",
		"-image", "agentflow/generic:v1", "-command", "python worker.py --role=writer")

	listed := run("agent", "list")
	for _, want := range []string{
		"research-agent", "agentflow/research:v1",
		"writer-agent", "python worker.py --role=writer",
		// An agent with no command says so, rather than showing a blank column
		// that reads like missing data.
		"(image entrypoint)",
	} {
		if !strings.Contains(listed, want) {
			t.Errorf("list output is missing %q:\n%s", want, listed)
		}
	}

	// Registering again re-points rather than duplicating.
	run("agent", "register", "-name", "research-agent", "-image", "agentflow/research:v2")
	relisted := run("agent", "list")
	if strings.Contains(relisted, "research:v1") {
		t.Errorf("re-registering did not replace the image:\n%s", relisted)
	}
	if strings.Count(relisted, "research-agent") != 1 {
		t.Errorf("re-registering duplicated the agent:\n%s", relisted)
	}

	run("agent", "remove", "-name", "writer-agent")
	if out := run("agent", "list"); strings.Contains(out, "writer-agent") {
		t.Errorf("the agent survived removal:\n%s", out)
	}
}
