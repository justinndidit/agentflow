package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

// AgentSpec is what the runtime needs to know about an agent: which image, and
// what to run inside it.
type AgentSpec struct {
	Image string

	// Command overrides the image's entrypoint. Empty for most agents, so the
	// image runs whatever it was built to run.
	Command []string
}

// AgentLookup resolves an agent name to the container that implements it.
type AgentLookup interface {
	Lookup(ctx context.Context, agentName string) (AgentSpec, error)
}

// CachedAgents resolves each agent once and remembers the answer.
//
// The mapping changes only when someone registers or re-points an agent, which
// is a deployment action rather than something that happens mid-run. Querying
// per task would add a round trip to the hot path to re-learn a value that is
// almost always the same.
//
// The trade is that a node keeps running the old image until it restarts. That
// is the right default for a scheduler — an image changing underneath a running
// workflow would make its tasks non-reproducible — but it does mean a re-point
// is not picked up live.
type CachedAgents struct {
	store repositories.AgentStore

	mu    sync.RWMutex
	specs map[string]AgentSpec
}

func NewCachedAgents(store repositories.AgentStore) *CachedAgents {
	return &CachedAgents{
		store: store,
		specs: map[string]AgentSpec{},
	}
}

func (c *CachedAgents) Lookup(ctx context.Context, agentName string) (AgentSpec, error) {
	c.mu.RLock()
	spec, cached := c.specs[agentName]
	c.mu.RUnlock()
	if cached {
		return spec, nil
	}

	agent, err := c.store.GetByName(ctx, agentName)
	if err != nil {
		return AgentSpec{}, fmt.Errorf("resolve agent %s: %w", agentName, err)
	}
	if agent.AgentImage == "" {
		return AgentSpec{}, fmt.Errorf("agent %s has no image registered", agentName)
	}

	spec = AgentSpec{Image: agent.AgentImage, Command: agent.AgentCommand}

	c.mu.Lock()
	c.specs[agentName] = spec
	c.mu.Unlock()

	return spec, nil
}

// StaticAgent returns the same spec for every agent. Used by runtimes that do
// not run containers, where the lookup would be a pointless query.
type StaticAgent AgentSpec

func (s StaticAgent) Lookup(context.Context, string) (AgentSpec, error) {
	return AgentSpec(s), nil
}
