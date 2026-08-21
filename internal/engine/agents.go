package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/justinndidit/agentflow/internal/persistence/repositories"
)

// AgentImages resolves an agent name to the container image implementing it.
type AgentImages interface {
	ImageFor(ctx context.Context, agentName string) (string, error)
}

// CachedAgentImages looks images up once per agent and remembers them.
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
type CachedAgentImages struct {
	store repositories.AgentStore

	mu     sync.RWMutex
	images map[string]string
}

func NewCachedAgentImages(store repositories.AgentStore) *CachedAgentImages {
	return &CachedAgentImages{
		store:  store,
		images: map[string]string{},
	}
}

func (c *CachedAgentImages) ImageFor(ctx context.Context, agentName string) (string, error) {
	c.mu.RLock()
	image, cached := c.images[agentName]
	c.mu.RUnlock()
	if cached {
		return image, nil
	}

	agent, err := c.store.GetByName(ctx, agentName)
	if err != nil {
		return "", fmt.Errorf("resolve image for agent %s: %w", agentName, err)
	}
	if agent.AgentImage == "" {
		return "", fmt.Errorf("agent %s has no image registered", agentName)
	}

	c.mu.Lock()
	c.images[agentName] = agent.AgentImage
	c.mu.Unlock()

	return agent.AgentImage, nil
}

// StaticAgentImages returns the same image for every agent. Used by runtimes
// that do not run containers, where the lookup would be a pointless query.
type StaticAgentImages string

func (s StaticAgentImages) ImageFor(context.Context, string) (string, error) {
	return string(s), nil
}
