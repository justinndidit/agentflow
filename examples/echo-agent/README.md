# echo-agent

A reference agent: it reads the resolved input from stdin and writes it back on
stdout. Useful for exercising a real container end to end without needing a
model provider.

```bash
docker build -t agentflow/echo-agent:latest examples/echo-agent

# Register it under whatever name your manifest uses.
go run ./cmd/agentflow agent register \
  -name research-agent -image agentflow/echo-agent:latest

AGENTFLOW__ENGINE__RUNTIME=docker go run ./cmd/agentflow engine
```

The worker contract is specified in
[docs/agentflow_architecture.md](../../docs/agentflow_architecture.md) §8.
