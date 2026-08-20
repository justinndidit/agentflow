# The smallest possible Agentflow agent, and a reference for the worker
# contract in docs/agentflow_architecture.md §8.
#
# The contract is deliberately thin: resolved JSON arrives on stdin, JSON goes
# out on stdout, exit 0 means success and anything else is a failed attempt with
# stderr captured as the reason. `cat` satisfies all of that — it echoes its
# input, exits 0, and writes nothing to stderr.
#
# Real agents replace cat with something that reads the input, calls a model,
# and prints a result. Everything else about the contract stays the same.
#
# The engine runs this with a read-only root filesystem, all capabilities
# dropped, no Docker socket, and memory, CPU and PID limits, so an agent should
# not expect to write outside its own stdout.
FROM alpine:3.20

# Runs as an unprivileged user: the engine drops capabilities, but the image
# should not be asking for root in the first place.
USER nobody

ENTRYPOINT ["cat"]
