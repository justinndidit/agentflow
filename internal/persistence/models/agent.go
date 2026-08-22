package models

// AgentRow is a registered agent: a name a manifest can refer to, and the
// container image that implements it.
type AgentRow struct {
	BaseModel

	Name string `db:"name"`

	// AgentImage is a container image reference. The engine never inspects it —
	// what runs inside is the worker author's business.
	AgentImage string `db:"agent_image"`

	// AgentCommand overrides the image's entrypoint. Null for most agents, so
	// the image runs whatever it was built to run; set where one image
	// implements several agents and distinguishes them by argument.
	AgentCommand []string `db:"agent_command"`
}

// NewAgentRow builds a registry entry. command may be empty, in which case the
// image runs whatever it was built to run.
func NewAgentRow(name, image string, command []string) AgentRow {
	return AgentRow{
		BaseModel:    NewBaseModel(),
		Name:         name,
		AgentImage:   image,
		AgentCommand: command,
	}
}
