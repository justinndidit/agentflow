package models

// AgentRow is a registered agent: a name a manifest can refer to, and the
// container image that implements it.
type AgentRow struct {
	BaseModel

	Name string `db:"name"`
	// AgentImage is a container image reference. The engine never inspects it —
	// what runs inside is the worker author's business.
	AgentImage string `db:"agent_image"`
}
