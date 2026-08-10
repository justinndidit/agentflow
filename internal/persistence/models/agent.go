// Package model
package models

type Agent struct {
	BaseModel
	AgentName string `db:"name"`
	Image     string `db:"agent_image"`
}
