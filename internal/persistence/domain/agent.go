// Package domain
package domain

type Agent struct {
	BaseModel
	Image string `json:"agent_image" db:"agent_image"`
}
