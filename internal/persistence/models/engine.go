package models

import "time"

type Engine struct {
	BaseModel

	HostName    string    `db:"hostname"`
	Status      string    `db:"status"`
	Capacity    int       `db:"capacity"`
	StartedAt   time.Time `db:"started_at"`
	HeartBeatAt time.Time `db:"heartbeat_at"`
}
