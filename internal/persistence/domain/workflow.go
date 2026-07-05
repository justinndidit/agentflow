package domain

type Workflow struct {
	BaseModel
	WorkflowName       string `db:"name"`
	WorkflowNameSpace  string `db:"namespace"`
	Manifest           []byte `db:"manifest"`
	Version            int    `db:"version"`
	Status             string `db:"status"`
	DefaultWorkerCount int    `db:"worker_count"`
	DefaultTimeout     string `db:"default_timeout"`
	MaxTokensPerRun    int64  `db:"max_tokens"`
}
