package worker

// WorkerHealthStats is the snapshot returned by a Pool's HealthStats call.
type WorkerHealthStats struct {
	TotalWorkers   int   `json:"total_workers"`
	ActiveWorkers  int32 `json:"active_workers"`
	IdleWorkers    int32 `json:"idle_workers"`
	TotalProcessed int64 `json:"total_processed"`
	TotalFailed    int64 `json:"total_failed"`
	UptimeSeconds  int64 `json:"uptime_seconds"`
}

// WorkerHealthProvider is implemented by anything that can report worker
// pool health, typically *Pool.
type WorkerHealthProvider interface {
	HealthStats() WorkerHealthStats
}
