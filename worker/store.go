package worker

import "context"

// Store is the minimal persistence contract the Pool's retry/DLQ loop
// needs: fetch a task, update its status, and record a delivery attempt.
// Handlers are free to use their own, richer persistence for results and
// for creating follow-up tasks — the Pool never needs to.
type Store interface {
	// GetTask fetches the task's current type, payload, priority, status,
	// and metadata.
	GetTask(ctx context.Context, id string) (Task, error)

	// UpdateStatus sets the task's persisted status (e.g. "processed",
	// "failed").
	UpdateStatus(ctx context.Context, id, status string) error

	// RecordAttempt logs one delivery attempt for observability/debugging.
	// errMsg is empty on success.
	RecordAttempt(ctx context.Context, id, status string, attempt int, errMsg string) error
}
