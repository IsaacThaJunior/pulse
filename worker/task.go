package worker

import "encoding/json"

// Task is a unit of work dequeued from a queue.Queue and handed to a
// registered HandlerFunc.
type Task struct {
	ID       string
	Type     string
	Payload  json.RawMessage
	Priority string

	// Status is the task's persisted status (e.g. "pending", "cancelled")
	// as of the last Store.GetTask call. The pool checks for "cancelled"
	// to skip processing.
	Status string

	// Metadata is a generic key/value bag for cross-cutting concerns that
	// need to travel with a task. The pool recognizes the "trace_context"
	// key by convention: if set, it's treated as a W3C traceparent header
	// and used to continue the caller's trace across the queue boundary.
	Metadata map[string]string
}
