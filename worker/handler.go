package worker

import (
	"context"
	"sync"
)

// HandlerFunc processes a single task. Returning a nil error marks the task
// processed; a non-nil error triggers a retry (and eventually the DLQ once
// retries are exhausted). Handlers are responsible for their own result
// persistence and for enqueueing any follow-up tasks.
type HandlerFunc func(ctx context.Context, task Task) error

// Mux routes tasks to a HandlerFunc by task type.
type Mux struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

// NewMux returns an empty Mux.
func NewMux() *Mux {
	return &Mux{handlers: make(map[string]HandlerFunc)}
}

// Handle registers h as the handler for taskType, replacing any existing
// registration.
func (m *Mux) Handle(taskType string, h HandlerFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[taskType] = h
}

// handler returns the registered HandlerFunc for taskType, if any.
func (m *Mux) handler(taskType string) (HandlerFunc, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.handlers[taskType]
	return h, ok
}
