// Package memstore is an in-memory implementation of worker.Store.
// Intended for tests and local development without a real database.
package memstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/isaacthajunior/pulse/worker"
)

var _ worker.Store = (*Store)(nil)

// Store is a mutex-protected, map-backed worker.Store.
type Store struct {
	mu    sync.Mutex
	tasks map[string]worker.Task
}

// New returns an empty Store.
func New() *Store {
	return &Store{tasks: make(map[string]worker.Task)}
}

// Put inserts or replaces a task. Callers use this the way an app would
// use its own repository's "save task" method — memstore has no separate
// creation API, just this and the worker.Store methods.
func (s *Store) Put(t worker.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[t.ID] = t
}

func (s *Store) GetTask(_ context.Context, id string) (worker.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return worker.Task{}, fmt.Errorf("memstore: task %s not found", id)
	}
	return t, nil
}

func (s *Store) UpdateStatus(_ context.Context, id, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("memstore: task %s not found", id)
	}
	t.Status = status
	s.tasks[id] = t
	return nil
}

func (s *Store) RecordAttempt(_ context.Context, id, status string, attempt int, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; !ok {
		return fmt.Errorf("memstore: task %s not found", id)
	}
	// memstore doesn't keep a delivery log today; this exists so callers
	// that want one can wrap Store and intercept RecordAttempt.
	return nil
}
