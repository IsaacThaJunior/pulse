package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- fakes ---

type fakeQueue struct {
	mu                sync.Mutex
	high, medium, low []string
	dlq               []string
}

func (q *fakeQueue) EnqueueWithPriority(taskID, priority string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	switch priority {
	case "high":
		q.high = append(q.high, taskID)
	case "low":
		q.low = append(q.low, taskID)
	default:
		q.medium = append(q.medium, taskID)
	}
	return nil
}

func (q *fakeQueue) DequeuePriorityBlocking(_ time.Duration) (string, string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	switch {
	case len(q.high) > 0:
		id := q.high[0]
		q.high = q.high[1:]
		return id, "events_high", nil
	case len(q.medium) > 0:
		id := q.medium[0]
		q.medium = q.medium[1:]
		return id, "events_medium", nil
	case len(q.low) > 0:
		id := q.low[0]
		q.low = q.low[1:]
		return id, "events_low", nil
	}
	// Avoid busy-spinning the test worker goroutine while idle.
	time.Sleep(2 * time.Millisecond)
	return "", "", nil
}

func (q *fakeQueue) Schedule(_, _ string, _ time.Time) error { return nil }
func (q *fakeQueue) PromoteScheduled() error                 { return nil }

func (q *fakeQueue) EnqueueToDLQ(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dlq = append(q.dlq, taskID)
	return nil
}

func (q *fakeQueue) GetQueueDepths() (map[string]int64, error) { return map[string]int64{}, nil }

func (q *fakeQueue) GetDLQItems() ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.dlq...), nil
}

func (q *fakeQueue) RemoveFromDLQ(_ string) error { return nil }

type fakeStore struct {
	mu       sync.Mutex
	tasks    map[string]Task
	attempts map[string][]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{tasks: make(map[string]Task), attempts: make(map[string][]string)}
}

func (s *fakeStore) put(t Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[t.ID] = t
}

func (s *fakeStore) GetTask(_ context.Context, id string) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, fmt.Errorf("task %s not found", id)
	}
	return t, nil
}

func (s *fakeStore) UpdateStatus(_ context.Context, id, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tasks[id]
	t.Status = status
	s.tasks[id] = t
	return nil
}

func (s *fakeStore) RecordAttempt(_ context.Context, id, status string, _ int, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[id] = append(s.attempts[id], status)
	return nil
}

func (s *fakeStore) status(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tasks[id].Status
}

func waitForStatus(t *testing.T, store *fakeStore, taskID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if store.status(taskID) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("task %s: status never became %q, last was %q", taskID, want, store.status(taskID))
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// --- tests ---

func TestPool_Success(t *testing.T) {
	q := &fakeQueue{}
	store := newFakeStore()
	mux := NewMux()

	var calls atomic.Int32
	mux.Handle("echo", func(_ context.Context, task Task) error {
		calls.Add(1)
		return nil
	})

	store.put(Task{ID: "t1", Type: "echo", Priority: "medium"})
	q.EnqueueWithPriority("t1", "medium")

	pool := NewPool(q, store, mux, 1, testLogger())
	pool.Start()
	defer pool.Stop()

	waitForStatus(t, store, "t1", "processed", 2*time.Second)
	if calls.Load() != 1 {
		t.Fatalf("expected handler to run once, ran %d times", calls.Load())
	}
	if pool.HealthStats().TotalProcessed != 1 {
		t.Fatalf("expected TotalProcessed=1, got %d", pool.HealthStats().TotalProcessed)
	}
}

func TestPool_RetryThenSucceed(t *testing.T) {
	q := &fakeQueue{}
	store := newFakeStore()
	mux := NewMux()

	var calls atomic.Int32
	mux.Handle("flaky", func(_ context.Context, task Task) error {
		n := calls.Add(1)
		if n < 3 {
			return fmt.Errorf("attempt %d failed", n)
		}
		return nil
	})

	store.put(Task{ID: "t1", Type: "flaky", Priority: "medium"})
	q.EnqueueWithPriority("t1", "medium")

	pool := NewPool(q, store, mux, 1, testLogger(), WithBaseDelay(time.Millisecond))
	pool.Start()
	defer pool.Stop()

	waitForStatus(t, store, "t1", "processed", 2*time.Second)
	if calls.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls.Load())
	}
}

func TestPool_RetryExhaustion_DLQ(t *testing.T) {
	q := &fakeQueue{}
	store := newFakeStore()
	mux := NewMux()

	mux.Handle("always_fails", func(_ context.Context, task Task) error {
		return fmt.Errorf("nope")
	})

	store.put(Task{ID: "t1", Type: "always_fails", Priority: "medium"})
	q.EnqueueWithPriority("t1", "medium")

	pool := NewPool(q, store, mux, 1, testLogger(), WithMaxRetries(2), WithBaseDelay(time.Millisecond))
	pool.Start()
	defer pool.Stop()

	waitForStatus(t, store, "t1", "failed", 2*time.Second)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		q.mu.Lock()
		n := len(q.dlq)
		q.mu.Unlock()
		if n == 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("expected task to land in the DLQ")
}

func TestPool_HandlerPanicRecovered(t *testing.T) {
	q := &fakeQueue{}
	store := newFakeStore()
	mux := NewMux()

	mux.Handle("panics", func(_ context.Context, task Task) error {
		panic("boom")
	})

	store.put(Task{ID: "t1", Type: "panics", Priority: "medium"})
	q.EnqueueWithPriority("t1", "medium")

	pool := NewPool(q, store, mux, 1, testLogger(), WithMaxRetries(2), WithBaseDelay(time.Millisecond))
	pool.Start()
	defer pool.Stop()

	// If the panic isn't recovered, this whole test binary crashes instead
	// of reaching this assertion — that's the strongest possible signal.
	waitForStatus(t, store, "t1", "failed", 2*time.Second)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		q.mu.Lock()
		n := len(q.dlq)
		q.mu.Unlock()
		if n == 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("expected panicking task to land in the DLQ like any other failure")
}

func TestPool_CancelledTaskSkipped(t *testing.T) {
	q := &fakeQueue{}
	store := newFakeStore()
	mux := NewMux()

	var called atomic.Bool
	mux.Handle("noop", func(_ context.Context, task Task) error {
		called.Store(true)
		return nil
	})

	store.put(Task{ID: "t1", Type: "noop", Priority: "medium", Status: "cancelled"})
	q.EnqueueWithPriority("t1", "medium")

	pool := NewPool(q, store, mux, 1, testLogger(), WithBaseDelay(time.Millisecond))
	pool.Start()
	defer pool.Stop()

	// Give the worker a chance to dequeue and process (or skip) the task.
	time.Sleep(100 * time.Millisecond)

	if called.Load() {
		t.Fatal("handler should not have run for a cancelled task")
	}
	if got := store.status("t1"); got != "cancelled" {
		t.Fatalf("expected status to remain \"cancelled\", got %q", got)
	}
	q.mu.Lock()
	dlqLen := len(q.dlq)
	q.mu.Unlock()
	if dlqLen != 0 {
		t.Fatalf("cancelled task should not be sent to the DLQ, got %d dlq items", dlqLen)
	}
}
