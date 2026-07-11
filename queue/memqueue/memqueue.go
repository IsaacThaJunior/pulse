// Package memqueue is an in-process implementation of queue.Queue, backed
// by mutex-protected slices instead of Redis. Intended for tests, local
// development, and running pulse without any external infra — not a
// high-throughput production primitive (DequeuePriorityBlocking polls
// rather than blocking on a true wakeup signal).
package memqueue

import (
	"fmt"
	"sync"
	"time"

	"github.com/isaacthajunior/pulse/queue"
)

var _ queue.Queue = (*Queue)(nil)

type scheduledTask struct {
	taskID    string
	priority  string
	executeAt time.Time
}

// Queue is an in-process priority queue with delayed scheduling and a DLQ.
type Queue struct {
	mu                sync.Mutex
	high, medium, low []string
	scheduled         []scheduledTask
	dlq               []string
}

// New returns an empty Queue.
func New() *Queue {
	return &Queue{}
}

func (q *Queue) EnqueueWithPriority(taskID, priority string) error {
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

func (q *Queue) tryDequeue() (string, string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	switch {
	case len(q.high) > 0:
		id := q.high[0]
		q.high = q.high[1:]
		return id, "events_high", true
	case len(q.medium) > 0:
		id := q.medium[0]
		q.medium = q.medium[1:]
		return id, "events_medium", true
	case len(q.low) > 0:
		id := q.low[0]
		q.low = q.low[1:]
		return id, "events_low", true
	}
	return "", "", false
}

const pollInterval = 5 * time.Millisecond

// DequeuePriorityBlocking polls (rather than blocking on a true wakeup
// signal) until an item is available or timeout elapses.
func (q *Queue) DequeuePriorityBlocking(timeout time.Duration) (string, string, error) {
	deadline := time.Now().Add(timeout)
	for {
		if id, name, ok := q.tryDequeue(); ok {
			return id, name, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", "", nil
		}
		if remaining < pollInterval {
			time.Sleep(remaining)
		} else {
			time.Sleep(pollInterval)
		}
	}
}

func (q *Queue) Schedule(taskID, priority string, executeAt time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.scheduled = append(q.scheduled, scheduledTask{taskID: taskID, priority: priority, executeAt: executeAt})
	return nil
}

func (q *Queue) PromoteScheduled() error {
	q.mu.Lock()
	now := time.Now()
	var remaining, due []scheduledTask
	for _, s := range q.scheduled {
		if s.executeAt.After(now) {
			remaining = append(remaining, s)
		} else {
			due = append(due, s)
		}
	}
	q.scheduled = remaining
	q.mu.Unlock()

	for _, s := range due {
		if err := q.EnqueueWithPriority(s.taskID, s.priority); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) EnqueueToDLQ(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dlq = append(q.dlq, taskID)
	return nil
}

func (q *Queue) GetQueueDepths() (map[string]int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return map[string]int64{
		"high":      int64(len(q.high)),
		"medium":    int64(len(q.medium)),
		"low":       int64(len(q.low)),
		"scheduled": int64(len(q.scheduled)),
		"dlq":       int64(len(q.dlq)),
	}, nil
}

func (q *Queue) GetDLQItems() ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.dlq...), nil
}

func (q *Queue) RemoveFromDLQ(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, id := range q.dlq {
		if id == taskID {
			q.dlq = append(q.dlq[:i], q.dlq[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("task %s not found in DLQ", taskID)
}
