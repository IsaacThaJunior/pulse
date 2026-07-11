package worker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/isaacthajunior/pulse/queue/memqueue"
	"github.com/isaacthajunior/pulse/worker"
	"github.com/isaacthajunior/pulse/worker/memstore"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// TestPool_ConcurrentStress hammers a Pool backed by memqueue + memstore
// with many tasks enqueued from many goroutines while the pool is already
// running, under go test -race. It exists to catch data races or lost
// updates in Queue/Store/Mux that a single low-throughput consumer
// (the reference app) wouldn't surface.
func TestPool_ConcurrentStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	const numTasks = 2000
	const numWorkers = 16
	const failEvery = 37 // deliberately-always-failing subset -> exercises retry/DLQ under load

	q := memqueue.New()
	store := memstore.New()
	mux := worker.NewMux()

	var handlerCalls atomic.Int64
	mux.Handle("stress", func(_ context.Context, task worker.Task) error {
		handlerCalls.Add(1)
		if task.Metadata["fail"] == "true" {
			return fmt.Errorf("stress: deliberate failure for %s", task.ID)
		}
		return nil
	})

	pool := worker.NewPool(q, store, mux, numWorkers, discardLogger(),
		worker.WithMaxRetries(3),
		worker.WithBaseDelay(time.Millisecond),
	)
	pool.Start()
	defer pool.Stop()

	priorities := []string{"high", "medium", "low"}
	var expectedFailed int64
	var wg sync.WaitGroup
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("task-%d", i)
			meta := map[string]string{}
			if i%failEvery == 0 {
				meta["fail"] = "true"
			}
			store.Put(worker.Task{
				ID:       id,
				Type:     "stress",
				Priority: priorities[i%len(priorities)],
				Metadata: meta,
			})
			if err := q.EnqueueWithPriority(id, priorities[i%len(priorities)]); err != nil {
				t.Errorf("enqueue %s: %v", id, err)
			}
		}(i)
	}
	for i := 0; i < numTasks; i++ {
		if i%failEvery == 0 {
			expectedFailed++
		}
	}
	wg.Wait()

	deadline := time.Now().Add(30 * time.Second)
	var stats worker.WorkerHealthStats
	for time.Now().Before(deadline) {
		stats = pool.HealthStats()
		if stats.TotalProcessed+stats.TotalFailed >= numTasks {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	terminal := stats.TotalProcessed + stats.TotalFailed
	if terminal != numTasks {
		t.Fatalf("expected all %d tasks to reach a terminal state within the deadline, got %d (processed=%d failed=%d)",
			numTasks, terminal, stats.TotalProcessed, stats.TotalFailed)
	}
	if stats.TotalFailed != expectedFailed {
		t.Errorf("expected %d permanently-failed tasks, got %d", expectedFailed, stats.TotalFailed)
	}

	dlqItems, err := q.GetDLQItems()
	if err != nil {
		t.Fatalf("GetDLQItems: %v", err)
	}
	if int64(len(dlqItems)) != expectedFailed {
		t.Errorf("expected %d DLQ items, got %d", expectedFailed, len(dlqItems))
	}
}
