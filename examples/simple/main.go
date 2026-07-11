// Command simple demonstrates the plain, non-chaining common case: one
// handler, in-memory queue and store, zero external infra. Run with:
//
//	go run ./examples/simple
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/isaacthajunior/pulse/queue/memqueue"
	"github.com/isaacthajunior/pulse/worker"
	"github.com/isaacthajunior/pulse/worker/memstore"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	q := memqueue.New()
	store := memstore.New()
	mux := worker.NewMux()

	mux.Handle("uppercase", func(_ context.Context, task worker.Task) error {
		text := string(task.Payload)
		fmt.Printf("task %s: %q -> %q\n", task.ID, text, strings.ToUpper(text))
		return nil
	})

	pool := worker.NewPool(q, store, mux, 3, logger)
	pool.Start()
	defer pool.Stop()

	words := []string{"hello", "pulse", "background jobs"}
	for i, word := range words {
		id := fmt.Sprintf("task-%d", i)
		store.Put(worker.Task{ID: id, Type: "uppercase", Payload: []byte(word), Priority: "medium"})
		if err := q.EnqueueWithPriority(id, "medium"); err != nil {
			logger.Error("enqueue failed", "error", err)
		}
	}

	time.Sleep(500 * time.Millisecond) // let the pool drain before exiting
	fmt.Println("done")
}
