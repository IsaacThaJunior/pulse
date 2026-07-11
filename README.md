# pulse

A backend-agnostic priority job queue and worker engine for Go.

`pulse` gives you the parts of a background job system that are easy to get
subtly wrong and tedious to write yourself: priority queues, delayed/scheduled
tasks, retry with exponential backoff, and a dead-letter queue for tasks that
exhaust their retries — plus a worker pool to run it all. You bring your own
task types, handler functions, and storage.

It intentionally does **not** ship any business logic, HTTP API, or specific
database integration. Those are your app's job.

## Packages

- **`queue`** — the `Queue` interface: `EnqueueWithPriority`, `DequeuePriorityBlocking`,
  `Schedule`, `PromoteScheduled`, DLQ operations, queue depths.
- **`queue/redisqueue`** — a Redis-backed implementation of `Queue`.
- **`worker`** — `Pool`, the engine that dequeues tasks and dispatches them to
  handlers registered on a `Mux`, with retry/backoff/DLQ built in. Also
  defines `Store` (the small persistence interface `Pool` needs) and
  `Metrics` (optional observability hooks).

## Usage

```go
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"

	"github.com/isaacthajunior/pulse/queue/redisqueue"
	"github.com/isaacthajunior/pulse/worker"
)

func main() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	q := redisqueue.NewRedisQueue(client, "my_queue")

	mux := worker.NewMux()
	mux.Handle("send_email", func(ctx context.Context, task worker.Task) error {
		// do the work; return an error to trigger a retry
		return nil
	})

	store := myStore{} // implement worker.Store against your own database

	pool := worker.NewPool(q, store, mux, 5, slog.New(slog.NewTextHandler(os.Stdout, nil)))
	pool.Start()
	defer pool.Stop()

	// enqueue work from anywhere:
	q.EnqueueWithPriority("task-123", "high")

	select {} // keep running
}
```

## `Store`

`Pool` only needs three methods from your persistence layer — it doesn't
know or care what database you use:

```go
type Store interface {
	GetTask(ctx context.Context, id string) (Task, error)
	UpdateStatus(ctx context.Context, id, status string) error
	RecordAttempt(ctx context.Context, id, status string, attempt int, errMsg string) error
}
```

Anything beyond that — saving task results, chaining follow-up tasks — is
ordinary code in your own handler functions, using your own storage and the
same `Queue` you gave the pool. `pulse` has no concept of task chaining.

## Options

```go
worker.NewPool(q, store, mux, workers, logger,
	worker.WithMaxRetries(10),
	worker.WithBaseDelay(2*time.Second),
	worker.WithMetrics(myPrometheusAdapter{}),
)
```
