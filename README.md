# pulse

[![CI](https://github.com/IsaacThaJunior/pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/IsaacThaJunior/pulse/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/isaacthajunior/pulse.svg)](https://pkg.go.dev/github.com/isaacthajunior/pulse)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A backend-agnostic priority job queue and worker engine for Go.

`pulse` gives you the parts of a background job system that are tedious and
easy to get subtly wrong: priority queues, delayed/scheduled tasks, retry
with exponential backoff, a dead-letter queue for tasks that exhaust their
retries, and a worker pool to run it all — with pluggable handlers, storage,
and metrics. You bring your task types, your handler functions, and your
database. `pulse` doesn't ship business logic, an HTTP API, or a specific
database integration; those are your app's job, not the library's.

> **Status:** `v0.1.0`, pre-1.0. APIs may still shift between minor
> versions — pin a version and read the [tag notes](https://github.com/IsaacThaJunior/pulse/tags)
> before upgrading.

---

## Contents

- [Why](#why)
- [Install](#install)
- [Quickstart](#quickstart)
- [Packages](#packages)
- [Core concepts](#core-concepts)
  - [Task](#task)
  - [HandlerFunc and Mux](#handlerfunc-and-mux)
  - [Store](#store)
  - [Metrics](#metrics)
  - [Options](#options)
- [What pulse deliberately doesn't do](#what-pulse-deliberately-doesnt-do)
- [Reliability](#reliability)
- [Examples](#examples)
- [Reference app](#reference-app)

---

## Why

Every background-job system needs the same handful of unglamorous pieces —
priority ordering, retries with backoff, a dead-letter queue, delayed
execution, graceful shutdown — before you can write a single line of your
actual task logic. Writing that plumbing yourself is easy to get wrong in
ways that only show up under load (lost updates, races between the poller
and the enqueuer, retries that don't back off, a DLQ nobody's watching).

`pulse` is that plumbing, isolated from any specific business logic,
database, or even a specific queue backend. The interfaces
(`queue.Queue`, `worker.Store`, `worker.HandlerFunc`) are deliberately small
— derived from what the worker pool's retry/DLQ loop actually needs, not
guessed upfront — so a new backend or storage layer is a handful of methods,
not a rewrite.

## Install

```bash
go get github.com/isaacthajunior/pulse@v0.1.0
```

## Quickstart

No Redis, no database — this runs standalone:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/isaacthajunior/pulse/queue/memqueue"
	"github.com/isaacthajunior/pulse/worker"
	"github.com/isaacthajunior/pulse/worker/memstore"
)

func main() {
	q := memqueue.New()
	store := memstore.New()
	mux := worker.NewMux()

	mux.Handle("uppercase", func(_ context.Context, task worker.Task) error {
		fmt.Println(strings.ToUpper(string(task.Payload)))
		return nil
	})

	pool := worker.NewPool(q, store, mux, 3, slog.New(slog.NewTextHandler(os.Stdout, nil)))
	pool.Start()
	defer pool.Stop()

	store.Put(worker.Task{ID: "task-1", Type: "uppercase", Payload: []byte("hello pulse"), Priority: "medium"})
	q.EnqueueWithPriority("task-1", "medium")
}
```

The full runnable version is in [`examples/simple`](examples/simple) —
`go run ./examples/simple`.

For production, swap `memqueue` for `queue/redisqueue` and `memstore` for
your own `worker.Store` implementation (Postgres, MySQL, SQLite — whatever
you already use):

```go
import "github.com/isaacthajunior/pulse/queue/redisqueue"

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
q := redisqueue.NewRedisQueue(client)
```

## Packages

| Package | What it is |
|---|---|
| [`queue`](queue) | The `Queue` interface every backend implements: `EnqueueWithPriority`, `DequeuePriorityBlocking`, `Schedule`, `PromoteScheduled`, DLQ operations, queue depths. |
| [`queue/redisqueue`](queue/redisqueue) | Redis-backed `Queue`. The production backend — lists for priority classes and the DLQ, a sorted set for scheduled tasks. |
| [`queue/memqueue`](queue/memqueue) | In-process, mutex-protected `Queue`. No external infra. For tests, local dev, or running `pulse` without standing up Redis. Polls rather than blocking on a true wakeup signal — not a high-throughput primitive. |
| [`worker`](worker) | `Pool` — the engine. Dequeues tasks, dispatches to handlers registered on a `Mux`, retries with backoff, moves exhausted tasks to the DLQ. Also defines `Store`, `Metrics`, `Task`, `WorkerHealthStats`. |
| [`worker/memstore`](worker/memstore) | In-memory, map-backed `Store`. Same pairing as `memqueue` — tests and local dev without a real database. |
| [`examples/simple`](examples/simple) | A minimal, runnable, infra-free consumer — the fastest way to see `pulse` work. |

## Core concepts

### Task

```go
type Task struct {
	ID       string
	Type     string
	Payload  json.RawMessage
	Priority string
	Status   string            // e.g. "pending", "cancelled" — pool skips cancelled tasks
	Metadata map[string]string // generic bag for cross-cutting concerns
}
```

`Metadata` is how app-specific, cross-cutting data rides along without
`pulse` needing to know what it means. The pool itself only recognizes one
convention: a `"trace_context"` key, treated as a W3C traceparent header, so
a trace can continue across the queue boundary if you set it. Everything
else in `Metadata` — chaining hints, tenant IDs, whatever your app needs —
is opaque to `pulse` and meaningful only to your own code.

### HandlerFunc and Mux

```go
type HandlerFunc func(ctx context.Context, task Task) error

mux := worker.NewMux()
mux.Handle("send_email", func(ctx context.Context, task worker.Task) error {
	// do the work; a non-nil error triggers a retry (and eventually the DLQ)
	return nil
})
```

One function per task type. No hardcoded switch statement to extend, no
task types `pulse` needs to know about in advance.

### Store

`Pool`'s retry/DLQ loop needs exactly three things from your persistence
layer — nothing about results, nothing about your schema:

```go
type Store interface {
	GetTask(ctx context.Context, id string) (Task, error)
	UpdateStatus(ctx context.Context, id, status string) error
	RecordAttempt(ctx context.Context, id, status string, attempt int, errMsg string) error
}
```

Saving a task's result, or creating and enqueueing a follow-up task, is
ordinary code inside your own handler — using your own storage and the same
`Queue` you gave the pool. **`pulse` has no concept of task chaining.** If
you want it, write it as a small wrapper around `HandlerFunc` in your own
app (see [the reference app's `internal/chaining`](#reference-app) for a
worked example) — that keeps chaining semantics entirely out of the
library, where they don't belong for every consumer.

### Metrics

Optional, no-op by default:

```go
type Metrics interface {
	TaskProcessed(taskType string)
	TaskRetried(taskType string)
	TaskFailed(taskType string)
	TaskDuration(seconds float64)
	QueueDepth(queueName string, depth int64)
}
```

Wire in Prometheus, StatsD, or anything else via `worker.WithMetrics(...)`.
Tracing is not made pluggable the same way — `pulse` calls the OpenTelemetry
API directly, which already no-ops safely if you never configure a
`TracerProvider`, so a custom interface would add indirection without
adding real decoupling.

### Options

```go
worker.NewPool(q, store, mux, workers, logger,
	worker.WithMaxRetries(10),         // default 5
	worker.WithBaseDelay(2*time.Second), // default 1s, exponential: 1s→2s→4s→8s→16s
	worker.WithMetrics(myPrometheusAdapter{}),
)
```

`Pool` also implements `WorkerHealthProvider`:

```go
type WorkerHealthStats struct {
	TotalWorkers, ActiveWorkers, IdleWorkers int / int32
	TotalProcessed, TotalFailed              int64
	UptimeSeconds                            int64
}

stats := pool.HealthStats()
```

## What pulse deliberately doesn't do

- **No task chaining.** See [Store](#store) above.
- **No HTTP API.** `pulse` is a library, not a service.
- **No specific database integration.** `Store` is 3 methods; implement it
  against whatever you already use.
- **No pluggable tracing.** OTel is called directly; it already no-ops
  without a configured provider.
- **No built-in dashboard — yet.** A cross-backend job dashboard
  ("Grafana for background jobs," watching Redis via `pulse` alongside
  other queue backends) is the eventual direction, but doesn't exist today.
  Don't build against it; it's not here.

## Reliability

- Unit tests cover the pool's core state machine: success, retry-then-succeed,
  retry-exhaustion → DLQ, and cancelled-task skip (`worker/pool_test.go`).
- A concurrency stress test (`worker/concurrency_test.go`) runs 2,000 tasks
  across 16 workers, enqueued from many goroutines *while the pool is
  already running*, under `go test -race` — exercising `Queue`/`Store`
  under real concurrent load, not just this library's own low-throughput
  usage.
- `queue/redisqueue` has integration tests against a real Redis
  (`testcontainers-go`), including a concurrent-enqueue test.
- Benchmarks for handler dispatch and the full retry pipeline live in
  `worker/pool_bench_test.go`.

Run everything: `go test ./... -race` (needs Docker for the Redis
integration tests; everything else is infra-free).

## Examples

`go run ./examples/simple` — the quickstart above, runnable as-is.

## Reference app

`pulse` was extracted from **[event-processing-platform](https://github.com/IsaacThaJunior/event-processing-platform)**,
a real task-processing API (HTTP submission, Postgres persistence, MinIO
file storage, image resizing/URL scraping/report generation as task types)
that now imports `pulse` for everything generic instead of implementing it
inline. That app is the proving ground for `pulse`'s design, including the
`internal/chaining` pattern referenced above for adding declarative task
chaining on top of `pulse` without the library needing to know about it.
