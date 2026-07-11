// Package worker provides a generic, backend-agnostic job-processing
// engine: a Pool dequeues tasks from a queue.Queue, dispatches them to
// handlers registered on a Mux, and tracks status/attempts through a Store.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"github.com/isaacthajunior/pulse/queue"
)

var tracer = otel.Tracer("worker")

const (
	defaultMaxRetries = 5
	defaultBaseDelay  = time.Second
)

// Pool dequeues tasks from a queue.Queue and runs them through handlers
// registered on a Mux, with retry-with-backoff and a dead-letter queue for
// tasks that exhaust their retries.
type Pool struct {
	queue   queue.Queue
	store   Store
	mux     *Mux
	workers int
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	logger  *slog.Logger
	metrics Metrics

	maxRetries int
	baseDelay  time.Duration

	activeWorkers  atomic.Int32
	totalProcessed atomic.Int64
	totalFailed    atomic.Int64
	startTime      time.Time
}

// Option configures a Pool at construction time.
type Option func(*Pool)

// WithMaxRetries overrides the default of 5 attempts before a task is sent
// to the dead-letter queue.
func WithMaxRetries(n int) Option {
	return func(p *Pool) { p.maxRetries = n }
}

// WithBaseDelay overrides the default 1-second base for exponential
// backoff between retries.
func WithBaseDelay(d time.Duration) Option {
	return func(p *Pool) { p.baseDelay = d }
}

// WithMetrics wires an observability sink. Without this option, all
// metrics calls are no-ops.
func WithMetrics(m Metrics) Option {
	return func(p *Pool) { p.metrics = m }
}

// NewPool builds a Pool. Call Start to begin processing and Stop to drain
// and shut down.
func NewPool(q queue.Queue, store Store, mux *Mux, workers int, logger *slog.Logger, opts ...Option) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		queue:      q,
		store:      store,
		mux:        mux,
		workers:    workers,
		ctx:        ctx,
		cancel:     cancel,
		logger:     logger,
		metrics:    NoopMetrics{},
		maxRetries: defaultMaxRetries,
		baseDelay:  defaultBaseDelay,
		startTime:  time.Now(),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// HealthStats returns a snapshot of the pool's current activity.
func (p *Pool) HealthStats() WorkerHealthStats {
	active := p.activeWorkers.Load()
	return WorkerHealthStats{
		TotalWorkers:   p.workers,
		ActiveWorkers:  active,
		IdleWorkers:    int32(p.workers) - active,
		TotalProcessed: p.totalProcessed.Load(),
		TotalFailed:    p.totalFailed.Load(),
		UptimeSeconds:  int64(time.Since(p.startTime).Seconds()),
	}
}

// Start launches the scheduler, DLQ monitor, and worker goroutines. It
// returns immediately; processing happens in the background.
func (p *Pool) Start() {
	p.wg.Add(1)
	go p.scheduler()

	p.wg.Add(1)
	go p.dlqMonitor()

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// Stop signals all goroutines to exit and blocks until they do.
func (p *Pool) Stop() {
	p.cancel()
	p.wg.Wait()
}

func (p *Pool) scheduler() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			if err := p.queue.PromoteScheduled(); err != nil {
				p.logger.Debug("scheduler: failed to promote scheduled tasks", "error", err)
			}
		}
	}
}

// dlqMonitor polls queue depths every 30s, reports them via Metrics, and
// logs a warning whenever the DLQ is non-empty — a growing DLQ signals
// systemic failure.
func (p *Pool) dlqMonitor() {
	defer p.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			depths, err := p.queue.GetQueueDepths()
			if err != nil {
				p.logger.Debug("dlqMonitor: failed to get queue depths", "error", err)
				continue
			}
			for queueName, depth := range depths {
				p.metrics.QueueDepth(queueName, depth)
			}
			if dlq := depths["dlq"]; dlq > 0 {
				p.logger.Warn("DLQ is non-empty — tasks have exhausted all retries and need attention",
					"dlq_depth", dlq)
			}
		}
	}
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()
	p.logger.Debug("worker started", "worker_id", id)

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug("worker stopping", "worker_id", id)
			return
		default:
			taskID, queueName, err := p.queue.DequeuePriorityBlocking(5 * time.Second)
			if err != nil {
				p.logger.Debug("failed to dequeue", "error", err)
				continue
			}
			if taskID == "" {
				continue
			}
			p.processWithRetry(taskID, id, queueName)
		}
	}
}

func (p *Pool) processWithRetry(taskID string, workerID int, queueName string) {
	p.activeWorkers.Add(1)
	defer p.activeWorkers.Add(-1)

	ctx := context.Background()
	start := time.Now()

	var task Task
	var lastErr error

	for attempt := 1; attempt <= p.maxRetries; attempt++ {
		t, err := p.store.GetTask(ctx, taskID)
		if err != nil {
			p.logger.Debug("get task failed", "task_id", taskID, "attempt", attempt, "error", err)
			time.Sleep(p.baseDelay * time.Duration(1<<(attempt-1)))
			continue
		}
		task = t

		// Restore trace context from the caller so this span appears in
		// the same trace, if one was propagated via Task.Metadata.
		spanCtx := ctx
		if traceparent := task.Metadata["trace_context"]; traceparent != "" {
			carrier := propagation.MapCarrier{"traceparent": traceparent}
			spanCtx = otel.GetTextMapPropagator().Extract(ctx, carrier)
		}
		spanCtx, span := tracer.Start(spanCtx, "worker.process-task")
		span.SetAttributes(
			attribute.String("task.id", taskID),
			attribute.String("task.type", task.Type),
			attribute.String("task.priority", task.Priority),
			attribute.String("queue", queueName),
			attribute.Int("worker.id", workerID),
			attribute.Int("attempt", attempt),
		)
		traceID := span.SpanContext().TraceID().String()

		if task.Status == "cancelled" {
			p.logger.Debug("task cancelled, skipping", "task_id", taskID, "trace_id", traceID)
			span.End()
			return
		}

		handlerFn, ok := p.mux.handler(task.Type)
		if !ok {
			err = fmt.Errorf("no handler registered for task type %q", task.Type)
		} else {
			execStart := time.Now()
			err = handlerFn(spanCtx, task)
			p.metrics.TaskDuration(time.Since(execStart).Seconds())
		}

		if err == nil {
			span.End()

			if uErr := p.store.UpdateStatus(ctx, taskID, "processed"); uErr != nil {
				p.logger.Debug("update status failed", "task_id", taskID, "error", uErr)
			}
			p.totalProcessed.Add(1)
			p.metrics.TaskProcessed(task.Type)

			if rErr := p.store.RecordAttempt(ctx, taskID, "processed", attempt, ""); rErr != nil {
				p.logger.Debug("record attempt failed", "task_id", taskID, "error", rErr)
			}

			p.logger.Info("task processed",
				"task_id", taskID,
				"worker_id", workerID,
				"queue", queueName,
				"task_type", task.Type,
				"priority", task.Priority,
				"attempt", attempt,
				"trace_id", traceID,
				slog.Duration("duration", time.Since(start)),
			)
			return
		}

		lastErr = err
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()

		p.metrics.TaskRetried(task.Type)
		if rErr := p.store.RecordAttempt(ctx, taskID, "retry", attempt, err.Error()); rErr != nil {
			p.logger.Debug("record retry attempt failed", "task_id", taskID, "error", rErr)
		}

		time.Sleep(p.baseDelay * time.Duration(1<<(attempt-1)))
	}

	// Max retries exhausted → Dead Letter Queue
	p.totalFailed.Add(1)
	p.metrics.TaskFailed(task.Type)

	if err := p.store.UpdateStatus(ctx, taskID, "failed"); err != nil {
		p.logger.Debug("update status failed", "task_id", taskID, "error", err)
	}
	if err := p.store.RecordAttempt(ctx, taskID, "failed", p.maxRetries, "max retries exceeded"); err != nil {
		p.logger.Debug("record final failure failed", "task_id", taskID, "error", err)
	}
	if err := p.queue.EnqueueToDLQ(taskID); err != nil {
		p.logger.Error("enqueue to DLQ failed", "task_id", taskID, "error", err)
	}

	p.logger.Warn("task failed",
		"task_id", taskID,
		"worker_id", workerID,
		"queue", queueName,
		"task_type", task.Type,
		"error", lastErr,
		slog.Duration("duration", time.Since(start)),
	)
}
