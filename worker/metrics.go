package worker

// Metrics receives observability callbacks from the Pool. Implementations
// typically forward these to Prometheus, StatsD, or similar. Use
// NoopMetrics (the default) to opt out entirely.
type Metrics interface {
	TaskProcessed(taskType string)
	TaskRetried(taskType string)
	TaskFailed(taskType string)
	TaskDuration(seconds float64)
	QueueDepth(queueName string, depth int64)
}

// NoopMetrics discards every call. It's the Pool's default Metrics
// implementation.
type NoopMetrics struct{}

func (NoopMetrics) TaskProcessed(taskType string)            {}
func (NoopMetrics) TaskRetried(taskType string)              {}
func (NoopMetrics) TaskFailed(taskType string)               {}
func (NoopMetrics) TaskDuration(seconds float64)             {}
func (NoopMetrics) QueueDepth(queueName string, depth int64) {}
