package monitoring

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// SQS HT-FIFO partition action labels. Stable string set so
// dashboards / alerts can rely on the values not changing.
const (
	SQSPartitionActionSend    = "send"
	SQSPartitionActionReceive = "receive"
	SQSPartitionActionDelete  = "delete"

	// sqsMaxTrackedQueues caps the number of distinct queue names
	// the metrics layer will emit a per-(queue, partition, action)
	// series for. Any queue beyond this cap collapses to the
	// _other label so a misbehaving caller (e.g. a script that
	// generates random queue names) cannot blow up the
	// Prometheus cardinality budget. Mirrors dynamoMaxTrackedTables.
	sqsMaxTrackedQueues = 512

	// sqsQueueOverflow is the placeholder label used when a queue
	// name is not in the tracked set (cap exceeded). Operators see
	// the overflow as a single _other series and know to look at
	// the application logs for the real names.
	sqsQueueOverflow = "_other"
)

// SQSPartitionObserver records per-(queue, partition, action)
// counters for HT-FIFO operations. The interface is small so
// adapter call sites can pass a no-op observer in tests without
// pulling in the full Prometheus registry.
type SQSPartitionObserver interface {
	// ObservePartitionMessage increments the
	// sqs_partition_messages_total counter for one operation on
	// one (queue, partition) pair. Action must be one of
	// SQSPartitionActionSend / Receive / Delete; any other value
	// is silently dropped so a typo at a future call site cannot
	// crash the process.
	ObservePartitionMessage(queue string, partition uint32, action string)
}

// SQSMetrics owns the Prometheus counter for HT-FIFO partition
// operations. Mirrors DynamoDBMetrics' shape: per-Registry
// instance, label-cardinality-bounded by sqsMaxTrackedQueues.
type SQSMetrics struct {
	partitionMessages *prometheus.CounterVec

	mu            sync.Mutex
	trackedQueues map[string]struct{}
}

func newSQSMetrics(registerer prometheus.Registerer) *SQSMetrics {
	m := &SQSMetrics{
		partitionMessages: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_sqs_partition_messages_total",
				Help: "Total HT-FIFO partition operations by queue, partition, and action (send / receive / delete). Non-zero only for queues with PartitionCount > 1; use to spot uneven MessageGroupId distributions across partitions.",
			},
			[]string{"queue", "partition", "action"},
		),
		trackedQueues: map[string]struct{}{},
	}
	registerer.MustRegister(m.partitionMessages)
	return m
}

// ObservePartitionMessage implements SQSPartitionObserver. The
// (queue, action) pair is validated and (queue) is collapsed to
// the overflow label past sqsMaxTrackedQueues distinct names.
func (m *SQSMetrics) ObservePartitionMessage(queue string, partition uint32, action string) {
	if m == nil {
		return
	}
	if !sqsValidPartitionAction(action) {
		return
	}
	if queue == "" {
		// Defensive: an empty queue name would collapse all
		// requests onto a single series — almost certainly a bug
		// at the call site. Drop silently rather than emit
		// poisoned data.
		return
	}
	queueLabel := m.queueLabelForCardinalityBudget(queue)
	// WithLabelValues avoids the prometheus.Labels map allocation
	// on every observe call. Label order matches the
	// NewCounterVec declaration: queue, partition, action.
	// Mirrors DynamoDBMetrics.
	m.partitionMessages.WithLabelValues(
		queueLabel,
		strconv.FormatUint(uint64(partition), 10),
		action,
	).Inc()
}

// queueLabelForCardinalityBudget returns queue if the metric has
// already emitted a series for it OR there is room in the
// tracked-queues set; returns sqsQueueOverflow otherwise. The
// cap-and-collapse pattern mirrors DynamoDBMetrics.tableLabel
// so a misbehaving caller cannot exhaust the Prometheus
// cardinality budget.
func (m *SQSMetrics) queueLabelForCardinalityBudget(queue string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.trackedQueues[queue]; ok {
		return queue
	}
	if len(m.trackedQueues) >= sqsMaxTrackedQueues {
		return sqsQueueOverflow
	}
	m.trackedQueues[queue] = struct{}{}
	return queue
}

// sqsValidPartitionAction returns true iff action is one of the
// stable label values. Keeps a typo at the call site (e.g.
// "Send" vs "send") from polluting the metric.
func sqsValidPartitionAction(action string) bool {
	switch action {
	case SQSPartitionActionSend, SQSPartitionActionReceive, SQSPartitionActionDelete:
		return true
	}
	return false
}
