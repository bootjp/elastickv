package monitoring

import (
	"context"
	"strconv"
	"sync"
	"time"

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

// SQSDepthSource is the contract a per-tick queue-depth source must
// satisfy. Implemented by *adapter.SQSServer; SQSObserver.Start
// calls SnapshotQueueDepths on every interval and writes the
// returned slice to the elastickv_sqs_queue_messages gauges.
//
// Mirrors the Raft observer's StatusReader / ConfigReader pattern
// (monitoring/raft.go): the source returns ready-to-use snapshots
// and the observer owns the gauge state machine (forget-on-disappear,
// cardinality cap). Implementations return an empty slice — not an
// error — when this node is a follower, so the dashboard's gauge
// set always mirrors what the leader's catalog scan would report.
type SQSDepthSource interface {
	SnapshotQueueDepths(ctx context.Context) []SQSQueueDepth
}

// SQSQueueDepth is one queue's depth-attribute snapshot. Mirrors
// adapter.SQSQueueDepth byte-for-byte and is re-declared here to
// keep the monitoring package free of an adapter import. A drift
// between the two definitions surfaces as a compile error at the
// SQSObserver call site.
type SQSQueueDepth struct {
	Queue      string
	Visible    int64
	NotVisible int64
	Delayed    int64
}

// SQSMetrics owns the Prometheus collectors for the SQS adapter.
// Mirrors DynamoDBMetrics' shape: per-Registry instance, label-
// cardinality-bounded by sqsMaxTrackedQueues, and split between
// counters (HT-FIFO partition activity) and gauges (queue depth).
type SQSMetrics struct {
	partitionMessages *prometheus.CounterVec
	queueDepth        *prometheus.GaugeVec

	mu            sync.Mutex
	trackedQueues map[string]struct{}
}

// SQS depth gauge state-label values. Stable so dashboards / alerts
// can hard-code state="visible" et al.
const (
	sqsQueueStateVisible    = "visible"
	sqsQueueStateNotVisible = "not_visible"
	sqsQueueStateDelayed    = "delayed"
)

func newSQSMetrics(registerer prometheus.Registerer) *SQSMetrics {
	m := &SQSMetrics{
		partitionMessages: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_sqs_partition_messages_total",
				Help: "Total HT-FIFO partition operations by queue, partition, and action (send / receive / delete). Non-zero only for queues with PartitionCount > 1; use to spot uneven MessageGroupId distributions across partitions.",
			},
			[]string{"queue", "partition", "action"},
		),
		queueDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_sqs_queue_messages",
				Help: "Approximate number of messages in each SQS queue, broken down by state. visible = ApproximateNumberOfMessages, not_visible = ApproximateNumberOfMessagesNotVisible, delayed = ApproximateNumberOfMessagesDelayed. Updated periodically by the leader's queue-depth scraper; followers report no value.",
			},
			[]string{"queue", "state"},
		),
		trackedQueues: map[string]struct{}{},
	}
	registerer.MustRegister(m.partitionMessages)
	registerer.MustRegister(m.queueDepth)
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

// ObserveQueueDepth implements SQSDepthObserver. Updates the three
// state-labelled gauges for queue. Negative values are clamped to 0
// so a transient scan failure (returning -1 sentinel from a future
// caller) cannot blast a fake backlog onto the dashboard.
func (m *SQSMetrics) ObserveQueueDepth(queue string, visible, notVisible, delayed int64) {
	if m == nil {
		return
	}
	if queue == "" {
		return
	}
	queueLabel := m.queueLabelForCardinalityBudget(queue)
	m.queueDepth.WithLabelValues(queueLabel, sqsQueueStateVisible).Set(float64(max(int64(0), visible)))
	m.queueDepth.WithLabelValues(queueLabel, sqsQueueStateNotVisible).Set(float64(max(int64(0), notVisible)))
	m.queueDepth.WithLabelValues(queueLabel, sqsQueueStateDelayed).Set(float64(max(int64(0), delayed)))
}

// ForgetQueue drops the three gauge series for a queue and frees
// its cardinality-budget slot so a long-running deployment that
// regularly creates and deletes queues (CI workloads, ephemeral
// per-job queues) doesn't permanently wedge the 512-entry budget.
// Without the trackedQueues cleanup, post-cap new queues would
// silently collapse onto the _other label even after their
// predecessors had been deleted.
//
// The (queue, partition, action) counter series stays —
// cumulative-by-design and disappearing it would mask retention-
// period activity. Queues that hit the cap and mapped to _other
// have no individual series to delete; we detect the not-tracked
// case and skip the DeleteLabelValues calls so we don't tear down
// the shared _other series for an unrelated queue.
//
// Caller-audit per the standing semantic-change rule: only
// SQSObserver.observeOnce calls this (registry plumbing aside),
// and it's invoked exactly when a queue is observed in the
// previous tick but not the current one — symmetric with
// ObserveQueueDepth's add side, so there is no path that calls
// ForgetQueue without a matching prior tracked-queue insert.
func (m *SQSMetrics) ForgetQueue(queue string) {
	if m == nil || queue == "" {
		return
	}
	m.mu.Lock()
	_, tracked := m.trackedQueues[queue]
	if tracked {
		delete(m.trackedQueues, queue)
	}
	m.mu.Unlock()
	if !tracked {
		// Queue was either never observed or had been collapsed onto
		// the _other overflow label. Either way: no per-queue series
		// to remove, and we must NOT delete the _other series here
		// because other overflow queues may still be sharing it.
		return
	}
	m.queueDepth.DeleteLabelValues(queue, sqsQueueStateVisible)
	m.queueDepth.DeleteLabelValues(queue, sqsQueueStateNotVisible)
	m.queueDepth.DeleteLabelValues(queue, sqsQueueStateDelayed)
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

// sqsDepthObserveInterval is the default tick cadence for
// SQSObserver. 30 s mirrors sqsReaperInterval — dashboards are
// rarely refreshed faster, and tighter ticks just add catalog-scan
// load on the leader for no observable benefit.
const sqsDepthObserveInterval = 30 * time.Second

// SQSObserver polls a SQSDepthSource on a fixed cadence and writes
// the result into the SQSMetrics gauge. Same shape as RaftObserver
// (monitoring/raft.go): the observer owns the state machine
// (current-vs-previous queue diff for ForgetQueue) and the source
// just returns ready-to-use snapshots. The observer is nil-tolerant
// at every entrypoint so test fixtures and metrics-disabled
// deployments can no-op without a defensive nil check.
type SQSObserver struct {
	metrics *SQSMetrics

	mu       sync.Mutex
	lastSeen map[string]struct{}
}

func newSQSObserver(metrics *SQSMetrics) *SQSObserver {
	return &SQSObserver{
		metrics:  metrics,
		lastSeen: map[string]struct{}{},
	}
}

// Start kicks off a background ticker that polls source every
// interval (defaulting to sqsDepthObserveInterval when zero) until
// ctx is canceled. The first observation runs synchronously so
// /metrics has fresh data on the first scrape; subsequent ticks
// run on the goroutine.
func (o *SQSObserver) Start(ctx context.Context, source SQSDepthSource, interval time.Duration) {
	if o == nil || source == nil {
		return
	}
	if interval <= 0 {
		interval = sqsDepthObserveInterval
	}
	o.observeOnce(ctx, source)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.observeOnce(ctx, source)
			}
		}
	}()
}

// ObserveOnce captures the latest depth snapshot synchronously.
// Mirrors RaftObserver.ObserveOnce; intended for tests that want
// deterministic single-tick behaviour without spinning up a ticker.
func (o *SQSObserver) ObserveOnce(source SQSDepthSource) {
	o.observeOnce(context.Background(), source)
}

// observeOnce assumes a single-writer contract: in production the
// only caller is the goroutine launched from Start, and tests use
// ObserveOnce serially. ObserveQueueDepth runs unlocked because the
// CounterVec / GaugeVec writes are individually atomic; the
// trackedQueues mutation inside is guarded by m.mu. Concurrent
// observeOnce invocations would race only on the lastSeen diff
// (held briefly under o.mu below), so a future caller that
// violates the single-writer rule would at worst double-emit a
// gauge — never a panic.
func (o *SQSObserver) observeOnce(ctx context.Context, source SQSDepthSource) {
	if o == nil || o.metrics == nil || source == nil {
		return
	}
	snaps := source.SnapshotQueueDepths(ctx)
	current := make(map[string]struct{}, len(snaps))
	for _, snap := range snaps {
		o.metrics.ObserveQueueDepth(snap.Queue, snap.Visible, snap.NotVisible, snap.Delayed)
		current[snap.Queue] = struct{}{}
	}
	// Diff against the previous tick: any queue that disappeared
	// (DeleteQueue, tombstoned cohort fully drained, leader stepped
	// down — source returned []) gets its gauge series dropped so
	// dashboards don't show a frozen backlog.
	o.mu.Lock()
	for prev := range o.lastSeen {
		if _, ok := current[prev]; !ok {
			o.metrics.ForgetQueue(prev)
		}
	}
	o.lastSeen = current
	o.mu.Unlock()
}
