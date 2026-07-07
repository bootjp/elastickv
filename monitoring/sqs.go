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

	SQSThrottleActionSend    = "send"
	SQSThrottleActionReceive = "receive"
	SQSThrottleActionDefault = "default"

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

// SQSThrottleObserver records per-queue throttle decisions. The SQS
// adapter calls it after a configured token-bucket charge so operators
// can observe both rejected requests and the live token balance.
type SQSThrottleObserver interface {
	ObserveThrottleDecision(queue string, action string, tokensRemaining float64, throttled bool)
}

// SQSDepthSource is the contract a per-tick queue-depth source must
// satisfy. Implemented by *adapter.SQSServer; SQSObserver.Start
// calls SnapshotQueueDepths on every interval and writes the
// returned slice to the elastickv_sqs_queue_messages gauges.
//
// Mirrors the Raft observer's StatusReader / ConfigReader pattern
// (monitoring/raft.go): the source returns ready-to-use snapshots
// and the observer owns the gauge state machine (forget-on-disappear,
// cardinality cap).
//
// Two distinct empty-snapshot states, signalled by ok:
//
//   - ok=true with an empty/nil slice — "this source legitimately
//     has no queues this tick". Triggers when the node is a
//     follower (leader-only emission) or the leader genuinely has
//     zero queues configured. The observer diffs against the
//     previous tick and ForgetQueue's any queue that disappeared
//     so a former leader's gauges are cleared on step-down.
//
//   - ok=false (regardless of the slice contents) — "the source
//     could not produce a snapshot this tick" (transient catalog-
//     read failure on the leader, context cancelled mid-scan).
//     The observer must skip the diff entirely: leave existing
//     gauges in place AND leave lastSeen untouched so the next
//     successful tick can still diff against the previous good
//     state. Without this branch a single failed scrape would
//     wipe every depth gauge and produce a false "all queues
//     drained" event on the dashboard until the next successful
//     tick.
type SQSDepthSource interface {
	SnapshotQueueDepths(ctx context.Context) (snaps []SQSQueueDepth, ok bool)
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
//
// The cardinality budget is split into two independent maps because
// the two metrics have different deletion semantics:
//
//   - partitionMessages is a CounterVec. Counters are cumulative;
//     deleting a series throws away its observed-since-process-start
//     value, so we never call DeleteLabelValues on it. The counter
//     budget therefore only ever grows — once a queue is admitted to
//     trackedCounterQueues it stays admitted, and ForgetQueue does
//     NOT touch this map.
//   - queueDepth is a GaugeVec. Gauges have no cumulative state, so
//     DeleteLabelValues is safe (and necessary, otherwise a deleted
//     queue keeps reporting a frozen backlog on the dashboard).
//     ForgetQueue both removes the gauge series and frees the
//     queue's slot in trackedDepthQueues so a churn-heavy deployment
//     can reuse the budget.
//
// Sharing one map across the two metrics regresses the counter cap:
// ForgetQueue would free a slot, a new queue would be admitted, and
// the previous queue's counter series would still occupy a real-name
// label in Prometheus — letting cardinality grow without bound under
// queue churn (or after a leader step-down clears the observer's
// lastSeen). This was the P1 finding on PR #743.
type SQSMetrics struct {
	partitionMessages *prometheus.CounterVec
	queueDepth        *prometheus.GaugeVec
	throttledRequests *prometheus.CounterVec
	throttleTokens    *prometheus.GaugeVec

	mu                         sync.Mutex
	trackedCounterQueues       map[string]struct{}
	trackedDepthQueues         map[string]struct{}
	trackedThrottleGaugeQueues map[string]struct{}
	// overflowDepthQueues is the set of queue names whose depth
	// gauge is currently collapsed onto the shared sqsQueueOverflow
	// label. Tracked separately from trackedDepthQueues (which
	// only holds real-name admissions) so ForgetQueue can ref-count
	// overflow queues and drop the shared _other gauge once the
	// last overflow queue disappears. Without this, churn-heavy
	// deployments with >sqsMaxTrackedQueues distinct queues leave
	// the dashboard pinned at whichever overflow queue last
	// reported, even after every overflow queue has been deleted.
	// Counter side has no equivalent map: counter series are
	// cumulative (never deleted), so the _other counter can't
	// produce phantom data — it correctly reflects the cumulative
	// count of all overflow operations, even from queues that no
	// longer exist.
	overflowDepthQueues         map[string]struct{}
	overflowThrottleGaugeQueues map[string]struct{}
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
		throttledRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_sqs_throttled_requests_total",
				Help: "Total SQS requests rejected by per-queue throttling, labelled by queue and effective throttle bucket action.",
			},
			[]string{"queue", "action"},
		),
		throttleTokens: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_sqs_throttle_tokens_remaining",
				Help: "Remaining tokens after the most recent SQS per-queue throttle decision, labelled by queue and effective throttle bucket action.",
			},
			[]string{"queue", "action"},
		),
		trackedCounterQueues:        map[string]struct{}{},
		trackedDepthQueues:          map[string]struct{}{},
		trackedThrottleGaugeQueues:  map[string]struct{}{},
		overflowDepthQueues:         map[string]struct{}{},
		overflowThrottleGaugeQueues: map[string]struct{}{},
	}
	registerer.MustRegister(m.partitionMessages)
	registerer.MustRegister(m.queueDepth)
	registerer.MustRegister(m.throttledRequests)
	registerer.MustRegister(m.throttleTokens)
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
	queueLabel := m.admitForCounterBudget(queue)
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
	queueLabel := m.admitForDepthBudget(queue)
	m.queueDepth.WithLabelValues(queueLabel, sqsQueueStateVisible).Set(float64(max(int64(0), visible)))
	m.queueDepth.WithLabelValues(queueLabel, sqsQueueStateNotVisible).Set(float64(max(int64(0), notVisible)))
	m.queueDepth.WithLabelValues(queueLabel, sqsQueueStateDelayed).Set(float64(max(int64(0), delayed)))
}

// ObserveThrottleDecision implements SQSThrottleObserver. It updates
// the current token-balance gauge for every configured bucket decision
// and increments the rejected-request counter only when throttled=true.
func (m *SQSMetrics) ObserveThrottleDecision(queue string, action string, tokensRemaining float64, throttled bool) {
	if m == nil || queue == "" {
		return
	}
	if !sqsValidThrottleAction(action) {
		return
	}
	if tokensRemaining < 0 {
		tokensRemaining = 0
	}
	if throttled {
		queueCounterLabel := m.admitForCounterBudget(queue)
		m.throttledRequests.WithLabelValues(queueCounterLabel, action).Inc()
	}
	queueGaugeLabel := m.admitForThrottleGaugeBudget(queue)
	m.throttleTokens.WithLabelValues(queueGaugeLabel, action).Set(tokensRemaining)
}

// ForgetQueue drops the three gauge series for a queue and frees
// its slot in the depth-side cardinality budget so a long-running
// deployment that regularly creates and deletes queues (CI
// workloads, ephemeral per-job queues) doesn't permanently wedge
// the 512-entry depth budget.
//
// Three cases, by membership at call time:
//
//  1. Queue is in trackedDepthQueues (admitted under its real name):
//     drop the three state-labelled series and free the budget slot.
//  2. Queue is in overflowDepthQueues (admitted, then collapsed
//     onto the shared sqsQueueOverflow / "_other" label because the
//     budget was saturated): remove from the overflow set. If that
//     leaves the overflow set empty, drop the three _other series
//     too — there's no remaining queue reporting into them, so the
//     gauge would otherwise pin phantom backlog. While the overflow
//     set is still non-empty the _other series stays put: tearing
//     it down would zero out values that other overflow queues are
//     legitimately maintaining.
//  3. Queue is in neither map (never depth-observed): no-op.
//
// The (queue, partition, action) counter series stays in all three
// cases — cumulative-by-design — and the queue's slot in
// trackedCounterQueues stays consumed. Reclaiming the counter slot
// would let a later queue be admitted under its real name while the
// original counter series still sat in Prometheus, which would let
// counter cardinality grow past sqsMaxTrackedQueues under churn or
// after a leader step-down clears the observer's lastSeen (the P1
// finding on PR #743). Counter-side _other has no equivalent of
// the gauge phantom-backlog problem either, because cumulative
// counters legitimately reflect total operations from queues that
// have since been deleted.
//
// Caller-audit per the standing semantic-change rule: only
// SQSObserver.observeOnce calls this (registry plumbing aside),
// and it's invoked exactly when a queue is observed in the
// previous tick but not the current one. The caller's contract —
// "drop gauges for a queue that disappeared so dashboards don't
// show frozen backlog" — is preserved AND extended consistently:
// overflow queues, previously a silent no-op, now also stop
// pinning the shared gauge once the last one disappears. The
// narrowed-but-still-correct scope (counter budget never
// reclaimed) is invisible to observeOnce because the observer
// only writes gauges; counters are fed via ObservePartitionMessage
// from a different code path entirely.
func (m *SQSMetrics) ForgetQueue(queue string) {
	if m == nil || queue == "" {
		return
	}
	m.mu.Lock()
	depthForget := m.forgetDepthQueueLocked(queue)
	throttleForget := m.forgetThrottleQueueLocked(queue)
	m.mu.Unlock()
	m.dropForgottenDepthQueue(queue, depthForget)
	m.dropForgottenThrottleQueue(queue, throttleForget)
}

type sqsGaugeForget struct {
	tracked          bool
	overflowSetEmpty bool
}

func (m *SQSMetrics) forgetDepthQueueLocked(queue string) sqsGaugeForget {
	_, tracked := m.trackedDepthQueues[queue]
	if tracked {
		delete(m.trackedDepthQueues, queue)
	}
	// Ref-count the overflow set. The shared _other gauge series is
	// safe to drop only when no queue is still reporting into it;
	// while one or more overflow queues remain, the gauge carries
	// real (last-write-wins) data for those queues.
	_, overflow := m.overflowDepthQueues[queue]
	if overflow {
		delete(m.overflowDepthQueues, queue)
	}
	return sqsGaugeForget{tracked: tracked, overflowSetEmpty: overflow && len(m.overflowDepthQueues) == 0}
}

func (m *SQSMetrics) forgetThrottleQueueLocked(queue string) sqsGaugeForget {
	_, throttleTracked := m.trackedThrottleGaugeQueues[queue]
	if throttleTracked {
		delete(m.trackedThrottleGaugeQueues, queue)
	}
	_, throttleOverflow := m.overflowThrottleGaugeQueues[queue]
	if throttleOverflow {
		delete(m.overflowThrottleGaugeQueues, queue)
	}
	return sqsGaugeForget{tracked: throttleTracked, overflowSetEmpty: throttleOverflow && len(m.overflowThrottleGaugeQueues) == 0}
}

func (m *SQSMetrics) dropForgottenDepthQueue(queue string, forget sqsGaugeForget) {
	if forget.tracked {
		m.dropGaugeStatesFor(queue)
	}
	if forget.overflowSetEmpty {
		// Last overflow queue forgotten — drop the shared _other
		// series so dashboards stop showing phantom backlog for
		// queues that no longer exist. Safe because no remaining
		// queue is mapped to this label; any future overflow
		// queue will re-create the series via ObserveQueueDepth.
		m.dropGaugeStatesFor(sqsQueueOverflow)
	}
}

func (m *SQSMetrics) dropForgottenThrottleQueue(queue string, forget sqsGaugeForget) {
	if forget.tracked {
		m.dropThrottleGaugeActionsFor(queue)
	}
	if forget.overflowSetEmpty {
		m.dropThrottleGaugeActionsFor(sqsQueueOverflow)
	}
}

// admitForCounterBudget returns the canonical label for queue: the
// real name when the counter budget has room, sqsQueueOverflow once
// it is saturated. The counter budget never shrinks (counter series
// are cumulative — Prometheus has no semantically-clean delete), so
// this is a one-way admission: once a queue gets in, it stays.
// Mirrors DynamoDBMetrics.tableLabel.
func (m *SQSMetrics) admitForCounterBudget(queue string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.trackedCounterQueues[queue]; ok {
		return queue
	}
	if len(m.trackedCounterQueues) >= sqsMaxTrackedQueues {
		return sqsQueueOverflow
	}
	m.trackedCounterQueues[queue] = struct{}{}
	return queue
}

// admitForDepthBudget mirrors admitForCounterBudget for the gauge
// budget. Unlike the counter side, slots here can be freed via
// ForgetQueue, so a deployment that creates and deletes queues over
// time can reuse budget for new queue names instead of permanently
// exhausting it.
//
// Queues that hit the cap are recorded in overflowDepthQueues so
// ForgetQueue can ref-count them and tear down the shared _other
// gauge once the last one disappears (otherwise the dashboard
// pins phantom backlog for queues that no longer exist).
//
// Promotion path: when budget pressure eases (a slot opened up via
// ForgetQueue) a previously-overflowed queue gets admitted under
// its real name on the next observation. The stale overflow entry
// must be cleaned up at the same moment — otherwise ForgetQueue's
// ref-count is permanently off and the _other gauge can persist
// after every live queue has been promoted off it. If the
// promotion drains the overflow set entirely, drop the three
// _other state series too: they're carrying this queue's last
// overflow value but no queue maps to them anymore.
func (m *SQSMetrics) admitForDepthBudget(queue string) string {
	m.mu.Lock()
	if _, ok := m.trackedDepthQueues[queue]; ok {
		m.mu.Unlock()
		return queue
	}
	if len(m.trackedDepthQueues) >= sqsMaxTrackedQueues {
		m.overflowDepthQueues[queue] = struct{}{}
		m.mu.Unlock()
		return sqsQueueOverflow
	}
	m.trackedDepthQueues[queue] = struct{}{}
	_, wasOverflow := m.overflowDepthQueues[queue]
	if wasOverflow {
		delete(m.overflowDepthQueues, queue)
	}
	overflowSetEmpty := wasOverflow && len(m.overflowDepthQueues) == 0
	m.mu.Unlock()
	if overflowSetEmpty {
		m.dropGaugeStatesFor(sqsQueueOverflow)
	}
	return queue
}

// admitForThrottleGaugeBudget mirrors admitForDepthBudget for the
// event-driven throttle-token gauge. It is separate from queue depth
// because a queue can emit throttle decisions without being present in
// the next depth scrape yet.
func (m *SQSMetrics) admitForThrottleGaugeBudget(queue string) string {
	m.mu.Lock()
	if _, ok := m.trackedThrottleGaugeQueues[queue]; ok {
		m.mu.Unlock()
		return queue
	}
	if len(m.trackedThrottleGaugeQueues) >= sqsMaxTrackedQueues {
		m.overflowThrottleGaugeQueues[queue] = struct{}{}
		m.mu.Unlock()
		return sqsQueueOverflow
	}
	m.trackedThrottleGaugeQueues[queue] = struct{}{}
	_, wasOverflow := m.overflowThrottleGaugeQueues[queue]
	if wasOverflow {
		delete(m.overflowThrottleGaugeQueues, queue)
	}
	overflowSetEmpty := wasOverflow && len(m.overflowThrottleGaugeQueues) == 0
	m.mu.Unlock()
	if overflowSetEmpty {
		m.dropThrottleGaugeActionsFor(sqsQueueOverflow)
	}
	return queue
}

func (m *SQSMetrics) snapshotThrottleGaugeQueues() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	queues := make([]string, 0, len(m.trackedThrottleGaugeQueues)+len(m.overflowThrottleGaugeQueues))
	for queue := range m.trackedThrottleGaugeQueues {
		queues = append(queues, queue)
	}
	for queue := range m.overflowThrottleGaugeQueues {
		queues = append(queues, queue)
	}
	return queues
}

// dropGaugeStatesFor removes the three state-labelled series for
// label (a real queue name or sqsQueueOverflow) from the depth
// gauge. Single-line caller-readability wrapper — prometheus
// GaugeVec.DeleteLabelValues already does its own per-vector
// locking, so this is safe to call without holding m.mu.
func (m *SQSMetrics) dropGaugeStatesFor(label string) {
	m.queueDepth.DeleteLabelValues(label, sqsQueueStateVisible)
	m.queueDepth.DeleteLabelValues(label, sqsQueueStateNotVisible)
	m.queueDepth.DeleteLabelValues(label, sqsQueueStateDelayed)
}

func (m *SQSMetrics) dropThrottleGaugeActionsFor(label string) {
	m.throttleTokens.DeleteLabelValues(label, SQSThrottleActionSend)
	m.throttleTokens.DeleteLabelValues(label, SQSThrottleActionReceive)
	m.throttleTokens.DeleteLabelValues(label, SQSThrottleActionDefault)
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

func sqsValidThrottleAction(action string) bool {
	switch action {
	case SQSThrottleActionSend, SQSThrottleActionReceive, SQSThrottleActionDefault:
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
	snaps, ok := source.SnapshotQueueDepths(ctx)
	if !ok {
		// Source signalled "skip this tick" (transient catalog-scan
		// failure on the leader, ctx cancel mid-scan). Leave
		// existing gauges + lastSeen untouched so the dashboard
		// keeps the last successful snapshot rather than rendering
		// a false "all queues drained" event for the duration of
		// the failure. The next successful tick will diff against
		// the same lastSeen we leave behind here.
		return
	}
	current := make(map[string]struct{}, len(snaps))
	for _, snap := range snaps {
		current[snap.Queue] = struct{}{}
	}
	// Phase 1: forget queues that disappeared since the last tick.
	// This MUST run before phase 2 — emitting first would let
	// admitForDepthBudget see stale names still occupying
	// trackedDepthQueues, so any newly-active queue admitted while
	// the budget was full would silently collapse onto _other for
	// at least one interval even when capacity opens up the same
	// tick. Reclaiming first lets phase 2's admissions reuse the
	// freed slots immediately.
	o.mu.Lock()
	o.forgetMissingQueuesLocked(current)
	o.lastSeen = current
	o.mu.Unlock()
	// Phase 2: emit gauges for the current tick. Slots freed in
	// phase 1 are now visible to admitForDepthBudget (both phases
	// take the SQSMetrics.mu serially), so a brand-new queue
	// landing in the same tick that an old queue disappeared gets
	// the freed slot's real label rather than overflow.
	for _, snap := range snaps {
		o.metrics.ObserveQueueDepth(snap.Queue, snap.Visible, snap.NotVisible, snap.Delayed)
	}
}

func (o *SQSObserver) forgetMissingQueuesLocked(current map[string]struct{}) {
	for prev := range o.lastSeen {
		if _, ok := current[prev]; !ok {
			o.metrics.ForgetQueue(prev)
		}
	}
	for _, prev := range o.metrics.snapshotThrottleGaugeQueues() {
		if _, ok := current[prev]; !ok {
			o.metrics.ForgetQueue(prev)
		}
	}
}
