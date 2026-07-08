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
	ForgetThrottleAction(queue string, action string)
	SyncThrottleActions(queue string, enabledActions []string)
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
//   - ok=true with a nil slice — "this source is not emitting this tick".
//     The adapter returns this when the node is a follower (leader-only
//     emission). The observer clears both depth and token gauges so a former
//     leader does not keep exporting stale SQS series after step-down.
//
//   - ok=true with a non-nil slice, possibly empty — leader, scrape OK.
//     The observer writes the returned queue gauges and diffs against the
//     previous tick. A queue omitted from a non-nil snapshot can be a
//     per-queue scan miss, so throttle token gauges for depth-seen queues are
//     preserved until a config/delete path explicitly removes them.
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
// cardinality-bounded by sqsMaxTrackedQueues, and split by metric
// family so unrelated SQS signals cannot consume each other's
// queue-label budget.
//
// Counter and gauge families have different deletion semantics:
//
//   - partitionMessages is a CounterVec. Counters are cumulative;
//     deleting a series throws away its observed-since-process-start
//     value, so we never call DeleteLabelValues on it. The counter
//     budget therefore only ever grows — once a queue is admitted to
//     trackedCounterQueues it stays admitted, and ForgetQueue does
//     NOT touch this map.
//   - throttledRequests is also cumulative, but uses its own
//     trackedThrottleCounterQueues admission map. A burst of
//     rejected non-partitioned queues must not force later HT-FIFO
//     partition counters into _other, and partition traffic must not
//     hide throttle rejects.
//   - queueDepth is a GaugeVec. Gauges have no cumulative state, so
//     DeleteLabelValues is safe (and necessary, otherwise a deleted
//     queue keeps reporting a frozen backlog on the dashboard).
//     ForgetQueue both removes the gauge series and frees the
//     queue's slot in trackedDepthQueues so a churn-heavy deployment
//     can reuse the budget.
//   - throttleTokens is also a GaugeVec, but decisions are event-
//     driven and action-labelled. It has a separate queue/action
//     lifetime from the depth scraper: throttle config changes or
//     throttle-only cleanup drop token gauges, but transient depth
//     scan misses must not delete them.
//
// Sharing one map across counters and gauges regresses the counter cap:
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

	mu                           sync.Mutex
	trackedCounterQueues         map[string]struct{}
	trackedThrottleCounterQueues map[string]struct{}
	trackedDepthQueues           map[string]struct{}
	trackedThrottleGaugeQueues   map[string]map[string]struct{}
	throttleGaugeQueueSeq        map[string]uint64
	throttleGaugeActionSeq       map[string]map[string]uint64
	throttleGaugeSeq             uint64
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
	overflowThrottleGaugeQueues map[string]map[string]struct{}
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
		trackedCounterQueues:         map[string]struct{}{},
		trackedThrottleCounterQueues: map[string]struct{}{},
		trackedDepthQueues:           map[string]struct{}{},
		trackedThrottleGaugeQueues:   map[string]map[string]struct{}{},
		throttleGaugeQueueSeq:        map[string]uint64{},
		throttleGaugeActionSeq:       map[string]map[string]uint64{},
		overflowDepthQueues:          map[string]struct{}{},
		overflowThrottleGaugeQueues:  map[string]map[string]struct{}{},
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
		queueCounterLabel := m.admitForThrottleCounterBudget(queue)
		m.throttledRequests.WithLabelValues(queueCounterLabel, action).Inc()
	}
	m.mu.Lock()
	queueGaugeLabel := m.admitForThrottleGaugeBudgetLocked(queue, action)
	m.throttleTokens.WithLabelValues(queueGaugeLabel, action).Set(tokensRemaining)
	m.mu.Unlock()
}

// ObserveThrottleRejection increments only the rejected-request counter. The
// adapter uses it when a stale pre-invalidation request still returns
// Throttling to the client but must not recreate a token-balance gauge.
func (m *SQSMetrics) ObserveThrottleRejection(queue string, action string) {
	if m == nil || queue == "" {
		return
	}
	if !sqsValidThrottleAction(action) {
		return
	}
	queueCounterLabel := m.admitForThrottleCounterBudget(queue)
	m.throttledRequests.WithLabelValues(queueCounterLabel, action).Inc()
}

// ForgetThrottleAction removes the token-balance gauge for one queue/action
// whose throttle bucket is no longer configured. Throttled-request counters
// are cumulative and intentionally survive.
func (m *SQSMetrics) ForgetThrottleAction(queue string, action string) {
	if m == nil || queue == "" {
		return
	}
	if !sqsValidThrottleAction(action) {
		return
	}
	m.mu.Lock()
	forget := m.forgetThrottleActionLocked(queue, action)
	if forget.tracked {
		m.dropThrottleGaugeActionFor(queue, action)
	}
	if forget.overflowSetEmpty {
		m.dropThrottleGaugeActionFor(sqsQueueOverflow, action)
	}
	m.mu.Unlock()
}

// ForgetThrottleActionBefore removes a token-balance gauge only when that
// specific queue/action was not refreshed after cutoff. It lets config-change
// cleanup reset stale gauges without erasing a fresh post-change request that
// raced between bucket invalidation and metrics reconciliation.
func (m *SQSMetrics) ForgetThrottleActionBefore(queue string, action string, cutoff uint64) {
	if m == nil || queue == "" {
		return
	}
	if !sqsValidThrottleAction(action) {
		return
	}
	m.mu.Lock()
	if actionSeqs := m.throttleGaugeActionSeq[queue]; actionSeqs != nil {
		if seq := actionSeqs[action]; seq > cutoff {
			m.mu.Unlock()
			return
		}
	}
	forget := m.forgetThrottleActionLocked(queue, action)
	if forget.tracked {
		m.dropThrottleGaugeActionFor(queue, action)
	}
	if forget.overflowSetEmpty {
		m.dropThrottleGaugeActionFor(sqsQueueOverflow, action)
	}
	m.mu.Unlock()
}

// SyncThrottleActions reconciles token-balance gauges after a queue's throttle
// configuration changes, even when no later request touches the disabled
// bucket. enabledActions is the configured metric-action set; omitted actions
// have their gauges removed. Counters intentionally survive.
func (m *SQSMetrics) SyncThrottleActions(queue string, enabledActions []string) {
	if m == nil || queue == "" {
		return
	}
	enabled := make(map[string]struct{}, len(enabledActions))
	for _, action := range enabledActions {
		if sqsValidThrottleAction(action) {
			enabled[action] = struct{}{}
		}
	}
	m.mu.Lock()
	for _, action := range []string{SQSThrottleActionSend, SQSThrottleActionReceive, SQSThrottleActionDefault} {
		if _, ok := enabled[action]; ok {
			continue
		}
		forget := m.forgetThrottleActionLocked(queue, action)
		if forget.tracked {
			m.dropThrottleGaugeActionFor(queue, action)
		}
		if forget.overflowSetEmpty {
			m.dropThrottleGaugeActionFor(sqsQueueOverflow, action)
		}
	}
	m.mu.Unlock()
}

// ForgetQueue drops the three depth gauge series for a queue and frees its slot
// in the depth-side cardinality budget so a long-running deployment that
// regularly creates and deletes queues (CI workloads, ephemeral per-job queues)
// doesn't permanently wedge the 512-entry depth budget.
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
// SQSObserver.observeOnce calls this (registry plumbing aside), and it is invoked
// exactly when a queue was observed in the previous tick but not the current one.
// The caller's depth contract is preserved, including overflow ref-counting, but
// throttle token gauges are intentionally out of scope: a per-queue depth scan
// miss is not evidence that throttle config changed.
func (m *SQSMetrics) ForgetQueue(queue string) {
	if m == nil || queue == "" {
		return
	}
	m.mu.Lock()
	depthForget := m.forgetDepthQueueLocked(queue)
	m.mu.Unlock()
	m.dropForgottenDepthQueue(queue, depthForget)
}

// ForgetThrottleQueue drops every token-balance gauge for queue and frees its
// throttle-gauge budget slot. It is intentionally separate from ForgetQueue
// because queue-depth snapshots can omit a live queue on a transient per-queue
// scan error.
func (m *SQSMetrics) ForgetThrottleQueue(queue string) {
	if m == nil || queue == "" {
		return
	}
	m.mu.Lock()
	throttleForget := m.forgetThrottleQueueLocked(queue)
	m.dropForgottenThrottleQueue(queue, throttleForget)
	m.mu.Unlock()
}

type sqsGaugeForget struct {
	tracked          bool
	overflowSetEmpty bool
}

type sqsThrottleGaugeForget struct {
	tracked         bool
	overflowActions []string
}

type sqsThrottleActionForget struct {
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

func (m *SQSMetrics) forgetThrottleQueueLocked(queue string) sqsThrottleGaugeForget {
	_, throttleTracked := m.trackedThrottleGaugeQueues[queue]
	if throttleTracked {
		delete(m.trackedThrottleGaugeQueues, queue)
	}
	delete(m.throttleGaugeQueueSeq, queue)
	delete(m.throttleGaugeActionSeq, queue)
	return sqsThrottleGaugeForget{
		tracked:         throttleTracked,
		overflowActions: m.removeThrottleOverflowQueueLocked(queue),
	}
}

func (m *SQSMetrics) forgetThrottleActionLocked(queue string, action string) sqsThrottleActionForget {
	var forget sqsThrottleActionForget
	if actions, ok := m.trackedThrottleGaugeQueues[queue]; ok {
		if _, hasAction := actions[action]; hasAction {
			delete(actions, action)
			forget.tracked = true
		}
		if len(actions) == 0 {
			delete(m.trackedThrottleGaugeQueues, queue)
		}
	}
	if queues, ok := m.overflowThrottleGaugeQueues[action]; ok {
		if _, hasQueue := queues[queue]; hasQueue {
			delete(queues, queue)
			if len(queues) == 0 {
				delete(m.overflowThrottleGaugeQueues, action)
				forget.overflowSetEmpty = true
			}
		}
	}
	if !m.throttleGaugeQueueTrackedLocked(queue) {
		delete(m.throttleGaugeQueueSeq, queue)
	}
	if !m.throttleGaugeActionTrackedLocked(queue, action) {
		m.deleteThrottleGaugeActionSeqLocked(queue, action)
	}
	return forget
}

func (m *SQSMetrics) removeThrottleOverflowQueueLocked(queue string) []string {
	var emptyActions []string
	for action, queues := range m.overflowThrottleGaugeQueues {
		if _, ok := queues[queue]; !ok {
			continue
		}
		delete(queues, queue)
		if len(queues) == 0 {
			delete(m.overflowThrottleGaugeQueues, action)
			emptyActions = append(emptyActions, action)
		}
	}
	if !m.throttleGaugeQueueTrackedLocked(queue) {
		delete(m.throttleGaugeQueueSeq, queue)
	}
	delete(m.throttleGaugeActionSeq, queue)
	return emptyActions
}

func (m *SQSMetrics) removeThrottleOverflowActionLocked(queue string, action string) bool {
	queues, ok := m.overflowThrottleGaugeQueues[action]
	if !ok {
		return false
	}
	if _, ok := queues[queue]; !ok {
		return false
	}
	delete(queues, queue)
	empty := len(queues) == 0
	if empty {
		delete(m.overflowThrottleGaugeQueues, action)
	}
	if !m.throttleGaugeActionTrackedLocked(queue, action) {
		m.deleteThrottleGaugeActionSeqLocked(queue, action)
	}
	if !m.throttleGaugeQueueTrackedLocked(queue) {
		delete(m.throttleGaugeQueueSeq, queue)
	}
	return empty
}

func (m *SQSMetrics) throttleGaugeQueueTrackedLocked(queue string) bool {
	if _, ok := m.trackedThrottleGaugeQueues[queue]; ok {
		return true
	}
	for _, queues := range m.overflowThrottleGaugeQueues {
		if _, ok := queues[queue]; ok {
			return true
		}
	}
	return false
}

func (m *SQSMetrics) throttleGaugeActionTrackedLocked(queue string, action string) bool {
	if actions, ok := m.trackedThrottleGaugeQueues[queue]; ok {
		if _, ok := actions[action]; ok {
			return true
		}
	}
	if queues, ok := m.overflowThrottleGaugeQueues[action]; ok {
		if _, ok := queues[queue]; ok {
			return true
		}
	}
	return false
}

func (m *SQSMetrics) deleteThrottleGaugeActionSeqLocked(queue string, action string) {
	actionSeqs := m.throttleGaugeActionSeq[queue]
	if actionSeqs == nil {
		return
	}
	delete(actionSeqs, action)
	if len(actionSeqs) == 0 {
		delete(m.throttleGaugeActionSeq, queue)
	}
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

func (m *SQSMetrics) dropForgottenThrottleQueue(queue string, forget sqsThrottleGaugeForget) {
	if forget.tracked {
		m.dropThrottleGaugeActionsFor(queue)
	}
	for _, action := range forget.overflowActions {
		m.dropThrottleGaugeActionFor(sqsQueueOverflow, action)
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
	return admitCounterQueueLocked(queue, m.trackedCounterQueues)
}

func (m *SQSMetrics) admitForThrottleCounterBudget(queue string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return admitCounterQueueLocked(queue, m.trackedThrottleCounterQueues)
}

func admitCounterQueueLocked(queue string, tracked map[string]struct{}) string {
	if _, ok := tracked[queue]; ok {
		return queue
	}
	if len(tracked) >= sqsMaxTrackedQueues {
		return sqsQueueOverflow
	}
	tracked[queue] = struct{}{}
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

// admitForThrottleGaugeBudgetLocked mirrors admitForDepthBudget for the
// event-driven throttle-token gauge. It is separate from queue depth because a
// queue can emit throttle decisions without being present in the next depth
// scrape yet. m.mu must be held so _other deletions cannot race a concurrent
// overflow Set for the same action.
func (m *SQSMetrics) admitForThrottleGaugeBudgetLocked(queue string, action string) string {
	m.throttleGaugeSeq++
	m.throttleGaugeQueueSeq[queue] = m.throttleGaugeSeq
	if m.throttleGaugeActionSeq[queue] == nil {
		m.throttleGaugeActionSeq[queue] = map[string]uint64{}
	}
	m.throttleGaugeActionSeq[queue][action] = m.throttleGaugeSeq
	if actions, ok := m.trackedThrottleGaugeQueues[queue]; ok {
		actions[action] = struct{}{}
		if m.removeThrottleOverflowActionLocked(queue, action) {
			m.dropThrottleGaugeActionFor(sqsQueueOverflow, action)
		}
		return queue
	}
	if len(m.trackedThrottleGaugeQueues) >= sqsMaxTrackedQueues {
		queues := m.overflowThrottleGaugeQueues[action]
		if queues == nil {
			queues = map[string]struct{}{}
			m.overflowThrottleGaugeQueues[action] = queues
		}
		queues[queue] = struct{}{}
		return sqsQueueOverflow
	}
	m.trackedThrottleGaugeQueues[queue] = map[string]struct{}{action: {}}
	if m.removeThrottleOverflowActionLocked(queue, action) {
		m.dropThrottleGaugeActionFor(sqsQueueOverflow, action)
	}
	return queue
}

// ThrottleGaugeSnapshotCutoff returns the current token-gauge observation
// sequence. Config-change cleanup can use it to forget only gauges observed
// before a reset point while preserving later request observations.
func (m *SQSMetrics) ThrottleGaugeSnapshotCutoff() uint64 {
	return m.throttleGaugeSnapshotCutoff()
}

func (m *SQSMetrics) throttleGaugeSnapshotCutoff() uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.throttleGaugeSeq
}

func (m *SQSMetrics) snapshotThrottleGaugeQueues(cutoff uint64) []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]struct{}, len(m.trackedThrottleGaugeQueues))
	queues := make([]string, 0, len(m.trackedThrottleGaugeQueues)+len(m.overflowThrottleGaugeQueues))
	for queue := range m.trackedThrottleGaugeQueues {
		if m.throttleGaugeQueueSeq[queue] > cutoff {
			continue
		}
		seen[queue] = struct{}{}
		queues = append(queues, queue)
	}
	for _, overflowQueues := range m.overflowThrottleGaugeQueues {
		for queue := range overflowQueues {
			if m.throttleGaugeQueueSeq[queue] > cutoff {
				continue
			}
			if _, ok := seen[queue]; ok {
				continue
			}
			seen[queue] = struct{}{}
			queues = append(queues, queue)
		}
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
	for _, action := range []string{SQSThrottleActionSend, SQSThrottleActionReceive, SQSThrottleActionDefault} {
		m.dropThrottleGaugeActionFor(label, action)
	}
}

func (m *SQSMetrics) dropThrottleGaugeActionFor(label string, action string) {
	m.throttleTokens.DeleteLabelValues(label, action)
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

	mu                      sync.Mutex
	lastSeen                map[string]struct{}
	depthSeenThrottleQueues map[string]struct{}
}

func newSQSObserver(metrics *SQSMetrics) *SQSObserver {
	return &SQSObserver{
		metrics:                 metrics,
		lastSeen:                map[string]struct{}{},
		depthSeenThrottleQueues: map[string]struct{}{},
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
	throttleCutoff := o.metrics.throttleGaugeSnapshotCutoff()
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
	sourceInactive := snaps == nil
	o.mu.Lock()
	o.forgetMissingQueuesLocked(current, throttleCutoff, sourceInactive)
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

func (o *SQSObserver) forgetMissingQueuesLocked(current map[string]struct{}, throttleCutoff uint64, sourceInactive bool) {
	if sourceInactive {
		throttleQueues, _ := o.snapshotThrottleQueues(^uint64(0))
		o.forgetDepthQueuesMissingFrom(current, false)
		o.forgetInactiveSourceThrottleQueues(current, throttleQueues)
		clear(o.depthSeenThrottleQueues)
		return
	}
	throttleQueues, _ := o.snapshotThrottleQueues(throttleCutoff)
	_, currentThrottleQueues := o.snapshotThrottleQueues(^uint64(0))
	o.forgetDepthSeenThrottleQueuesWithoutGauge(currentThrottleQueues)
	o.forgetDepthQueuesMissingFrom(current, true)
	o.forgetThrottleOnlyQueues(current, throttleQueues)
}

func (o *SQSObserver) snapshotThrottleQueues(throttleCutoff uint64) ([]string, map[string]struct{}) {
	throttleQueues := o.metrics.snapshotThrottleGaugeQueues(throttleCutoff)
	activeThrottleQueues := make(map[string]struct{}, len(throttleQueues))
	for _, queue := range throttleQueues {
		activeThrottleQueues[queue] = struct{}{}
	}
	return throttleQueues, activeThrottleQueues
}

func (o *SQSObserver) forgetDepthSeenThrottleQueuesWithoutGauge(activeThrottleQueues map[string]struct{}) {
	for queue := range o.depthSeenThrottleQueues {
		if _, ok := activeThrottleQueues[queue]; !ok {
			delete(o.depthSeenThrottleQueues, queue)
		}
	}
}

func (o *SQSObserver) forgetDepthQueuesMissingFrom(current map[string]struct{}, preserveThrottle bool) {
	for prev := range o.lastSeen {
		if _, ok := current[prev]; !ok {
			if preserveThrottle {
				o.depthSeenThrottleQueues[prev] = struct{}{}
			}
			o.metrics.ForgetQueue(prev)
		}
	}
}

func (o *SQSObserver) forgetInactiveSourceThrottleQueues(current map[string]struct{}, throttleQueues []string) {
	for _, prev := range throttleQueues {
		if _, ok := current[prev]; ok {
			continue
		}
		o.metrics.ForgetThrottleQueue(prev)
	}
}

func (o *SQSObserver) forgetThrottleOnlyQueues(current map[string]struct{}, throttleQueues []string) {
	for _, prev := range throttleQueues {
		if _, ok := current[prev]; !ok {
			if _, wasDepthSeen := o.lastSeen[prev]; wasDepthSeen {
				continue
			}
			if _, wasDepthSeen := o.depthSeenThrottleQueues[prev]; wasDepthSeen {
				continue
			}
			o.metrics.ForgetThrottleQueue(prev)
		}
	}
}
