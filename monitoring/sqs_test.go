package monitoring

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func gatheredThrottleTokenValue(t *testing.T, reg *prometheus.Registry, labels map[string]string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "elastickv_sqs_throttle_tokens_remaining" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if !metricLabelsMatch(metric.GetLabel(), labels) {
				continue
			}
			switch {
			case metric.GetGauge() != nil:
				return metric.GetGauge().GetValue(), true
			case metric.GetCounter() != nil:
				return metric.GetCounter().GetValue(), true
			default:
				return 0, true
			}
		}
	}
	return 0, false
}

func metricLabelsMatch(labels []*dto.LabelPair, want map[string]string) bool {
	if len(labels) != len(want) {
		return false
	}
	for _, label := range labels {
		got, ok := want[label.GetName()]
		if !ok || got != label.GetValue() {
			return false
		}
	}
	return true
}

// TestSQSMetrics_ObservePartitionMessage_IncrementsByLabelTriple
// pins the basic counter contract: each (queue, partition,
// action) tuple gets its own series and only the matching
// observation increments it.
func TestSQSMetrics_ObservePartitionMessage_IncrementsByLabelTriple(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObservePartitionMessage("q.fifo", 0, SQSPartitionActionSend)
	m.ObservePartitionMessage("q.fifo", 0, SQSPartitionActionSend)
	m.ObservePartitionMessage("q.fifo", 1, SQSPartitionActionSend)
	m.ObservePartitionMessage("q.fifo", 0, SQSPartitionActionReceive)
	m.ObservePartitionMessage("q.fifo", 0, SQSPartitionActionDelete)
	m.ObservePartitionMessage("other.fifo", 7, SQSPartitionActionSend)

	require.InDelta(t, 2.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("q.fifo", "0", SQSPartitionActionSend)), 0.001)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("q.fifo", "1", SQSPartitionActionSend)), 0.001)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("q.fifo", "0", SQSPartitionActionReceive)), 0.001)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("q.fifo", "0", SQSPartitionActionDelete)), 0.001)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("other.fifo", "7", SQSPartitionActionSend)), 0.001)
}

// TestSQSMetrics_ObservePartitionMessage_DropsInvalidAction pins
// the typo guard: an action label that isn't one of the three
// stable values (send / receive / delete) is silently dropped so
// a future call-site bug cannot pollute the metric with new
// label values that dashboards / alerts have to learn about.
func TestSQSMetrics_ObservePartitionMessage_DropsInvalidAction(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	for _, bad := range []string{"", "Send", "SEND", "publish", "create"} {
		m.ObservePartitionMessage("q.fifo", 0, bad)
	}
	// Valid action increments after the dropped attempts to prove
	// the metric is still healthy and only the bogus actions were
	// rejected.
	m.ObservePartitionMessage("q.fifo", 0, SQSPartitionActionSend)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("q.fifo", "0", SQSPartitionActionSend)), 0.001)
}

// TestSQSMetrics_ObservePartitionMessage_DropsEmptyQueue pins
// that an empty queue name is dropped — collapsing all empty-
// name observations onto a single series would mask a call-site
// bug. Mirrors the typo guard above.
func TestSQSMetrics_ObservePartitionMessage_DropsEmptyQueue(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObservePartitionMessage("", 0, SQSPartitionActionSend)

	// A subsequent valid observation lands on its own series —
	// proves the empty-name observe was dropped, not collapsed
	// into the valid one.
	m.ObservePartitionMessage("q.fifo", 0, SQSPartitionActionSend)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("q.fifo", "0", SQSPartitionActionSend)), 0.001)
}

// TestSQSMetrics_NilReceiverIsSafe pins the nil-receiver
// short-circuit: adapter call sites pass the observer through
// without checking, and the SQSPartitionObserver interface lets
// a nil concrete value land here. The observe call must be a
// no-op rather than a nil-pointer panic.
func TestSQSMetrics_NilReceiverIsSafe(t *testing.T) {
	t.Parallel()
	var m *SQSMetrics
	require.NotPanics(t, func() {
		m.ObservePartitionMessage("q.fifo", 0, SQSPartitionActionSend)
		m.ObserveThrottleDecision("q.fifo", SQSThrottleActionSend, 1, true)
	})
}

// TestSQSMetrics_QueueLabelOverflow pins the cardinality cap:
// after sqsMaxTrackedQueues distinct queue names, every new
// name collapses to the _other label so a misbehaving caller
// cannot exhaust Prometheus's series budget. The first
// sqsMaxTrackedQueues names retain their real labels.
func TestSQSMetrics_QueueLabelOverflow(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ObservePartitionMessage("q-"+strconv.Itoa(i)+".fifo", 0, SQSPartitionActionSend)
	}
	// All observations within the cap retain their real names.
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("q-0.fifo", "0", SQSPartitionActionSend)), 0.001)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("q-"+strconv.Itoa(sqsMaxTrackedQueues-1)+".fifo", "0", SQSPartitionActionSend)), 0.001)

	// One more name overflows to the placeholder label.
	m.ObservePartitionMessage("overflow.fifo", 0, SQSPartitionActionSend)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues(sqsQueueOverflow, "0", SQSPartitionActionSend)), 0.001)

	// Hitting the same overflow name again accumulates on the
	// _other series, not on a fresh real-name series — the cap
	// is a one-way collapse.
	m.ObservePartitionMessage("overflow.fifo", 0, SQSPartitionActionSend)
	require.InDelta(t, 2.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues(sqsQueueOverflow, "0", SQSPartitionActionSend)), 0.001)
}

// TestSQSMetrics_RegistryWiring pins the integration with the
// public Registry: a freshly-built Registry hands back an
// observer whose increments show up on the underlying gatherer
// under the documented metric name. Catches a regression that
// either skipped the SQS metrics registration or exposed it
// under the wrong name.
func TestSQSMetrics_RegistryWiring(t *testing.T) {
	t.Parallel()
	r := NewRegistry("node-test", "127.0.0.1:50051")
	obs := r.SQSPartitionObserver()
	require.NotNil(t, obs)

	obs.ObservePartitionMessage("q.fifo", 3, SQSPartitionActionReceive)
	throttleObs, ok := obs.(SQSThrottleObserver)
	require.True(t, ok)
	throttleObs.ObserveThrottleDecision("q.fifo", SQSThrottleActionSend, 4, true)

	mfs, err := r.Gatherer().Gather()
	require.NoError(t, err)
	var foundPartition bool
	var foundThrottleCounter bool
	var foundThrottleGauge bool
	for _, mf := range mfs {
		switch {
		case strings.HasPrefix(mf.GetName(), "elastickv_sqs_partition_messages_total"):
			foundPartition = true
			require.Len(t, mf.GetMetric(), 1)
		case strings.HasPrefix(mf.GetName(), "elastickv_sqs_throttled_requests_total"):
			foundThrottleCounter = true
			require.Len(t, mf.GetMetric(), 1)
		case strings.HasPrefix(mf.GetName(), "elastickv_sqs_throttle_tokens_remaining"):
			foundThrottleGauge = true
			require.Len(t, mf.GetMetric(), 1)
		default:
			continue
		}
	}
	require.True(t, foundPartition,
		"elastickv_sqs_partition_messages_total must be registered on the public Registry")
	require.True(t, foundThrottleCounter,
		"elastickv_sqs_throttled_requests_total must be registered on the public Registry")
	require.True(t, foundThrottleGauge,
		"elastickv_sqs_throttle_tokens_remaining must be registered on the public Registry")
}

func TestSQSMetrics_ObserveThrottleDecision_EmitsCounterAndGauge(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveThrottleDecision("orders.fifo", SQSThrottleActionSend, 9, false)
	require.InDelta(t, 9.0, testutil.ToFloat64(m.throttleTokens.WithLabelValues("orders.fifo", SQSThrottleActionSend)), 0.001)
	require.InDelta(t, 0.0, testutil.ToFloat64(m.throttledRequests.WithLabelValues("orders.fifo", SQSThrottleActionSend)), 0.001)

	m.ObserveThrottleDecision("orders.fifo", SQSThrottleActionSend, 0, true)
	require.InDelta(t, 0.0, testutil.ToFloat64(m.throttleTokens.WithLabelValues("orders.fifo", SQSThrottleActionSend)), 0.001)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.throttledRequests.WithLabelValues("orders.fifo", SQSThrottleActionSend)), 0.001)
}

func TestSQSMetrics_ObserveThrottleDecision_AllowedDoesNotConsumeCounterBudget(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ObserveThrottleDecision("allow-"+strconv.Itoa(i)+".fifo", SQSThrottleActionSend, 1, false)
	}
	require.Empty(t, m.trackedCounterQueues, "allowed-only throttle decisions must not consume counter label budget")

	m.ObserveThrottleDecision("blocked.fifo", SQSThrottleActionSend, 0, true)
	require.Empty(t, m.trackedCounterQueues, "throttle rejects must not consume the partition counter label budget")
	require.Len(t, m.trackedThrottleCounterQueues, 1, "throttle rejects consume the throttle counter label budget")
	require.InDelta(t, 1.0,
		testutil.ToFloat64(m.throttledRequests.WithLabelValues("blocked.fifo", SQSThrottleActionSend)),
		0.001,
		"first actual reject must keep its real queue label instead of overflowing after allowed-only decisions")
}

func TestSQSMetrics_ThrottleCounterBudgetIsIndependent(t *testing.T) {
	t.Parallel()

	t.Run("throttle rejects do not hide later partition counters", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		m := newSQSMetrics(reg)

		for i := 0; i < sqsMaxTrackedQueues; i++ {
			m.ObserveThrottleDecision("throttle-"+strconv.Itoa(i)+".fifo", SQSThrottleActionSend, 0, true)
		}
		require.Len(t, m.trackedThrottleCounterQueues, sqsMaxTrackedQueues)
		require.Empty(t, m.trackedCounterQueues)

		m.ObservePartitionMessage("partition.fifo", 0, SQSPartitionActionSend)
		require.InDelta(t, 1.0,
			testutil.ToFloat64(m.partitionMessages.WithLabelValues("partition.fifo", "0", SQSPartitionActionSend)),
			0.001,
			"partition counter must still get a real queue label after throttle counter budget fills")
	})

	t.Run("partition counters do not hide later throttle rejects", func(t *testing.T) {
		t.Parallel()
		reg := prometheus.NewRegistry()
		m := newSQSMetrics(reg)

		for i := 0; i < sqsMaxTrackedQueues; i++ {
			m.ObservePartitionMessage("partition-"+strconv.Itoa(i)+".fifo", 0, SQSPartitionActionSend)
		}
		require.Len(t, m.trackedCounterQueues, sqsMaxTrackedQueues)
		require.Empty(t, m.trackedThrottleCounterQueues)

		m.ObserveThrottleDecision("blocked.fifo", SQSThrottleActionSend, 0, true)
		require.InDelta(t, 1.0,
			testutil.ToFloat64(m.throttledRequests.WithLabelValues("blocked.fifo", SQSThrottleActionSend)),
			0.001,
			"throttle counter must still get a real queue label after partition counter budget fills")
	})
}

func TestSQSMetrics_ObserveThrottleDecision_DropsInvalidLabels(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveThrottleDecision("", SQSThrottleActionSend, 1, true)
	m.ObserveThrottleDecision("orders.fifo", "Send", 1, true)

	counterCount, err := testutil.GatherAndCount(reg, "elastickv_sqs_throttled_requests_total")
	require.NoError(t, err)
	require.Equal(t, 0, counterCount)
	gaugeCount, err := testutil.GatherAndCount(reg, "elastickv_sqs_throttle_tokens_remaining")
	require.NoError(t, err)
	require.Equal(t, 0, gaugeCount)
}

func TestSQSMetrics_ForgetThrottleAction_DropsGaugeAndFreesSlot(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveThrottleDecision("orders.fifo", SQSThrottleActionSend, 5, false)
	m.ObserveThrottleDecision("orders.fifo", SQSThrottleActionReceive, 7, false)
	require.Len(t, m.trackedThrottleGaugeQueues, 1)

	m.ForgetThrottleAction("orders.fifo", SQSThrottleActionSend)
	_, found := gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  "orders.fifo",
		"action": SQSThrottleActionSend,
	})
	require.False(t, found, "disabled send bucket must drop the stale token gauge")
	remaining, found := gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  "orders.fifo",
		"action": SQSThrottleActionReceive,
	})
	require.True(t, found, "other configured actions for the same queue must survive")
	require.InDelta(t, 7.0, remaining, 0.001)
	require.Len(t, m.trackedThrottleGaugeQueues, 1, "queue stays admitted while another action remains")

	m.ForgetThrottleAction("orders.fifo", SQSThrottleActionReceive)
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_throttle_tokens_remaining")
	require.NoError(t, err)
	require.Equal(t, 0, count)
	require.Empty(t, m.trackedThrottleGaugeQueues, "last disabled action frees the throttle gauge queue slot")
}

func TestSQSMetrics_SyncThrottleActions_DropsDisabledWithoutTraffic(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveThrottleDecision("orders.fifo", SQSThrottleActionSend, 5, false)
	m.ObserveThrottleDecision("orders.fifo", SQSThrottleActionReceive, 7, false)

	m.SyncThrottleActions("orders.fifo", []string{SQSThrottleActionSend})
	send, found := gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  "orders.fifo",
		"action": SQSThrottleActionSend,
	})
	require.True(t, found, "configured send bucket must keep its token gauge")
	require.InDelta(t, 5.0, send, 0.001)
	_, found = gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  "orders.fifo",
		"action": SQSThrottleActionReceive,
	})
	require.False(t, found, "disabled receive bucket must be removed without waiting for more traffic")
	require.Len(t, m.trackedThrottleGaugeQueues, 1, "queue stays admitted while send remains configured")

	m.SyncThrottleActions("orders.fifo", nil)
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_throttle_tokens_remaining")
	require.NoError(t, err)
	require.Equal(t, 0, count)
	require.Empty(t, m.trackedThrottleGaugeQueues, "disabling the last action frees the gauge slot")
}

func TestSQSMetrics_ForgetQueue_OverflowThrottleGaugeClearsPerAction(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ObserveThrottleDecision("real-"+strconv.Itoa(i)+".fifo", SQSThrottleActionSend, 1, false)
	}
	require.Len(t, m.trackedThrottleGaugeQueues, sqsMaxTrackedQueues)

	m.ObserveThrottleDecision("overflow-a.fifo", SQSThrottleActionSend, 10, false)
	m.ObserveThrottleDecision("overflow-b.fifo", SQSThrottleActionReceive, 20, false)

	send, found := gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  sqsQueueOverflow,
		"action": SQSThrottleActionSend,
	})
	require.True(t, found)
	require.InDelta(t, 10.0, send, 0.001)
	receive, found := gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  sqsQueueOverflow,
		"action": SQSThrottleActionReceive,
	})
	require.True(t, found)
	require.InDelta(t, 20.0, receive, 0.001)

	m.ForgetQueue("overflow-a.fifo")
	_, found = gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  sqsQueueOverflow,
		"action": SQSThrottleActionSend,
	})
	require.False(t, found, "last overflow send queue disappearing must drop only _other/send")
	receive, found = gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  sqsQueueOverflow,
		"action": SQSThrottleActionReceive,
	})
	require.True(t, found, "_other/receive must survive while another overflow receive queue remains")
	require.InDelta(t, 20.0, receive, 0.001)

	m.ForgetQueue("overflow-b.fifo")
	_, found = gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  sqsQueueOverflow,
		"action": SQSThrottleActionReceive,
	})
	require.False(t, found, "last overflow receive queue disappearing must drop _other/receive")
}

// TestSQSMetrics_ObserveQueueDepth_EmitsThreeStates pins that one
// ObserveQueueDepth call produces three series (visible / not_visible
// / delayed) under elastickv_sqs_queue_messages keyed on queue name.
func TestSQSMetrics_ObserveQueueDepth_EmitsThreeStates(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveQueueDepth("orders.fifo", 5, 2, 1)

	require.InDelta(t, 5.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", sqsQueueStateVisible)), 0.001)
	require.InDelta(t, 2.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", sqsQueueStateNotVisible)), 0.001)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", sqsQueueStateDelayed)), 0.001)
}

// TestSQSMetrics_ObserveQueueDepth_ClampsNegativeToZero pins the
// defensive clamp: a future caller that passes a -1 sentinel from a
// failed scan must not blast a fake backlog onto dashboards.
func TestSQSMetrics_ObserveQueueDepth_ClampsNegativeToZero(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveQueueDepth("orders.fifo", -7, -1, -100)

	for _, state := range []string{sqsQueueStateVisible, sqsQueueStateNotVisible, sqsQueueStateDelayed} {
		require.InDelta(t, 0.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", state)), 0.001,
			"state %s must be clamped to 0 on negative input", state)
	}
}

// TestSQSMetrics_ObserveQueueDepth_DropsEmptyQueue pins the
// defensive empty-name drop: same rule as the partition counter,
// preventing all callers from collapsing onto the empty-label
// series under a future bug.
func TestSQSMetrics_ObserveQueueDepth_DropsEmptyQueue(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveQueueDepth("", 1, 1, 1)

	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, 0, count, "empty queue name must not emit any series")
}

// TestSQSMetrics_ForgetQueue_DropsThreeSeries pins that ForgetQueue
// (a) removes all three state-labelled series so a deleted queue
// stops reporting a frozen backlog, (b) frees the cardinality-
// budget slot so a churn-heavy deployment doesn't permanently
// exhaust the 512-entry budget, and (c) leaves the
// (queue, partition, action) counter alone (cumulative-by-design).
func TestSQSMetrics_ForgetQueue_DropsThreeSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveQueueDepth("orders.fifo", 3, 0, 0)
	m.ObserveThrottleDecision("orders.fifo", SQSThrottleActionSend, 4, true)
	m.ObservePartitionMessage("orders.fifo", 0, SQSPartitionActionSend)
	require.Len(t, m.trackedDepthQueues, 1, "queue must have been admitted to the depth budget on ObserveQueueDepth")
	require.Len(t, m.trackedThrottleGaugeQueues, 1, "queue must have been admitted to the throttle gauge budget on ObserveThrottleDecision")
	require.Len(t, m.trackedCounterQueues, 1, "queue must have been admitted to the counter budget on ObservePartitionMessage")

	m.ForgetQueue("orders.fifo")

	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, 0, count, "ForgetQueue must drop every state series for the queue")
	throttleGaugeCount, err := testutil.GatherAndCount(reg, "elastickv_sqs_throttle_tokens_remaining")
	require.NoError(t, err)
	require.Equal(t, 0, throttleGaugeCount, "ForgetQueue must drop every throttle-token gauge for the queue")
	require.Empty(t, m.trackedDepthQueues, "ForgetQueue must free the depth-side cardinality slot")
	require.Empty(t, m.trackedThrottleGaugeQueues, "ForgetQueue must free the throttle-gauge cardinality slot")
	require.Len(t, m.trackedCounterQueues, 1, "ForgetQueue must NOT free the counter-side slot (counters are cumulative)")
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("orders.fifo", "0", SQSPartitionActionSend)), 0.001,
		"counter must survive ForgetQueue (cumulative metric)")
	require.InDelta(t, 1.0, testutil.ToFloat64(m.throttledRequests.WithLabelValues("orders.fifo", SQSThrottleActionSend)), 0.001,
		"throttled-request counter must survive ForgetQueue (cumulative metric)")

	// Observe again post-forget: the new series must be keyed on
	// the real name (slot was freed), NOT on the _other overflow
	// label. Without the trackedQueues cleanup this would silently
	// collapse to _other once the budget eventually filled.
	m.ObserveQueueDepth("orders.fifo", 7, 0, 0)
	require.InDelta(t, 7.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", sqsQueueStateVisible)), 0.001,
		"post-forget Observe must re-emit under the real queue name")
}

// TestSQSMetrics_ForgetQueue_OverflowQueueIsNoOp pins the
// overflow-collision safety: a queue that hit the cardinality cap
// and got collapsed onto the _other label has no individual series
// to delete. ForgetQueue must NOT call DeleteLabelValues with the
// _other label because that series is shared with every other
// overflow queue — tearing it down would zero-out unrelated data.
func TestSQSMetrics_ForgetQueue_OverflowQueueIsNoOp(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	// Saturate the budget with sqsMaxTrackedQueues real queues so
	// any further observation collapses to _other.
	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ObserveQueueDepth("real-"+strconv.Itoa(i)+".fifo", 1, 0, 0)
	}
	require.Len(t, m.trackedDepthQueues, sqsMaxTrackedQueues)

	// Two overflow queues — both share the _other label.
	m.ObserveQueueDepth("overflow-a.fifo", 100, 0, 0)
	m.ObserveQueueDepth("overflow-b.fifo", 200, 0, 0)
	overflowVisible := testutil.ToFloat64(m.queueDepth.WithLabelValues(sqsQueueOverflow, sqsQueueStateVisible))
	require.InDelta(t, 200.0, overflowVisible, 0.001,
		"second observe should overwrite the shared _other gauge")

	// ForgetQueue on an overflow queue must leave the _other series
	// (and the budget) untouched.
	m.ForgetQueue("overflow-a.fifo")
	require.Len(t, m.trackedDepthQueues, sqsMaxTrackedQueues,
		"ForgetQueue on an overflow queue must not change the depth budget")
	require.InDelta(t, 200.0,
		testutil.ToFloat64(m.queueDepth.WithLabelValues(sqsQueueOverflow, sqsQueueStateVisible)),
		0.001,
		"_other series must still carry the latest value (overflow-b)")
}

// TestSQSMetrics_AdmitForDepthBudget_PromotionClearsOverflow pins
// the second P2 found in PR #743 review: admitForDepthBudget must
// remove a queue from overflowDepthQueues when promoting it from
// the shared _other label to a real-name label, AND drop the
// _other gauge series if the overflow set becomes empty as a
// result.
//
// Pre-fix bug: when budget pressure eased (a slot opened up via
// ForgetQueue) and a previously-overflowed queue was re-observed,
// admitForDepthBudget added it to trackedDepthQueues but left a
// stale entry in overflowDepthQueues. Two consequences:
//
//  1. ForgetQueue's overflow ref-count was permanently off by one
//     per stale entry — the _other gauge could persist even when
//     no live queue mapped to it.
//  2. The _other series itself still carried the queue's last
//     value as overflow, even though that queue was now reporting
//     under its real name. Dashboards see double-counted backlog.
//
// Post-fix expectation: a successful promotion both removes the
// queue from overflowDepthQueues and (if that drained the set)
// drops the three _other state series, mirroring ForgetQueue's
// "last overflow gone" branch.
func TestSQSMetrics_AdmitForDepthBudget_PromotionClearsOverflow(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	// Saturate the depth budget.
	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ObserveQueueDepth("real-"+strconv.Itoa(i)+".fifo", 1, 0, 0)
	}

	// Overflow queue X collapses to _other.
	m.ObserveQueueDepth("overflow-x.fifo", 100, 0, 0)
	require.InDelta(t, 100.0,
		testutil.ToFloat64(m.queueDepth.WithLabelValues(sqsQueueOverflow, sqsQueueStateVisible)),
		0.001, "sanity: _other carries X's value while X is mapped to overflow")

	// Free a slot so X can be promoted on the next observation.
	m.ForgetQueue("real-0.fifo")

	// Re-observe X. With room in the budget admitForDepthBudget
	// must promote it to its real label AND clean up the stale
	// overflowDepthQueues entry.
	m.ObserveQueueDepth("overflow-x.fifo", 200, 0, 0)
	require.InDelta(t, 200.0,
		testutil.ToFloat64(m.queueDepth.WithLabelValues("overflow-x.fifo", sqsQueueStateVisible)),
		0.001, "X must now report under its real label after promotion")

	// 3 state series per queue:
	//   511 remaining tracked queues (real-1..real-511) + X promoted = 512 × 3 = 1536.
	// Pre-fix the _other series persists with X's stale 100 value
	// (1539 total). Post-fix the promotion drains the overflow set
	// and drops _other.
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, sqsMaxTrackedQueues*3, count,
		"_other series must be dropped when promotion drains the overflow set")
}

// TestSQSMetrics_ForgetQueue_LastOverflowClearsOtherGauge pins the
// P2 found in PR #743 review: when the depth budget is saturated
// and queues collapse onto the shared sqsQueueOverflow ("_other")
// label, ForgetQueue must drop the _other gauge series once the
// LAST overflow queue disappears. Otherwise a churn-heavy
// deployment with >512 queues leaves the dashboard pinned at
// whichever overflow queue last reported, even after every
// overflow queue has been deleted — phantom backlog for queues
// that no longer exist.
//
// Pre-fix bug: ForgetQueue early-returned for any queue not in
// trackedDepthQueues, which lumps together "never observed" (no
// series to delete) with "currently collapsed onto _other"
// (series exists, shared, must drop only when ref-count hits 0).
//
// Post-fix expectation: track overflow queue membership
// separately from trackedDepthQueues so ForgetQueue can
// ref-count overflow queues and clear the shared _other gauge
// the moment the set becomes empty.
//
// The single-overflow-not-empty case (don't tear down _other
// while other overflow queues still report into it) is pinned
// by TestSQSMetrics_ForgetQueue_OverflowQueueIsNoOp below.
func TestSQSMetrics_ForgetQueue_LastOverflowClearsOtherGauge(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	// Saturate the depth budget with sqsMaxTrackedQueues real queues
	// so subsequent observations collapse onto _other.
	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ObserveQueueDepth("real-"+strconv.Itoa(i)+".fifo", 1, 0, 0)
	}

	// Two overflow queues — both share the _other label.
	m.ObserveQueueDepth("overflow-a.fifo", 100, 0, 0)
	m.ObserveQueueDepth("overflow-b.fifo", 200, 0, 0)
	require.InDelta(t, 200.0,
		testutil.ToFloat64(m.queueDepth.WithLabelValues(sqsQueueOverflow, sqsQueueStateVisible)),
		0.001,
		"sanity: _other gauge carries the latest overflow value")

	// Forget the first overflow queue. _other must remain — the
	// other overflow queue is still reporting into it.
	m.ForgetQueue("overflow-a.fifo")
	require.InDelta(t, 200.0,
		testutil.ToFloat64(m.queueDepth.WithLabelValues(sqsQueueOverflow, sqsQueueStateVisible)),
		0.001,
		"_other must survive ForgetQueue while at least one overflow queue is still alive")

	// Forget the last overflow queue. _other must now be gone:
	// no overflow queue exists, so the gauge would only be showing
	// phantom data for queues that no longer exist.
	m.ForgetQueue("overflow-b.fifo")
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	// 3 state-labelled series per queue. Only the original tracked
	// queues should remain — _other (visible/not_visible/delayed)
	// must be dropped now that the overflow set is empty.
	require.Equal(t, sqsMaxTrackedQueues*3, count,
		"only the 3 state series per real queue should remain after the last overflow queue is forgotten")
}

// TestSQSMetrics_ForgetQueue_DoesNotReclaimCounterBudget pins the
// P1 found in PR #743 review: ForgetQueue must NOT free a counter-
// side cardinality budget slot. The (queue, partition, action)
// counter series is cumulative and cannot be deleted from
// Prometheus without losing its observed value, so once a queue
// has been admitted to the counter budget the slot must stay
// permanently consumed — even if the queue is dropped from the
// gauge side.
//
// Pre-fix bug: trackedQueues was a single shared map. ForgetQueue
// would delete the entry, freeing the slot. A subsequent
// ObservePartitionMessage on a NEW queue then got admitted with a
// real label, even though the old queue's counter series was still
// alive in Prometheus. Repeating this (e.g. leader step-down clears
// lastSeen → ForgetQueue called for every queue → 512 fresh queues
// arrive and get admitted) lets counter cardinality grow unbounded.
//
// Post-fix expectation: the counter budget is one-way. Once
// saturated, every later queue collapses to _other regardless of
// how many ForgetQueue calls have run.
func TestSQSMetrics_ForgetQueue_DoesNotReclaimCounterBudget(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	// Saturate the counter budget with sqsMaxTrackedQueues distinct
	// queue names — every one is admitted as a real label.
	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ObservePartitionMessage("orig-"+strconv.Itoa(i)+".fifo", 0, SQSPartitionActionSend)
	}

	// Drop every queue via ForgetQueue. This used to free the
	// budget slot for every queue, including ones that had only
	// counter activity (depth side never touched them — pre-fix
	// the check was non-distinguishing).
	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ForgetQueue("orig-" + strconv.Itoa(i) + ".fifo")
	}

	// New queue arrives. Pre-fix this would be admitted as a real
	// label because the slot looked free. Post-fix the counter
	// budget is still saturated (counters were never deleted), so
	// the new queue must collapse to _other.
	m.ObservePartitionMessage("new-after-forget.fifo", 0, SQSPartitionActionSend)

	require.InDelta(t, 0.0,
		testutil.ToFloat64(m.partitionMessages.WithLabelValues("new-after-forget.fifo", "0", SQSPartitionActionSend)),
		0.001,
		"counter for the new queue must not exist under its real name — budget should be exhausted")
	require.InDelta(t, 1.0,
		testutil.ToFloat64(m.partitionMessages.WithLabelValues(sqsQueueOverflow, "0", SQSPartitionActionSend)),
		0.001,
		"new queue past the counter cap must collapse to _other")
}

// TestSQSMetrics_ForgetQueue_StillReclaimsDepthBudget pins the
// converse: the gauge side IS reclaimable, because gauges can be
// deleted from Prometheus without losing semantically-meaningful
// state. A churn-heavy deployment that observes depth for 512
// distinct queues, then forgets all of them, must be able to
// observe a 513th distinct queue under its real name.
func TestSQSMetrics_ForgetQueue_StillReclaimsDepthBudget(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ObserveQueueDepth("orig-"+strconv.Itoa(i)+".fifo", 1, 0, 0)
	}
	for i := 0; i < sqsMaxTrackedQueues; i++ {
		m.ForgetQueue("orig-" + strconv.Itoa(i) + ".fifo")
	}
	m.ObserveQueueDepth("fresh.fifo", 42, 0, 0)

	require.InDelta(t, 42.0,
		testutil.ToFloat64(m.queueDepth.WithLabelValues("fresh.fifo", sqsQueueStateVisible)),
		0.001,
		"gauge budget must reclaim slots so a churn-heavy deployment can keep emitting real labels")
}

// TestSQSMetrics_DepthNilReceiverIsSafe mirrors the partition test:
// a typed-nil *SQSMetrics handed to the adapter must not panic on
// either ObserveQueueDepth or ForgetQueue calls.
func TestSQSMetrics_DepthNilReceiverIsSafe(t *testing.T) {
	t.Parallel()
	var m *SQSMetrics
	require.NotPanics(t, func() {
		m.ObserveQueueDepth("q.fifo", 1, 2, 3)
		m.ForgetQueue("q.fifo")
	})
}

// TestSQSMetrics_DepthRegistryWiring pins that the depth gauge
// surfaces under the documented metric name on the public Registry
// after a snapshot is observed via the registry's SQSMetrics. The
// SQSObserver path is exercised separately in
// TestSQSObserver_ObserveOnce_EmitsAndForgets below.
func TestSQSMetrics_DepthRegistryWiring(t *testing.T) {
	t.Parallel()
	r := NewRegistry("node-test", "127.0.0.1:50051")
	require.NotNil(t, r.SQSObserver(), "registry must expose a non-nil SQSObserver")
	require.NotNil(t, r.sqs, "registry must wire the SQSMetrics")

	r.sqs.ObserveQueueDepth("q.fifo", 4, 1, 0)

	mfs, err := r.Gatherer().Gather()
	require.NoError(t, err)
	var found bool
	for _, mf := range mfs {
		if !strings.HasPrefix(mf.GetName(), "elastickv_sqs_queue_messages") {
			continue
		}
		found = true
		require.Len(t, mf.GetMetric(), 3,
			"three state labels (visible / not_visible / delayed) for one queue")
	}
	require.True(t, found,
		"elastickv_sqs_queue_messages must be registered on the public Registry")
}

// fakeDepthTick is one scripted return from fakeDepthSource. ok
// mirrors the SQSDepthSource contract: false models a transient
// scan failure that should NOT cause the observer to wipe gauges.
type fakeDepthTick struct {
	snaps []SQSQueueDepth
	ok    bool
}

// fakeDepthSource lets the SQSObserver tests script the per-tick
// snapshot without standing up a real SQS adapter + coordinator.
// Past the scripted ticks, returns the empty-OK sentinel so trailing
// observeOnce calls drain to a clean state rather than blowing up
// with an out-of-range index.
type fakeDepthSource struct {
	ticks []fakeDepthTick
	calls int
}

func (f *fakeDepthSource) SnapshotQueueDepths(_ context.Context) ([]SQSQueueDepth, bool) {
	if f.calls >= len(f.ticks) {
		f.calls++
		return nil, true
	}
	t := f.ticks[f.calls]
	f.calls++
	return t.snaps, t.ok
}

type callbackDepthSource struct {
	fn func() ([]SQSQueueDepth, bool)
}

func (f callbackDepthSource) SnapshotQueueDepths(context.Context) ([]SQSQueueDepth, bool) {
	return f.fn()
}

// TestSQSObserver_ObserveOnce_EmitsAndForgets pins the observer's
// state machine: queues present in the current tick get gauges
// emitted, queues that disappeared since the last tick get
// ForgetQueue'd. Without that diff the dashboard would show a
// frozen backlog after DeleteQueue.
func TestSQSObserver_ObserveOnce_EmitsAndForgets(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)
	obs := newSQSObserver(m)

	source := &fakeDepthSource{
		ticks: []fakeDepthTick{
			// tick 1: two queues, scrape OK.
			{snaps: []SQSQueueDepth{
				{Queue: "orders.fifo", Visible: 5, NotVisible: 2, Delayed: 0},
				{Queue: "audio.fifo", Visible: 0, NotVisible: 0, Delayed: 3},
			}, ok: true},
			// tick 2: orders.fifo disappeared from the snapshot,
			// scrape OK.
			{snaps: []SQSQueueDepth{
				{Queue: "audio.fifo", Visible: 1, NotVisible: 0, Delayed: 0},
			}, ok: true},
		},
	}

	obs.ObserveOnce(source)
	require.InDelta(t, 5.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", sqsQueueStateVisible)), 0.001)
	require.InDelta(t, 3.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("audio.fifo", sqsQueueStateDelayed)), 0.001)

	obs.ObserveOnce(source)
	// orders.fifo gauges should be gone.
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, 3, count, "after second tick: only audio.fifo's three state gauges remain")
	require.InDelta(t, 1.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("audio.fifo", sqsQueueStateVisible)), 0.001)
}

// TestSQSObserver_ObserveOnce_LeaderStepDownClearsAll pins the
// leader-step-down behaviour: when the source returns an empty
// slice (the adapter's not-leader branch), every gauge from the
// previous tick must be cleared so a stepped-down leader doesn't
// leave stale numbers on the dashboard until the new leader runs
// its first tick.
func TestSQSObserver_ObserveOnce_LeaderStepDownClearsAll(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)
	obs := newSQSObserver(m)

	source := &fakeDepthSource{
		ticks: []fakeDepthTick{
			{snaps: []SQSQueueDepth{
				{Queue: "orders.fifo", Visible: 5},
				{Queue: "audio.fifo", Visible: 1},
			}, ok: true},
			// Leader stepped down: empty snapshot, but ok=true so
			// the observer ForgetQueue's the gauges from tick 1
			// rather than skipping the diff.
			{snaps: nil, ok: true},
		},
	}
	obs.ObserveOnce(source)
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, 6, count, "tick 1: 2 queues × 3 states = 6 series")

	obs.ObserveOnce(source)
	count, err = testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, 0, count, "tick 2 (leader step-down): all gauges cleared")
}

func TestSQSObserver_ObserveOnce_ForgetsThrottleOnlyQueues(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)
	obs := newSQSObserver(m)

	m.ObserveThrottleDecision("short-lived.fifo", SQSThrottleActionSend, 4, false)
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_throttle_tokens_remaining")
	require.NoError(t, err)
	require.Equal(t, 1, count, "sanity: throttle-only queue emitted a token gauge before the depth tick")

	obs.ObserveOnce(&fakeDepthSource{
		ticks: []fakeDepthTick{{snaps: nil, ok: true}},
	})

	count, err = testutil.GatherAndCount(reg, "elastickv_sqs_throttle_tokens_remaining")
	require.NoError(t, err)
	require.Equal(t, 0, count, "successful depth tick must clear throttle-only queues absent from the snapshot")
	require.Empty(t, m.trackedThrottleGaugeQueues, "throttle gauge budget slot must be reclaimed for the short-lived queue")
}

func TestSQSObserver_ObserveOnce_PreservesThrottleGaugeNewerThanDepthSnapshot(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)
	obs := newSQSObserver(m)

	obs.ObserveOnce(callbackDepthSource{fn: func() ([]SQSQueueDepth, bool) {
		m.ObserveThrottleDecision("newer.fifo", SQSThrottleActionSend, 4, false)
		return nil, true
	}})

	value, found := gatheredThrottleTokenValue(t, reg, map[string]string{
		"queue":  "newer.fifo",
		"action": SQSThrottleActionSend,
	})
	require.True(t, found, "a throttle gauge emitted after the cleanup cutoff must survive until the next depth tick")
	require.InDelta(t, 4.0, value, 0.001)

	obs.ObserveOnce(&fakeDepthSource{
		ticks: []fakeDepthTick{{snaps: nil, ok: true}},
	})
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_throttle_tokens_remaining")
	require.NoError(t, err)
	require.Equal(t, 0, count, "the next successful depth tick can clear the now-old throttle-only gauge")
}

// TestSQSObserver_ObserveOnce_TransientScanErrorPreservesGauges
// pins the P2 fix from PR #743 r6: when the source returns
// ok=false (transient catalog-scan failure on the leader, ctx
// cancel mid-scan), the observer must NOT diff against the
// previous tick or call ForgetQueue. The existing gauges should
// keep their last successful values until a later successful
// scrape — otherwise a single failed catalog read renders a
// false "all queues drained" event on the dashboard for the
// entire duration of the failure.
//
// Pre-fix shape: SnapshotQueueDepths returned bare nil on scan
// failure, observeOnce treated bare nil as "no queues exist now"
// and ForgetQueue'd every previously-seen queue. Post-fix shape:
// the two cases are distinguishable via the ok return; observer
// short-circuits when ok=false and leaves both the gauges and
// the lastSeen map untouched.
func TestSQSObserver_ObserveOnce_TransientScanErrorPreservesGauges(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)
	obs := newSQSObserver(m)

	source := &fakeDepthSource{
		ticks: []fakeDepthTick{
			// tick 1: two queues, scrape OK.
			{snaps: []SQSQueueDepth{
				{Queue: "orders.fifo", Visible: 5},
				{Queue: "audio.fifo", Visible: 1},
			}, ok: true},
			// tick 2: scan failed — observer must skip.
			{snaps: nil, ok: false},
			// tick 3: scan recovered, both queues still present.
			{snaps: []SQSQueueDepth{
				{Queue: "orders.fifo", Visible: 7},
				{Queue: "audio.fifo", Visible: 2},
			}, ok: true},
		},
	}

	obs.ObserveOnce(source)
	require.InDelta(t, 5.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", sqsQueueStateVisible)), 0.001)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("audio.fifo", sqsQueueStateVisible)), 0.001)

	obs.ObserveOnce(source)
	// Pre-fix: orders/audio gauges would be wiped via ForgetQueue.
	// Post-fix: gauges still carry tick-1 values.
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, 6, count, "transient scan failure must preserve existing gauges (2 queues × 3 states = 6)")
	require.InDelta(t, 5.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", sqsQueueStateVisible)), 0.001,
		"orders.fifo gauge must keep its tick-1 value while the scan is failing")
	require.InDelta(t, 1.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("audio.fifo", sqsQueueStateVisible)), 0.001,
		"audio.fifo gauge must keep its tick-1 value while the scan is failing")

	obs.ObserveOnce(source)
	// Tick 3 recovery: both queues still present so neither is
	// forgotten; gauges update to the new values. The lastSeen
	// map kept its tick-1 contents through the failed tick, so
	// no spurious ForgetQueue happens here either.
	require.InDelta(t, 7.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("orders.fifo", sqsQueueStateVisible)), 0.001)
	require.InDelta(t, 2.0, testutil.ToFloat64(m.queueDepth.WithLabelValues("audio.fifo", sqsQueueStateVisible)), 0.001)
}

// TestSQSObserver_ObserveOnce_HighChurnReclaimsBeforeAdmit pins
// the P2 fix from PR #743 r9: observeOnce must run the
// disappeared-queue ForgetQueue diff BEFORE emitting the current
// tick's gauges, so that depth-budget slots freed by a
// just-disappeared queue are available for a brand-new queue
// admitted in the same tick.
//
// Pre-fix shape: emit ran first (admitForDepthBudget saw stale
// names still occupying trackedDepthQueues, so any new queue
// admitted while the budget was full collapsed to _other), then
// forget cleared the stale names. End state: half the new queues
// stuck on _other for at least one interval even though
// capacity opened up the same tick.
//
// Post-fix shape: forget runs first (slots reclaim under m.mu),
// emit runs second (admit sees the freed slots and gives every
// new queue a real label). End state: every queue admitted under
// its real name, no _other in the overflow gauge.
//
// Scenario: tick 1 saturates the budget with sqsMaxTrackedQueues
// real queues; tick 2 keeps half and replaces the other half
// with brand-new names — full churn at the cap.
func TestSQSObserver_ObserveOnce_HighChurnReclaimsBeforeAdmit(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)
	obs := newSQSObserver(m)

	tick1 := make([]SQSQueueDepth, sqsMaxTrackedQueues)
	for i := 0; i < sqsMaxTrackedQueues; i++ {
		tick1[i] = SQSQueueDepth{Queue: "old-" + strconv.Itoa(i) + ".fifo", Visible: 1}
	}

	half := sqsMaxTrackedQueues / 2
	tick2 := make([]SQSQueueDepth, 0, sqsMaxTrackedQueues)
	for i := 0; i < half; i++ {
		// First half of old queues carry over.
		tick2 = append(tick2, SQSQueueDepth{Queue: "old-" + strconv.Itoa(i) + ".fifo", Visible: 2})
	}
	for i := 0; i < half; i++ {
		// Brand-new queues replacing the disappeared half.
		tick2 = append(tick2, SQSQueueDepth{Queue: "new-" + strconv.Itoa(i) + ".fifo", Visible: 3})
	}

	source := &fakeDepthSource{
		ticks: []fakeDepthTick{
			{snaps: tick1, ok: true},
			{snaps: tick2, ok: true},
		},
	}

	obs.ObserveOnce(source)
	obs.ObserveOnce(source)

	// Series count first — must come before any per-queue
	// WithLabelValues call below, because WithLabelValues
	// materialises the labelled gauge as a side effect (creating
	// it with default value 0 if absent). Asserting "_other has
	// no series" via WithLabelValues would actually CREATE the
	// _other series and inflate the count; instead we assert the
	// total series count here and let the math prove the absence
	// (512 real queues × 3 states = 1536 with no room for any
	// _other series; pre-fix would land at 256 real × 3 + _other
	// × 3 = 771).
	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, sqsMaxTrackedQueues*3, count,
		"512 queues × 3 states = 1536 series, all real labels — pre-fix would be 256 real × 3 + _other × 3 = 771")

	// Per-queue spot checks: the brand-new queues admitted under
	// real labels (these series already exist from tick 2's emit
	// phase, so WithLabelValues hits the existing children and
	// doesn't perturb the count).
	require.InDelta(t, 3.0,
		testutil.ToFloat64(m.queueDepth.WithLabelValues("new-0.fifo", sqsQueueStateVisible)),
		0.001,
		"new queue must report under its real label — slot freed by old-(half) forget within same tick")
	require.InDelta(t, 3.0,
		testutil.ToFloat64(m.queueDepth.WithLabelValues("new-"+strconv.Itoa(half-1)+".fifo", sqsQueueStateVisible)),
		0.001,
		"last new queue must also report under its real label")
}

// TestSQSObserver_NilTolerant pins that nil observer / nil source
// don't panic — the same nil-tolerant contract Raft / Redis
// observers carry, so a metrics-disabled deployment can pass nil
// through without defensive code at every call site.
func TestSQSObserver_NilTolerant(t *testing.T) {
	t.Parallel()
	var nilObs *SQSObserver
	require.NotPanics(t, func() {
		nilObs.Start(context.Background(), &fakeDepthSource{}, time.Millisecond)
		nilObs.ObserveOnce(&fakeDepthSource{})
	})
	obs := newSQSObserver(newSQSMetrics(prometheus.NewRegistry()))
	require.NotPanics(t, func() {
		obs.Start(context.Background(), nil, time.Millisecond)
		obs.ObserveOnce(nil)
	})
}
