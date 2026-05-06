package monitoring

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

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

	mfs, err := r.Gatherer().Gather()
	require.NoError(t, err)
	var found bool
	for _, mf := range mfs {
		if !strings.HasPrefix(mf.GetName(), "elastickv_sqs_partition_messages_total") {
			continue
		}
		found = true
		require.Len(t, mf.GetMetric(), 1)
	}
	require.True(t, found,
		"elastickv_sqs_partition_messages_total must be registered on the public Registry")
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
// removes all three state-labelled series so a deleted queue stops
// reporting a frozen backlog. The (queue, partition, action) counter
// is intentionally untouched (cumulative-by-design).
func TestSQSMetrics_ForgetQueue_DropsThreeSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveQueueDepth("orders.fifo", 3, 0, 0)
	m.ObservePartitionMessage("orders.fifo", 0, SQSPartitionActionSend)

	m.ForgetQueue("orders.fifo")

	count, err := testutil.GatherAndCount(reg, "elastickv_sqs_queue_messages")
	require.NoError(t, err)
	require.Equal(t, 0, count, "ForgetQueue must drop every state series for the queue")
	require.InDelta(t, 1.0, testutil.ToFloat64(m.partitionMessages.WithLabelValues("orders.fifo", "0", SQSPartitionActionSend)), 0.001,
		"counter must survive ForgetQueue (cumulative metric)")
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

// fakeDepthSource lets the SQSObserver tests script the per-tick
// snapshot without standing up a real SQS adapter + coordinator.
type fakeDepthSource struct {
	snapshots [][]SQSQueueDepth
	calls     int
}

func (f *fakeDepthSource) SnapshotQueueDepths(_ context.Context) []SQSQueueDepth {
	if f.calls >= len(f.snapshots) {
		f.calls++
		return nil
	}
	out := f.snapshots[f.calls]
	f.calls++
	return out
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
		snapshots: [][]SQSQueueDepth{
			// tick 1: two queues
			{
				{Queue: "orders.fifo", Visible: 5, NotVisible: 2, Delayed: 0},
				{Queue: "audio.fifo", Visible: 0, NotVisible: 0, Delayed: 3},
			},
			// tick 2: orders.fifo disappeared
			{
				{Queue: "audio.fifo", Visible: 1, NotVisible: 0, Delayed: 0},
			},
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
		snapshots: [][]SQSQueueDepth{
			{
				{Queue: "orders.fifo", Visible: 5},
				{Queue: "audio.fifo", Visible: 1},
			},
			nil, // leader stepped down
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
