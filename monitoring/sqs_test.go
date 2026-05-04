package monitoring

import (
	"strconv"
	"strings"
	"testing"

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
