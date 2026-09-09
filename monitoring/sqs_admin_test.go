package monitoring

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSQSAdminCountersRecordOutcomes(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveAdminPurgeQueue("orders", SQSAdminOutcomeOK)
	m.ObserveAdminPurgeQueue("orders", SQSAdminOutcomePurgeInProgress)
	m.ObserveAdminPeekQueue("orders-dlq", SQSAdminOutcomeOK)
	m.ObserveAdminPeekQueue("orders-dlq", SQSAdminOutcomeThrottled)

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(`
# HELP elastickv_sqs_admin_purge_queue_total Total admin PurgeQueue calls by queue and outcome.
# TYPE elastickv_sqs_admin_purge_queue_total counter
elastickv_sqs_admin_purge_queue_total{outcome="ok",queue="orders"} 1
elastickv_sqs_admin_purge_queue_total{outcome="purge_in_progress",queue="orders"} 1
# HELP elastickv_sqs_admin_peek_queue_total Total admin PeekQueue calls by queue and outcome.
# TYPE elastickv_sqs_admin_peek_queue_total counter
elastickv_sqs_admin_peek_queue_total{outcome="ok",queue="orders-dlq"} 1
elastickv_sqs_admin_peek_queue_total{outcome="throttled",queue="orders-dlq"} 1
`),
		"elastickv_sqs_admin_purge_queue_total",
		"elastickv_sqs_admin_peek_queue_total",
	))
}

// TestSQSAdminCountersBoundTheOutcomeLabel is the cardinality guard.
// The label must never be derived from an error string; an
// unrecognised value collapses into internal_error rather than
// minting a series.
func TestSQSAdminCountersBoundTheOutcomeLabel(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	for _, bogus := range []string{"", "connection reset by peer", "i/o timeout"} {
		m.ObserveAdminPurgeQueue("orders", bogus)
		m.ObserveAdminPeekQueue("orders", bogus)
	}

	require.Equal(t, 1, testutil.CollectAndCount(m.adminPurgeQueue))
	require.Equal(t, 1, testutil.CollectAndCount(m.adminPeekQueue))
	require.InDelta(t, 3.0, testutil.ToFloat64(
		m.adminPurgeQueue.WithLabelValues("orders", SQSAdminOutcomeInternalError)), 0.0001)
}

// TestSQSAdminOutcomeSetsAreAsymmetric pins the deliberate difference
// between the two closed sets: purge reports contention as
// purge_in_progress and peek reports it as throttled. Accepting both
// on either counter would let the two paths drift into describing the
// same condition two ways.
func TestSQSAdminOutcomeSetsAreAsymmetric(t *testing.T) {
	t.Parallel()

	require.Equal(t, SQSAdminOutcomeInternalError,
		normalizeSQSAdminPurgeOutcome(SQSAdminOutcomeThrottled),
		"purge has no throttled outcome")
	require.Equal(t, SQSAdminOutcomeInternalError,
		normalizeSQSAdminPeekOutcome(SQSAdminOutcomePurgeInProgress),
		"peek has no purge_in_progress outcome")

	require.Equal(t, SQSAdminOutcomePurgeInProgress,
		normalizeSQSAdminPurgeOutcome(SQSAdminOutcomePurgeInProgress))
	require.Equal(t, SQSAdminOutcomeThrottled,
		normalizeSQSAdminPeekOutcome(SQSAdminOutcomeThrottled))
}

// TestSQSAdminCountersBoundTheQueueLabel pins that operator-supplied
// queue names cannot grow the series set without limit: past the
// shared budget they collapse into the overflow label.
func TestSQSAdminCountersBoundTheQueueLabel(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	for i := range sqsMaxTrackedQueues + 50 {
		m.ObserveAdminPurgeQueue(queueNameForIndex(i), SQSAdminOutcomeOK)
	}

	require.LessOrEqual(t, testutil.CollectAndCount(m.adminPurgeQueue), sqsMaxTrackedQueues+1,
		"queue names past the budget must collapse into the overflow label")
	require.Positive(t, testutil.ToFloat64(
		m.adminPurgeQueue.WithLabelValues(sqsQueueOverflow, SQSAdminOutcomeOK)))
}

// TestSQSAdminCountersAttributeAnEmptyQueueToOverflow covers the
// validation rejection, where there is no queue to attribute to.
func TestSQSAdminCountersAttributeAnEmptyQueueToOverflow(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := newSQSMetrics(reg)

	m.ObserveAdminPurgeQueue("", SQSAdminOutcomeValidation)
	require.InDelta(t, 1.0, testutil.ToFloat64(
		m.adminPurgeQueue.WithLabelValues(sqsQueueOverflow, SQSAdminOutcomeValidation)), 0.0001)
}

func TestSQSAdminCountersNilReceiverIsInert(t *testing.T) {
	t.Parallel()

	var m *SQSMetrics
	require.NotPanics(t, func() {
		m.ObserveAdminPurgeQueue("q", SQSAdminOutcomeOK)
		m.ObserveAdminPeekQueue("q", SQSAdminOutcomeOK)
	})
}

func queueNameForIndex(i int) string {
	return "queue-" + strings.Repeat("x", i%3) + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
