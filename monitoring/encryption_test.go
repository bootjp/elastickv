package monitoring

import (
	"strings"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestEncryptionMetricsCountDecryptFailuresByReason(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)

	metrics.ObserveEncryptionDecryptFailure(encryption.DecryptFailureReasonTagMismatch)
	metrics.ObserveEncryptionDecryptFailure(encryption.DecryptFailureReasonTagMismatch)
	metrics.ObserveEncryptionDecryptFailure(encryption.DecryptFailureReasonUnknownKeyID)
	metrics.ObserveEncryptionDecryptFailure(encryption.DecryptFailureReasonTruncated)
	metrics.ObserveEncryptionDecryptFailure(encryption.DecryptFailureReasonBadVersion)

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(`
# HELP elastickv_encryption_decrypt_failures_total Total encrypted-value reads that failed to decode or authenticate, by failure reason. Any non-zero value is a paging-grade signal.
# TYPE elastickv_encryption_decrypt_failures_total counter
elastickv_encryption_decrypt_failures_total{reason="bad_version"} 1
elastickv_encryption_decrypt_failures_total{reason="tag_mismatch"} 2
elastickv_encryption_decrypt_failures_total{reason="truncated"} 1
elastickv_encryption_decrypt_failures_total{reason="unknown_key_id"} 1
`),
		"elastickv_encryption_decrypt_failures_total",
	))
}

// TestEncryptionMetricsFoldUnrecognisedReasonIntoUnknown is the label
// cardinality guard. The reason label is derived from data that a disk
// attacker can influence, so an unexpected string must collapse into
// the closed set rather than mint a new series per distinct value.
func TestEncryptionMetricsFoldUnrecognisedReasonIntoUnknown(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)

	metrics.ObserveEncryptionDecryptFailure("wat")
	metrics.ObserveEncryptionDecryptFailure("")
	metrics.ObserveEncryptionDecryptFailure("another-unexpected-value")

	require.Equal(t, 1, testutil.CollectAndCount(metrics.decryptFailures),
		"unrecognised reasons must collapse into a single series")
	require.InDelta(t, 3.0,
		testutil.ToFloat64(metrics.decryptFailures.WithLabelValues(encryption.DecryptFailureReasonUnknown)),
		0.0001)
}

func TestEncryptionMetricsCountWritesPerDEKAndValueOverhead(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)

	// Two writes under DEK 4, one under DEK 9.
	metrics.ObserveEncryptionWrite(4, 100, 132)
	metrics.ObserveEncryptionWrite(4, 200, 232)
	metrics.ObserveEncryptionWrite(9, 50, 82)

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(`
# HELP elastickv_encryption_writes_per_dek Total encrypted values written under each DEK, by key_id. Drives the writes-based rotation trigger.
# TYPE elastickv_encryption_writes_per_dek counter
elastickv_encryption_writes_per_dek{key_id="4"} 2
elastickv_encryption_writes_per_dek{key_id="9"} 1
`),
		"elastickv_encryption_writes_per_dek",
	))
	require.Equal(t, 1, testutil.CollectAndCount(metrics.valueOverhead))
}

// TestEncryptionMetricsRecordNegativeOverheadWhenCompressionShrinksValue
// pins the §6.4 interaction: a compressible value stores fewer bytes
// than its plaintext. Clamping at zero would erase exactly the signal
// the histogram exists to expose.
func TestEncryptionMetricsRecordNegativeOverheadWhenCompressionShrinksValue(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)

	metrics.ObserveEncryptionWrite(1, 10_000, 400)

	families, err := reg.Gather()
	require.NoError(t, err)

	var sum float64
	var found bool
	for _, family := range families {
		if family.GetName() != "elastickv_encryption_value_overhead_bytes" {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		sum = family.GetMetric()[0].GetHistogram().GetSampleSum()
		found = true
	}
	require.True(t, found, "value overhead histogram must be registered")
	require.InDelta(t, -9600.0, sum, 0.0001,
		"a compressed value stores fewer bytes than its plaintext; the negative overhead must survive")
}

// TestEncryptionMetricsWriteCounterCacheIsRaceFree exercises the
// memoized per-key_id counter from many goroutines: the storage write
// path is concurrent, so a torn read of the cache map would be a live
// data race under -race.
func TestEncryptionMetricsWriteCounterCacheIsRaceFree(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)

	const goroutines = uint32(16)
	const perGoroutine = 64
	const distinctKeys = uint32(4)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				metrics.ObserveEncryptionWrite(g%distinctKeys, 10, 42)
			}
		}()
	}
	wg.Wait()

	total := 0.0
	for keyID := range distinctKeys {
		total += testutil.ToFloat64(metrics.writeCounter(keyID))
	}
	require.InDelta(t, float64(goroutines)*float64(perGoroutine), total, 0.0001)
	require.Equal(t, int(distinctKeys), testutil.CollectAndCount(metrics.writesPerDEK))
}

// TestEncryptionMetricsNilReceiverIsInert covers the disabled-metrics
// node: the store holds a nil observer and must not panic.
func TestEncryptionMetricsNilReceiverIsInert(t *testing.T) {
	t.Parallel()

	var metrics *EncryptionMetrics
	require.NotPanics(t, func() {
		metrics.ObserveEncryptionDecryptFailure(encryption.DecryptFailureReasonTagMismatch)
		metrics.ObserveEncryptionWrite(1, 10, 42)
	})
}

func TestRegistryExposesEncryptionObserver(t *testing.T) {
	t.Parallel()

	require.NotNil(t, NewRegistry("n1", "127.0.0.1:1").EncryptionObserver())

	var nilRegistry *Registry
	require.Nil(t, nilRegistry.EncryptionObserver())
}
