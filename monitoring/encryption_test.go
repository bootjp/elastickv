package monitoring

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
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

// fakeEncryptionState drives the §9.2 sidecar gauges.
type fakeEncryptionState struct {
	storageID uint32
	raftID    uint32
	raftIndex uint64
}

func (f fakeEncryptionState) ActiveStorageKeyID() (uint32, bool) {
	return f.storageID, f.storageID != 0
}

func (f fakeEncryptionState) ActiveRaftKeyID() (uint32, bool) {
	return f.raftID, f.raftID != 0
}

func (f fakeEncryptionState) SidecarRaftAppliedIndex() uint64 { return f.raftIndex }

// TestEncryptionMetricsPublishPreBootstrapPostureAtConstruction pins
// that both purpose series exist from process start. Without it an
// alert on active_dek_id == 0 could not tell "not bootstrapped" from
// "this node never reported", which are very different incidents.
func TestEncryptionMetricsPublishPreBootstrapPostureAtConstruction(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(`
# HELP elastickv_encryption_active_dek_id Currently active DEK id per purpose; 0 means the cluster has not bootstrapped that purpose.
# TYPE elastickv_encryption_active_dek_id gauge
elastickv_encryption_active_dek_id{purpose="raft"} 0
elastickv_encryption_active_dek_id{purpose="storage"} 0
`),
		"elastickv_encryption_active_dek_id",
	))
	require.NotNil(t, metrics)
}

func TestEncryptionMetricsObserveSidecarState(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)

	metrics.observeState(fakeEncryptionState{storageID: 7, raftID: 8, raftIndex: 4211})

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(`
# HELP elastickv_encryption_active_dek_id Currently active DEK id per purpose; 0 means the cluster has not bootstrapped that purpose.
# TYPE elastickv_encryption_active_dek_id gauge
elastickv_encryption_active_dek_id{purpose="raft"} 8
elastickv_encryption_active_dek_id{purpose="storage"} 7
# HELP elastickv_encryption_sidecar_raft_index The encryption sidecar's persisted raft_applied_index. A persistent gap below the FSM applied index is the sidecar-divergence signal.
# TYPE elastickv_encryption_sidecar_raft_index gauge
elastickv_encryption_sidecar_raft_index 4211
`),
		"elastickv_encryption_active_dek_id",
		"elastickv_encryption_sidecar_raft_index",
	))
}

// TestEncryptionStateObserverSamplesImmediatelyAndOnTick covers the
// startup case: an operator restarting a node must not wait a full
// interval before the gauges reflect reality.
func TestEncryptionStateObserverSamplesImmediatelyAndOnTick(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)
	observer := newEncryptionStateObserver(metrics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer.Start(ctx, fakeEncryptionState{storageID: 3, raftID: 4, raftIndex: 99}, time.Hour)

	// No tick has fired; the immediate sample must already be visible.
	require.InDelta(t, 3.0, testutil.ToFloat64(
		metrics.activeDEKID.WithLabelValues(encryptionPurposeStorage)), 0.0001)
	require.InDelta(t, 4.0, testutil.ToFloat64(
		metrics.activeDEKID.WithLabelValues(encryptionPurposeRaft)), 0.0001)
	require.InDelta(t, 99.0, testutil.ToFloat64(metrics.sidecarRaftIdx), 0.0001)
}

func TestEncryptionStateObserverIsInertWithoutSource(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	observer := newEncryptionStateObserver(newEncryptionMetrics(reg))

	var nilObserver *EncryptionStateObserver
	require.NotPanics(t, func() {
		observer.Start(context.Background(), nil, time.Second)
		nilObserver.Start(context.Background(), fakeEncryptionState{}, time.Second)
	})
}

// fakeKEK is a kek.Wrapper whose Unwrap can be made slow and failing.
type fakeKEK struct {
	unwrapErr error
	calls     int
}

func (k *fakeKEK) Wrap(dek []byte) ([]byte, error) { return append([]byte("w:"), dek...), nil }
func (k *fakeKEK) Name() string                    { return "fake" }
func (k *fakeKEK) Unwrap(wrapped []byte) ([]byte, error) {
	k.calls++
	if k.unwrapErr != nil {
		return nil, k.unwrapErr
	}
	return append([]byte("u:"), wrapped...), nil
}

func TestTimedKEKUnwrapperRecordsLatencyAndDelegates(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)
	inner := &fakeKEK{}

	timed := NewTimedKEKUnwrapper(inner, metrics)
	require.NotNil(t, timed)

	// Drive a deterministic 250ms round trip.
	decorator, ok := timed.(*TimedKEKUnwrapper)
	require.True(t, ok)
	base := time.Unix(1_700_000_000, 0)
	step := 0
	decorator.now = func() time.Time {
		step++
		if step == 1 {
			return base
		}
		return base.Add(250 * time.Millisecond)
	}

	out, err := timed.Unwrap([]byte("dek"))
	require.NoError(t, err)
	require.Equal(t, []byte("u:dek"), out)
	require.Equal(t, 1, inner.calls)

	// Name and Wrap must pass through untouched.
	require.Equal(t, "fake", timed.Name())
	wrapped, err := timed.Wrap([]byte("dek"))
	require.NoError(t, err)
	require.Equal(t, []byte("w:dek"), wrapped)

	families, err := reg.Gather()
	require.NoError(t, err)
	var sum float64
	for _, family := range families {
		if family.GetName() == "elastickv_encryption_kek_unwrap_seconds" {
			sum = family.GetMetric()[0].GetHistogram().GetSampleSum()
		}
	}
	require.InDelta(t, 0.25, sum, 0.0001)
}

// TestTimedKEKUnwrapperTimesAndPreservesFailures pins two things at
// once: a failing unwrap is still timed (a KMS outage shows up as slow
// errors, and dropping them would hide the signal), and the decorator
// stays transparent to errors.Is so the startup guards that match
// ErrKEKMismatch keep working through it.
func TestTimedKEKUnwrapperTimesAndPreservesFailures(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	metrics := newEncryptionMetrics(reg)
	sentinel := encryption.ErrKEKMismatch
	timed := NewTimedKEKUnwrapper(&fakeKEK{unwrapErr: sentinel}, metrics)

	_, err := timed.Unwrap([]byte("dek"))
	require.Error(t, err)
	require.True(t, errors.Is(err, sentinel),
		"the decorator must not hide the inner typed error from the startup guards")

	require.Equal(t, uint64(1), gatheredHistogramCount(t, reg, "elastickv_encryption_kek_unwrap_seconds"),
		"a failed unwrap must still be timed")
}

// gatheredHistogramCount returns a histogram's observation count.
// CollectAndCount is the wrong tool here: it counts SERIES, and a
// histogram is one series whether or not anything was observed.
func gatheredHistogramCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		return family.GetMetric()[0].GetHistogram().GetSampleCount()
	}
	t.Fatalf("histogram %s not registered", name)
	return 0
}

func TestNewTimedKEKUnwrapperReturnsInnerWhenNotObservable(t *testing.T) {
	t.Parallel()

	inner := &fakeKEK{}
	require.Same(t, inner, NewTimedKEKUnwrapper(inner, nil),
		"a node without metrics must keep the undecorated source")

	var nilRegistry *Registry
	require.Nil(t, NewTimedKEKUnwrapper(nil, nilRegistry.KEKUnwrapObserver()))
}

func TestRegistryExposesEncryptionStateAndKEKObservers(t *testing.T) {
	t.Parallel()

	reg := NewRegistry("n1", "127.0.0.1:1")
	require.NotNil(t, reg.EncryptionStateObserver())
	require.NotNil(t, reg.KEKUnwrapObserver())

	var nilRegistry *Registry
	require.Nil(t, nilRegistry.EncryptionStateObserver())
	require.Nil(t, nilRegistry.KEKUnwrapObserver())
}
