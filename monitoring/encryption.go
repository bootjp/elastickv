package monitoring

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Purpose label values for elastickv_encryption_active_dek_id.
const (
	encryptionPurposeStorage = "storage"
	encryptionPurposeRaft    = "raft"
)

// EncryptionObserver is the surface the storage envelope path uses to
// report §9.2 encryption telemetry. The store holds it as an interface
// so a node with metrics disabled carries a nil observer and pays only
// a nil check per write.
//
// ObserveEncryptionWrite takes both sizes rather than a precomputed
// overhead so the caller cannot get the subtraction order wrong, and so
// the histogram's unit stays owned by this package.
type EncryptionObserver interface {
	// ObserveEncryptionDecryptFailure counts one failed envelope
	// decode-or-open, labelled by an encryption.DecryptFailureReason*
	// value. Any non-zero rate is paging-grade per §9.2.
	ObserveEncryptionDecryptFailure(reason string)

	// ObserveEncryptionWrite counts one envelope emitted under keyID
	// and records payloadBytes-plaintextBytes as value overhead.
	ObserveEncryptionWrite(keyID uint32, plaintextBytes, payloadBytes int)
}

// EncryptionStateSource is the sidecar-state surface the §9.2
// collector polls. *encryption.StateCache implements it.
//
// Declared here rather than importing the concrete cache so a test can
// drive the collector without building an applier, matching the
// HLCSource precedent in this package.
type EncryptionStateSource interface {
	ActiveStorageKeyID() (uint32, bool)
	ActiveRaftKeyID() (uint32, bool)
	SidecarRaftAppliedIndex() uint64
}

// EncryptionMetrics implements EncryptionObserver over Prometheus.
//
// writesPerDEK is labelled by key_id, whose cardinality is bounded by
// the number of DEKs the cluster has rotated through rather than by
// traffic. The per-key_id child counter is memoized because the write
// path would otherwise strconv-format the key_id on every write; key_ids
// are few and long-lived, so the cache is effectively read-only after
// the first write under each DEK.
type EncryptionMetrics struct {
	decryptFailures *prometheus.CounterVec
	writesPerDEK    *prometheus.CounterVec
	valueOverhead   prometheus.Histogram

	activeDEKID     *prometheus.GaugeVec
	sidecarRaftIdx  prometheus.Gauge
	kekUnwrapSecond prometheus.Histogram

	mu       sync.RWMutex
	writeCtr map[uint32]prometheus.Counter
}

func newEncryptionMetrics(registerer prometheus.Registerer) *EncryptionMetrics {
	m := &EncryptionMetrics{
		decryptFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_encryption_decrypt_failures_total",
				Help: "Total encrypted-value reads that failed to decode or authenticate, by failure reason. Any non-zero value is a paging-grade signal.",
			},
			[]string{"reason"},
		),
		writesPerDEK: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_encryption_writes_per_dek",
				Help: "Total encrypted values written under each DEK, by key_id. Drives the writes-based rotation trigger.",
			},
			[]string{"key_id"},
		),
		valueOverhead: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "elastickv_encryption_value_overhead_bytes",
				Help: "Per-write envelope overhead in bytes (stored payload size minus plaintext size). Negative observations are possible when compression is enabled.",
				// Spans the fixed floor (header+tag, tens of bytes)
				// through the compression-dominated tail, where a
				// large compressible value makes the overhead
				// strongly negative.
				Buckets: []float64{-1048576, -65536, -4096, -256, 0, 16, 32, 48, 64, 96, 128, 256, 1024, 4096, 65536},
			},
		),
		activeDEKID: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_encryption_active_dek_id",
				Help: "Currently active DEK id per purpose; 0 means the cluster has not bootstrapped that purpose.",
			},
			[]string{"purpose"},
		),
		sidecarRaftIdx: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "elastickv_encryption_sidecar_raft_index",
				Help: "The encryption sidecar's persisted raft_applied_index. A persistent gap below the FSM applied index is the sidecar-divergence signal.",
			},
		),
		kekUnwrapSecond: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "elastickv_encryption_kek_unwrap_seconds",
				Help: "KEK unwrap round-trip latency. For a remote KMS this is a network call; sustained growth indicates a KMS outage.",
				// Spans a local file unwrap (microseconds) through a
				// remote KMS call and into timeout territory.
				Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
			},
		),
		writeCtr: make(map[uint32]prometheus.Counter),
	}
	registerer.MustRegister(
		m.decryptFailures,
		m.writesPerDEK,
		m.valueOverhead,
		m.activeDEKID,
		m.sidecarRaftIdx,
		m.kekUnwrapSecond,
	)
	// Publish the pre-bootstrap posture immediately so both purposes
	// exist as series from process start. Without this an alert on
	// active_dek_id == 0 could not distinguish "not bootstrapped"
	// from "this node never reported", which are very different.
	m.activeDEKID.WithLabelValues(encryptionPurposeStorage).Set(0)
	m.activeDEKID.WithLabelValues(encryptionPurposeRaft).Set(0)
	return m
}

// ObserveEncryptionKEKUnwrap records one KEK unwrap round-trip.
func (m *EncryptionMetrics) ObserveEncryptionKEKUnwrap(d time.Duration) {
	if m == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	m.kekUnwrapSecond.Observe(d.Seconds())
}

// observeState publishes one sample of the sidecar-derived gauges.
func (m *EncryptionMetrics) observeState(source EncryptionStateSource) {
	if m == nil || source == nil {
		return
	}
	storageID, _ := source.ActiveStorageKeyID()
	raftID, _ := source.ActiveRaftKeyID()
	// The `ok` half is deliberately dropped: it is exactly
	// `id != 0`, and 0 is the design's "not bootstrapped" sentinel,
	// so reporting the raw id keeps the metric and the sentinel
	// carrying the same meaning.
	m.activeDEKID.WithLabelValues(encryptionPurposeStorage).Set(float64(storageID))
	m.activeDEKID.WithLabelValues(encryptionPurposeRaft).Set(float64(raftID))
	m.sidecarRaftIdx.Set(float64(source.SidecarRaftAppliedIndex()))
}

// ObserveEncryptionDecryptFailure counts one decrypt-path failure.
// An unrecognised reason is folded into the doc's `unknown` bucket
// rather than registering a new label value, so the series stays
// bounded even if a caller passes something unexpected.
func (m *EncryptionMetrics) ObserveEncryptionDecryptFailure(reason string) {
	if m == nil {
		return
	}
	m.decryptFailures.WithLabelValues(normalizeDecryptFailureReason(reason)).Inc()
}

// ObserveEncryptionWrite records one encrypted write.
//
// The overhead observation is deliberately not clamped at zero:
// compression (§6.4) legitimately makes the stored payload smaller
// than the plaintext, and clamping would hide exactly the compression
// behaviour the histogram exists to show.
func (m *EncryptionMetrics) ObserveEncryptionWrite(keyID uint32, plaintextBytes, payloadBytes int) {
	if m == nil {
		return
	}
	m.writeCounter(keyID).Inc()
	m.valueOverhead.Observe(float64(payloadBytes - plaintextBytes))
}

// writeCounter returns the memoized per-key_id child counter,
// creating it on first use under the write lock.
func (m *EncryptionMetrics) writeCounter(keyID uint32) prometheus.Counter {
	m.mu.RLock()
	ctr, ok := m.writeCtr[keyID]
	m.mu.RUnlock()
	if ok {
		return ctr
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if ctr, ok := m.writeCtr[keyID]; ok {
		return ctr
	}
	ctr = m.writesPerDEK.WithLabelValues(strconv.FormatUint(uint64(keyID), 10))
	m.writeCtr[keyID] = ctr
	return ctr
}

// normalizeDecryptFailureReason keeps the reason label inside the
// closed §9.2 set.
func normalizeDecryptFailureReason(reason string) string {
	switch reason {
	case encryption.DecryptFailureReasonTagMismatch,
		encryption.DecryptFailureReasonUnknownKeyID,
		encryption.DecryptFailureReasonTruncated,
		encryption.DecryptFailureReasonBadVersion,
		encryption.DecryptFailureReasonUnknown:
		return reason
	default:
		return encryption.DecryptFailureReasonUnknown
	}
}

// EncryptionStateObserver polls an EncryptionStateSource and mirrors
// it into the §9.2 gauges.
type EncryptionStateObserver struct {
	metrics *EncryptionMetrics
}

func newEncryptionStateObserver(metrics *EncryptionMetrics) *EncryptionStateObserver {
	return &EncryptionStateObserver{metrics: metrics}
}

// Start samples source immediately and then on every tick until ctx is
// cancelled. A nil receiver or nil source silently no-ops so a node
// without encryption wired needs no conditional at the call site.
func (o *EncryptionStateObserver) Start(ctx context.Context, source EncryptionStateSource, interval time.Duration) {
	if o == nil || source == nil {
		return
	}
	if interval <= 0 {
		interval = defaultObserveInterval
	}
	o.metrics.observeState(source)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.metrics.observeState(source)
			}
		}
	}()
}

// TimedKEKUnwrapper decorates a KEK source so every unwrap feeds
// elastickv_encryption_kek_unwrap_seconds.
//
// It wraps kek.Wrapper (rather than the narrower
// encryption.KEKUnwrapper) because that is the type main.go threads
// through the shard-group wiring; Wrap and Name delegate untouched.
// Only Unwrap is timed, per §9.2: unwrap is the call on the startup
// and apply paths, so a KMS outage surfaces there first.
type TimedKEKUnwrapper struct {
	inner    kek.Wrapper
	observer KEKUnwrapObserver
	now      func() time.Time
}

// KEKUnwrapObserver receives KEK unwrap durations.
type KEKUnwrapObserver interface {
	ObserveEncryptionKEKUnwrap(d time.Duration)
}

// NewTimedKEKUnwrapper returns inner unchanged when either inner or
// observer is nil, so wiring stays a single unconditional call and a
// node without a KEK source keeps passing the same nil it had before.
func NewTimedKEKUnwrapper(inner kek.Wrapper, observer KEKUnwrapObserver) kek.Wrapper {
	if inner == nil || observer == nil {
		return inner
	}
	return &TimedKEKUnwrapper{inner: inner, observer: observer, now: time.Now}
}

// Unwrap times the inner call. Failures are timed too: a KMS outage
// usually shows up as slow errors, and excluding them would hide the
// signal this histogram exists to expose.
func (u *TimedKEKUnwrapper) Unwrap(wrapped []byte) ([]byte, error) {
	start := u.now()
	out, err := u.inner.Unwrap(wrapped)
	u.observer.ObserveEncryptionKEKUnwrap(u.now().Sub(start))
	if err != nil {
		// errors.Wrap preserves Is/As, so the startup guards that
		// match ErrKEKMismatch and friends still see through this.
		return nil, errors.Wrapf(err, "kek %s: unwrap", u.inner.Name())
	}
	return out, nil
}

// Wrap delegates untouched.
func (u *TimedKEKUnwrapper) Wrap(dek []byte) ([]byte, error) {
	return u.inner.Wrap(dek)
}

// Name delegates so the decorated source still reports its real
// provider ("file", "aws-kms", ...) in logs and the status RPC.
func (u *TimedKEKUnwrapper) Name() string { return u.inner.Name() }
