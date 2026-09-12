package monitoring

import (
	"strconv"
	"sync"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/prometheus/client_golang/prometheus"
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
		writeCtr: make(map[uint32]prometheus.Counter),
	}
	registerer.MustRegister(
		m.decryptFailures,
		m.writesPerDEK,
		m.valueOverhead,
	)
	return m
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
