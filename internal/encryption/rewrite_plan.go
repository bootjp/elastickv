package encryption

import (
	"math"
	"time"
)

// §5.4 rewrite-job decision layer.
//
// The rewrite job re-encrypts data under a new DEK so an old one can be
// retired. §5.4 is emphatic that it is NOT a single-pass conversion of
// "the live value of every key": Pebble holds MVCC history, and the
// snapshot and lease-read paths can read back retained versions, including
// the newest version at or below minRetainedTS. A rewrite that touched
// only the live version would
// leave older versions under the retiring DEK and quietly break
// snapshot reads the moment it was unloaded.
//
// Two pure decisions live here — which versions to rewrite, and when to
// yield to stay inside the write-rate budget — so both are testable
// without a Pebble instance. Driving the iterator and the batch is the
// execution half.

// RewriteVerdict is what the job does with one MVCC version.
type RewriteVerdict int

const (
	// RewriteSkip leaves the version untouched.
	RewriteSkip RewriteVerdict = iota
	// RewriteReencrypt re-encrypts the value under the active DEK, at
	// the SAME internal key. §5.4: no new MVCC version, no OCC
	// conflict, no visible change to readers.
	RewriteReencrypt
)

func (v RewriteVerdict) String() string {
	if v == RewriteReencrypt {
		return "reencrypt"
	}
	return "skip"
}

// Reasons, a closed set so the job can meter them directly.
const (
	RewriteReasonRetiringDEK  = "retiring_dek"
	RewriteReasonCleartext    = "cleartext_migration"
	RewriteReasonTombstone    = "tombstone"
	RewriteReasonDifferentDEK = "different_dek"
)

// RewriteDecision is a verdict plus its reason.
type RewriteDecision struct {
	Verdict RewriteVerdict
	Reason  string
}

// MVCCVersionRef is one retained version, as the iterator sees it.
//
// The iteration unit is (user_key, version_ts) rather than user_key —
// that distinction is the whole point of §5.4, so the type carries the
// timestamp rather than letting a caller forget it.
type MVCCVersionRef struct {
	CommitTS uint64
	// KeyID is the DEK this version's envelope names, or zero when the
	// version is stored cleartext.
	KeyID uint32
	// Cleartext is the MVCC metadata bit saying this version predates
	// the §7.1 cutover and holds no envelope.
	Cleartext bool
	// Tombstone versions carry no value bytes, so there is nothing to
	// re-encrypt.
	Tombstone bool
	// ValueBytes sizes the write for the rate budget.
	ValueBytes int64
}

// ClassifyMVCCRewrite decides one version's fate.
//
// retiringKeyID is the DEK being retired. migrateCleartext enables the
// §7.1 cleartext→encrypted sweep; it is separate from the retiring DEK
// because the two jobs run at different times and conflating them
// would have a routine rotation silently start encrypting data the
// operator had not opted in to encrypting.
//
// The iterator supplies physically retained versions. Its newest version
// at or below the retention floor can still serve a read at the floor, so
// the classifier must not skip an old version based on its timestamp alone.
func ClassifyMVCCRewrite(
	version MVCCVersionRef, retiringKeyID uint32, migrateCleartext bool,
) RewriteDecision {
	if version.Tombstone {
		// No value bytes to re-encrypt. Checked first because a
		// tombstone's KeyID is meaningless.
		return RewriteDecision{Verdict: RewriteSkip, Reason: RewriteReasonTombstone}
	}
	if version.Cleartext {
		if migrateCleartext {
			return RewriteDecision{Verdict: RewriteReencrypt, Reason: RewriteReasonCleartext}
		}
		return RewriteDecision{Verdict: RewriteSkip, Reason: RewriteReasonCleartext}
	}
	if version.KeyID == retiringKeyID {
		return RewriteDecision{Verdict: RewriteReencrypt, Reason: RewriteReasonRetiringDEK}
	}
	return RewriteDecision{Verdict: RewriteSkip, Reason: RewriteReasonDifferentDEK}
}

// bytesPerMiB converts the operator-facing `--rate=N MiB/s` unit into
// the byte-denominated arithmetic the throttle runs on.
const bytesPerMiB = 1024 * 1024

// RewriteThrottle implements the §5.4 `--rate=N MiB/s` write-rate
// budget by telling the job how long to yield between batches.
//
// Rate-limiting the rewrite matters because it competes with live
// traffic for the same Pebble write path: an unthrottled sweep of MVCC
// history is a sustained write amplification spike against a database
// that is also serving requests.
type RewriteThrottle struct {
	bytesPerSecond float64
	deadline       time.Time
	written        int64
}

// NewRewriteThrottle returns a throttle for the given rate. A
// non-positive rate disables throttling, which is what --rate=0 means:
// run as fast as the store allows.
func NewRewriteThrottle(mibPerSecond float64, now time.Time) *RewriteThrottle {
	return &RewriteThrottle{
		bytesPerSecond: mibPerSecond * bytesPerMiB,
		deadline:       now,
	}
}

// Record accounts for a committed batch and returns how long the job
// should yield before the next one.
//
// Each batch advances a virtual deadline from the later of the previous
// deadline and now. Idle time therefore never becomes credit for a burst.
func (t *RewriteThrottle) Record(batchBytes int64, now time.Time) time.Duration {
	if t == nil {
		return 0
	}
	if batchBytes > 0 {
		if batchBytes > math.MaxInt64-t.written {
			t.written = math.MaxInt64
		} else {
			t.written += batchBytes
		}
	}
	if t.bytesPerSecond <= 0 || math.IsNaN(t.bytesPerSecond) {
		return 0
	}
	if now.After(t.deadline) {
		t.deadline = now
	}
	if batchBytes > 0 {
		t.deadline = t.deadline.Add(rewriteBatchDelay(batchBytes, t.bytesPerSecond))
	}
	if !t.deadline.After(now) {
		return 0
	}
	return t.deadline.Sub(now)
}

func rewriteBatchDelay(batchBytes int64, bytesPerSecond float64) time.Duration {
	seconds := float64(batchBytes) / bytesPerSecond
	delay := seconds * float64(time.Second)
	if math.IsInf(delay, 1) || delay >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(delay)
}

// Written reports the cumulative bytes recorded.
func (t *RewriteThrottle) Written() int64 {
	if t == nil {
		return 0
	}
	return t.written
}
