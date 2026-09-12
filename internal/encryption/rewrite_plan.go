package encryption

import "time"

// §5.4 rewrite-job decision layer.
//
// The rewrite job re-encrypts data under a new DEK so an old one can be
// retired. §5.4 is emphatic that it is NOT a single-pass conversion of
// "the live value of every key": Pebble holds MVCC history, and the
// snapshot and lease-read paths can read back any version newer than
// minRetainedTS. A rewrite that touched only the live version would
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
	RewriteReasonAlreadyFresh = "already_under_active_dek"
	RewriteReasonUnretained   = "below_min_retained_ts"
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
// minRetainedTS is the MVCC retention floor: versions at or below it
// are unreachable by any snapshot or lease read, so rewriting them
// would be pure write amplification.
func ClassifyMVCCRewrite(
	version MVCCVersionRef, retiringKeyID uint32, migrateCleartext bool, minRetainedTS uint64,
) RewriteDecision {
	if version.Tombstone {
		// No value bytes to re-encrypt. Checked first because a
		// tombstone's KeyID is meaningless.
		return RewriteDecision{Verdict: RewriteSkip, Reason: RewriteReasonTombstone}
	}
	if version.CommitTS <= minRetainedTS {
		// Below the retention floor: no reader can reach it, and the
		// retirement criterion (§5.4 item 4) is stated against
		// minRetainedTS for exactly this reason.
		return RewriteDecision{Verdict: RewriteSkip, Reason: RewriteReasonUnretained}
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
	return RewriteDecision{Verdict: RewriteSkip, Reason: RewriteReasonAlreadyFresh}
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
	started        time.Time
	written        int64
}

// NewRewriteThrottle returns a throttle for the given rate. A
// non-positive rate disables throttling, which is what --rate=0 means:
// run as fast as the store allows.
func NewRewriteThrottle(mibPerSecond float64, now time.Time) *RewriteThrottle {
	return &RewriteThrottle{
		bytesPerSecond: mibPerSecond * bytesPerMiB,
		started:        now,
	}
}

// Record accounts for a committed batch and returns how long the job
// should yield before the next one.
//
// The delay is computed from CUMULATIVE bytes against cumulative
// elapsed time, not per batch. A per-batch calculation lets a job that
// stalls for other reasons "bank" idle time and then burst well above
// the configured rate — which is precisely the spike the budget exists
// to prevent.
func (t *RewriteThrottle) Record(batchBytes int64, now time.Time) time.Duration {
	if t == nil || t.bytesPerSecond <= 0 {
		return 0
	}
	if batchBytes > 0 {
		t.written += batchBytes
	}
	required := time.Duration(float64(t.written) / t.bytesPerSecond * float64(time.Second))
	elapsed := now.Sub(t.started)
	if elapsed >= required {
		return 0
	}
	return required - elapsed
}

// Written reports the cumulative bytes recorded.
func (t *RewriteThrottle) Written() int64 {
	if t == nil {
		return 0
	}
	return t.written
}
