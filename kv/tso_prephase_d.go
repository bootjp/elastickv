package kv

import (
	"time"

	"github.com/cockroachdb/errors"
)

// The §4.3 closure of the pre-Phase-D resolution carve-out
// (docs/design/2026_09_02_proposed_prephase_d_resolution_evidence.md).
//
// The carve-out exists because a cross-shard transaction that began before the
// Phase-D marker can still hold unresolved intents afterwards, and resolving
// them replays the commit timestamp the primary recorded. Rejecting that
// strands the transaction with its secondary keys locked, so the exemption
// cannot simply be deleted.
//
// What it could not do is tell a genuine replay from a fabricated one: both
// halves of its proof -- the resolution flag and the start timestamp -- arrive
// inside the request being validated. A caller reaching the internal listener
// could PREPARE at an arbitrary pre-D start and then COMMIT at another pre-D
// timestamp, inserting a write retroactively below timestamps that have already
// been read.
//
// Two mechanisms close it, because neither covers the whole surface:
//
//   - Durable evidence, where it exists. For a COMMIT whose primary key this
//     node can read, the primary's own commit record has to agree with the
//     claimed timestamp. This is exactly the evidence the legitimate resolver
//     already reads, so the honest path satisfies it by construction.
//   - A bound on intent CREATION, everywhere else. The attacker's leverage is
//     the PREPARE, not the COMMIT: they must be able to create a fresh pre-D
//     intent after Phase D is active. A transaction genuinely in flight across
//     the marker can only stay preparable for its lock TTL, so admitting a
//     pre-D start only inside that window shuts the fabrication route while
//     leaving the commit carve-out open indefinitely, as correctness requires.
//
// The bound reads a wall clock, which CLAUDE.md restricts to diagnostics. It is
// argued rather than assumed: this is an ADMISSION decision about whether a
// request may create state, not an ordering decision about where that state
// sorts. No visibility, OCC or MVCC comparison consults it, and a clock that is
// wrong only widens or narrows who may create a pre-D intent -- it can never
// place a write at the wrong point in the timestamp order. Skew therefore costs
// availability for legacy resolution, never correctness.

const (
	// prePhaseDAdmissionGrace is the slack added to the lock TTL when bounding
	// pre-Phase-D intent creation.
	//
	// It absorbs clock skew between nodes and the spread of a rolling Phase-D
	// activation, both of which can make a legitimately in-flight transaction
	// look older than it is. Too small strands long-TTL transactions; too large
	// only extends a window that is already bounded, so it errs long.
	prePhaseDAdmissionGrace = time.Hour

	// prePhaseDAdmissionWindow is how long after Phase-D activation a pre-D
	// start timestamp may still create an intent. Past it, no NEW pre-D intent
	// can exist, so the commit carve-out only ever applies to intents that
	// genuinely predate the marker.
	prePhaseDAdmissionWindow = time.Duration(maxTxnLockTTLms)*time.Millisecond + prePhaseDAdmissionGrace
)

// ErrPrePhaseDWindowClosed reports a pre-Phase-D start timestamp offered after
// the admission window shut. Its own sentinel: this is not "the timestamp is
// invalid" but "no transaction can still legitimately be preparing at it".
var ErrPrePhaseDWindowClosed = errors.New(
	"tso: pre-phase-D intent creation window has closed")

// ErrPrePhaseDCommitUnproven reports a pre-Phase-D commit resolution whose
// claimed timestamp the primary's durable record does not support.
var ErrPrePhaseDCommitUnproven = errors.New(
	"tso: pre-phase-D commit timestamp is not supported by the primary's record")

// PhaseDActivationMillis is the wall-clock instant Phase D became active,
// taken from the physical half of the floor.
//
// The floor is the highest timestamp issued before the marker applied, so its
// physical half IS the activation instant in the same units a wall clock
// reports -- no separate record is needed.
func PhaseDActivationMillis(phaseDFloor uint64) int64 {
	return clampUint64ToInt64(phaseDFloor >> HLCLogicalBits)
}

// PrePhaseDStartAdmissible reports whether a pre-Phase-D start timestamp may
// still create an intent.
//
// Pure, so the window can be reasoned about and tested without a clock: the
// caller supplies now.
func PrePhaseDStartAdmissible(phaseDFloor uint64, nowMs int64, window time.Duration) bool {
	if phaseDFloor == 0 {
		// Phase D never activated, so there is no window to be outside of and
		// nothing has been read at a post-marker timestamp yet.
		return true
	}
	return nowMs < PhaseDActivationMillis(phaseDFloor)+window.Milliseconds()
}

// PrimaryCommitEvidence is what a node can say about the primary's durable
// record for a transaction it is being asked to resolve.
type PrimaryCommitEvidence struct {
	// Readable is false when this node does not host the primary's shard, or
	// that shard is not locally ready. It is the "no safe verdict from evidence
	// alone" case, not a negative answer.
	Readable bool
	// Found reports whether a commit record exists for (primaryKey, startTS).
	Found bool
	// CommitTS is the timestamp that record carries.
	CommitTS uint64
}

// ClassifyPrePhaseDCommit decides whether a pre-Phase-D commit resolution may
// be admitted.
//
// Evidence first, window second. Where the primary record is readable it is
// decisive in both directions: a matching record admits, and a missing or
// disagreeing one refuses regardless of the window, because the claim is then
// positively contradicted rather than merely unproven.
//
// Where it is not readable -- the primary lives on another group -- there is no
// verdict to draw from evidence, and refusing would break legitimate
// cross-shard legacy resolution. Those fall back to the creation window, which
// is what makes the fallback safe: past it no new pre-D intent can exist, so
// the only resolutions left are for intents that genuinely predate the marker.
//
// An ABORT resolution synthesises its timestamp rather than replaying a
// recorded one, so no record can support it; it is always a window decision.
func ClassifyPrePhaseDCommit(
	ev PrimaryCommitEvidence,
	claimedCommitTS uint64,
	isAbort bool,
	withinWindow bool,
) error {
	if isAbort || !ev.Readable {
		if withinWindow {
			return nil
		}
		return errors.Wrapf(ErrPrePhaseDWindowClosed,
			"commit_ts=%d abort=%t primary_readable=%t", claimedCommitTS, isAbort, ev.Readable)
	}
	if !ev.Found {
		return errors.Wrapf(ErrPrePhaseDCommitUnproven,
			"no commit record for the primary at commit_ts=%d", claimedCommitTS)
	}
	if ev.CommitTS != claimedCommitTS {
		return errors.Wrapf(ErrPrePhaseDCommitUnproven,
			"primary recorded commit_ts=%d, request claims %d", ev.CommitTS, claimedCommitTS)
	}
	return nil
}

// prePhaseDNowMillis is the wall clock the admission window is measured
// against. A variable so tests can drive the window deterministically; see the
// header for why a wall-clock read is acceptable here and nowhere near an
// ordering decision.
var prePhaseDNowMillis = func() int64 { return time.Now().UnixMilli() }
