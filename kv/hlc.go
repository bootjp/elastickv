package kv

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
)

// ErrCeilingExpired is returned by HLC.NextFenced() when the
// Raft-agreed physical ceiling has expired — i.e. the wall clock has
// caught up to or passed the ceiling, meaning RunHLCLeaseRenewal has
// not applied a fresh ceiling within `hlcPhysicalWindowMs` of the
// current wall time. Callers MUST refuse to commit and propagate this
// to the client.
//
// This implements HLC-4 precondition (iii) from
// docs/design/2026_05_28_implemented_tla_safety_spec.md §5.1: every
// persistence-grade ts allocation is gated on `wall_now <
// physicalCeiling`. The TLA+ MCHLC_gap.cfg counterexample at depth 5
// demonstrates the safety property this enforces.
//
// Pre-bootstrap (ceiling == 0) is intentionally NOT fenced: there is
// no prior leader to protect against and tests / demo clusters need
// to issue ts before the first RunHLCLeaseRenewal cycle.  Strict
// bootstrap fencing is a follow-up consideration; the current
// semantics match the spec wherever the prior-leader hazard is real.
var ErrCeilingExpired = errors.New("hlc: physical ceiling expired (wall_now >= physicalCeiling); refusing to issue persistence timestamp")

const hlcLogicalBits = 16
const hlcLogicalMask uint64 = (1 << hlcLogicalBits) - 1

// HLCLogicalBits is the number of low bits in an HLC timestamp
// reserved for the in-memory logical counter (vs the upper bits
// which encode the Raft-agreed wall-clock millis). Exported so
// downstream tools — admin dashboard ISO-8601 formatting being the
// motivating case — can recover the physical half without
// hard-coding a magic number that silently drifts when the layout
// changes (Claude Issue 4 on PR #658).
const HLCLogicalBits = hlcLogicalBits

// HLC implements a hybrid logical clock where the physical part is agreed upon
// via Raft consensus and the logical counter is managed purely in memory.
//
// Layout (ms | logical):
//
//	high 48 bits: wall clock milliseconds since Unix epoch  ← Raft-agreed physical part
//	low 16 bits : logical counter                           ← in-memory only
//
// Physical ceiling (前半, consensus):
//
//	The leader periodically commits a HLC lease entry to the Raft log that
//	establishes an upper bound for the physical timestamp (physicalCeiling).
//	All nodes apply this entry via the FSM, advancing their local ceiling.
//	When a new leader is elected it inherits the committed ceiling from the
//	FSM state so it never issues timestamps below the previous leader's window.
//
// Logical counter (後半, memory):
//
//	The 16-bit counter increments purely in memory on every Next() call within
//	the same millisecond. It resets to 0 whenever wall time advances, and
//	overflows by bumping the wall millisecond by one. No Raft round-trip is
//	needed for logical counter advancement.
type HLC struct {
	// last holds the last issued timestamp in the same layout (ms<<bits | logical).
	last atomic.Uint64
	// physicalCeiling is the Raft-agreed upper bound for the physical (wall-clock)
	// part in Unix milliseconds. It is set by the FSM when a HLC lease entry is
	// applied, and is zero when no ceiling has been established yet.
	// Next() uses max(now, physicalCeiling) so that a new leader always starts
	// issuing timestamps above the previous leader's committed window.
	physicalCeiling atomic.Int64
	// nextFencedRejections counts the number of NextFenced() calls that
	// returned ErrCeilingExpired (HLC-4 (iii) fence fired). Exposed via
	// NextFencedRejections() so the monitoring layer can export it as a
	// Prometheus counter without coupling the kv package to a metric type.
	// A non-zero value here means the leader's lease renewal stopped long
	// enough for wall_now to catch up to physicalCeiling — operators should
	// alert on its rate.
	nextFencedRejections atomic.Uint64
}

func nonNegativeUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func clampUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func clampUint64ToUint16(v uint64) uint16 {
	max := uint64(^uint16(0))
	if v > max {
		return uint16(max)
	}
	return uint16(v)
}

func NewHLC() *HLC {
	return &HLC{}
}

// Next returns the next hybrid logical timestamp, ignoring the
// HLC-4 physical-ceiling fence.  Kept for non-persistence callers
// (diagnostics, identifiers, retry IDs, tests, demo wiring) and for
// adapter sites whose fail-closed migration is not yet completed.
//
// NEW persistence-grade allocations (everything that ends up as a
// startTS / commitTS, an MVCC write timestamp, or a lease/expiry
// boundary) MUST go through NextFenced instead — Next bypasses the
// HLC-4 (iii) ceiling fence and can therefore issue a timestamp inside
// a stale leader window after lease renewal stops.
//
// Physical part (upper 48 bits): derived from the wall clock, but never less
// than the Raft-agreed physicalCeiling so that a newly elected leader cannot
// issue timestamps that collide with the previous leader's window.
//
// Logical part (lower 16 bits): a pure in-memory counter that increments
// without any Raft round-trip. It resets to 0 whenever the physical millisecond
// advances and overflows by bumping the physical millisecond by one.
func (h *HLC) Next() uint64 {
	ts, _ := h.nextLocked(false)
	return ts
}

// NextFenced returns the next hybrid logical timestamp with the
// HLC-4 (iii) physical-ceiling fence enforced.  ALL persistence-grade
// allocations (startTS, commitTS, MVCC write ts, lease/expiry bounds)
// MUST go through this entry point so that an expired-ceiling
// allocation fails closed instead of silently issuing a ts that could
// collide with a subsequent leader's window after renewal catches up.
//
// Fence semantics:
//   - ceiling == 0 (pre-bootstrap, no prior leader): no fence, identical to Next.
//   - ceiling > 0 AND wall_now >= ceiling: returns (0, ErrCeilingExpired).
//   - ceiling > 0 AND wall_now < ceiling: floor wall at ceiling, then proceed.
//
// The TLA+ proof for this lives in tla/hlc/MCHLC_gap.cfg (HLC-4
// counterexample, depth 5) — see docs/design/2026_05_28_implemented_tla_safety_spec.md §5.1.
func (h *HLC) NextFenced() (uint64, error) {
	return h.nextLocked(true)
}

func (h *HLC) nextLocked(fence bool) (uint64, error) {
	for {
		prev := h.last.Load()
		wallPart := prev >> hlcLogicalBits
		logicalPart := prev & hlcLogicalMask
		prevWall := clampUint64ToInt64(wallPart)
		prevLogical := clampUint64ToUint16(logicalPart)

		nowMs := time.Now().UnixMilli()
		ceiling := h.physicalCeiling.Load()
		if ceiling > 0 {
			if fence && nowMs >= ceiling {
				// HLC-4 precondition (iii): ceiling has expired.  Fail
				// closed rather than issue a timestamp that could collide
				// with a subsequent leader's window after renewal catches
				// up.  Increment the counter so the monitoring layer can
				// alert on the rate (see NextFencedRejections).
				h.nextFencedRejections.Add(1)
				return 0, errors.WithStack(ErrCeilingExpired)
			}
			if nowMs < ceiling {
				// Physical part: floor at the Raft-agreed ceiling so a
				// new leader always starts above the previous leader's
				// issued window.
				nowMs = ceiling
			}
			// Non-fenced path with nowMs >= ceiling: keep nowMs as-is.
		}

		newWall := nowMs
		newLogical := uint16(0) // logical counter resets in memory for each new ms

		if nowMs <= prevWall {
			newWall = prevWall
			// Logical counter advances purely in memory — no Raft round-trip needed.
			newLogical = prevLogical + 1
			if newLogical == 0 { // overflow: bump physical ms by one
				newWall++
			}
		}

		next := (nonNegativeUint64(newWall) << hlcLogicalBits) | uint64(newLogical)
		if h.last.CompareAndSwap(prev, next) {
			return next, nil
		}
	}
}

// Current returns the last issued or observed HLC value without advancing it.
// If no timestamp has been generated yet, it returns 0.
func (h *HLC) Current() uint64 {
	return h.last.Load()
}

// Observe bumps the local clock if a higher timestamp is seen.
func (h *HLC) Observe(ts uint64) {
	for {
		prev := h.last.Load()
		if ts <= prev {
			return
		}
		if h.last.CompareAndSwap(prev, ts) {
			return
		}
	}
}

// SetPhysicalCeiling atomically advances the Raft-agreed physical ceiling.
// It is called by the FSM whenever a HLC lease entry is applied to the log.
// The ceiling is monotonically increasing: calls with a smaller value are
// silently ignored.
func (h *HLC) SetPhysicalCeiling(ms int64) {
	for {
		prev := h.physicalCeiling.Load()
		if ms <= prev {
			return
		}
		if h.physicalCeiling.CompareAndSwap(prev, ms) {
			return
		}
	}
}

// PhysicalCeiling returns the last Raft-committed physical ceiling in Unix
// milliseconds. Returns 0 if no ceiling has been established yet.
func (h *HLC) PhysicalCeiling() int64 {
	return h.physicalCeiling.Load()
}

// NextFencedRejections returns the cumulative count of NextFenced() calls
// that returned ErrCeilingExpired since process start.  The monitoring
// layer reads this on a fixed interval and exports it as a Prometheus
// counter so operators can alert on the rate.
//
// A non-zero value here means the HLC-4 (i) bounded-skew assumption
// (`MaxClockSkewMs < HlcPhysicalWindowMs` — see
// docs/design/2026_05_28_implemented_tla_safety_spec.md §5.1) has at some
// point been violated by enough margin that wall_now caught up to
// physicalCeiling and the fence fired — typically because the leader's
// lease renewal stopped (network partition, GC pause, …) for longer than
// `hlcPhysicalWindowMs`.
func (h *HLC) NextFencedRejections() uint64 {
	return h.nextFencedRejections.Load()
}
