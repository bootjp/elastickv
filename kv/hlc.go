package kv

import (
	"math"
	"sync/atomic"
	"time"
)

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

// Next returns the next hybrid logical timestamp.
//
// Physical part (upper 48 bits): derived from the wall clock, but never less
// than the Raft-agreed physicalCeiling so that a newly elected leader cannot
// issue timestamps that collide with the previous leader's window.
//
// Logical part (lower 16 bits): a pure in-memory counter that increments
// without any Raft round-trip. It resets to 0 whenever the physical millisecond
// advances and overflows by bumping the physical millisecond by one.
func (h *HLC) Next() uint64 {
	for {
		prev := h.last.Load()
		wallPart := prev >> hlcLogicalBits
		logicalPart := prev & hlcLogicalMask
		prevWall := clampUint64ToInt64(wallPart)
		prevLogical := clampUint64ToUint16(logicalPart)

		nowMs := time.Now().UnixMilli()
		// Physical part: floor at the Raft-agreed ceiling so a new leader always
		// starts above the previous leader's issued window.
		if ceiling := h.physicalCeiling.Load(); ceiling > 0 && nowMs < ceiling {
			nowMs = ceiling
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
			return next
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
