package kv

import (
	"math"
	"sync/atomic"
	"time"
)

const hlcLogicalBits = 16
const hlcLogicalMask uint64 = (1 << hlcLogicalBits) - 1

// HLC implements a simple hybrid logical clock suitable for issuing
// monotonically increasing timestamps across shards/raft groups.
//
// Layout (ms logical):
//
//	high 48 bits: wall clock milliseconds since Unix epoch
//	low 16 bits : logical counter to break ties when wall time does not advance
//
// This keeps ordering stable across leaders as long as clocks are loosely
// synchronized; it avoids dependence on per-raft commit indices that diverge
// between shards.
type HLC struct {
	// last holds the last issued timestamp in the same layout (ms<<bits | logical).
	last atomic.Uint64
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
func (h *HLC) Next() uint64 {
	for {
		prev := h.last.Load()
		wallPart := prev >> hlcLogicalBits
		logicalPart := prev & hlcLogicalMask
		prevWall := clampUint64ToInt64(wallPart)
		prevLogical := clampUint64ToUint16(logicalPart)

		nowMs := time.Now().UnixMilli()
		newWall := nowMs
		newLogical := uint16(0)

		if nowMs <= prevWall {
			newWall = prevWall
			newLogical = prevLogical + 1
			if newLogical == 0 { // overflow
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
