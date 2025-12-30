package kv

import (
	"math"
	"sync"
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
	mu       sync.Mutex
	lastWall int64
	logical  uint16
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
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().UnixMilli()
	if now > h.lastWall {
		h.lastWall = now
		h.logical = 0
	} else {
		h.logical++
		if h.logical == 0 { // overflow; bump wall to keep monotonicity
			h.lastWall++
		}
	}

	wall := nonNegativeUint64(h.lastWall)
	return (wall << hlcLogicalBits) | uint64(h.logical)
}

// Observe bumps the local clock if a higher timestamp is seen.
func (h *HLC) Observe(ts uint64) {
	wallPart := ts >> hlcLogicalBits
	logicalPart := ts & hlcLogicalMask

	h.mu.Lock()
	defer h.mu.Unlock()

	wall := clampUint64ToInt64(wallPart)
	logical := clampUint64ToUint16(logicalPart)

	if wall > h.lastWall || (wall == h.lastWall && logical > h.logical) {
		h.lastWall = wall
		h.logical = logical
	}
}
