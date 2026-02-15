package kv

import "time"

func hlcWallNow() uint64 {
	ms := time.Now().UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms) << hlcLogicalBits
}

func hlcWallFromNowMs(deltaMs uint64) uint64 {
	now := hlcWallNow()
	if deltaMs == 0 {
		return now
	}

	delta := deltaMs << hlcLogicalBits
	if delta>>hlcLogicalBits != deltaMs {
		// Saturate if the shift overflowed.
		return ^uint64(0)
	}
	if ^uint64(0)-now < delta {
		return ^uint64(0)
	}
	return now + delta
}
