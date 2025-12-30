package adapter

import "github.com/bootjp/elastickv/kv"

// snapshotTS returns a timestamp suitable for snapshot reads without
// unnecessarily advancing the logical clock. It relies solely on the shared
// HLC; if none has been issued yet, fall back to MaxUint64 to see latest
// committed versions irrespective of local clock lag.
func snapshotTS(clock *kv.HLC) uint64 {
	if clock == nil {
		return ^uint64(0)
	}
	if cur := clock.Current(); cur != 0 {
		return cur
	}
	return ^uint64(0)
}
