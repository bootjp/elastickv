package adapter

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
)

// snapshotTS picks a safe snapshot timestamp:
// - uses the store's last commit watermark if available,
// - otherwise the coordinator's HLC current value,
// - and falls back to MaxUint64 if neither is set.
func snapshotTS(clock *kv.HLC, st store.MVCCStore) uint64 {
	ts := uint64(0)
	if st != nil {
		ts = st.LastCommitTS()
	}
	if clock != nil {
		if cur := clock.Current(); cur > ts {
			ts = cur
		}
	}
	if ts == 0 {
		ts = ^uint64(0)
	}
	return ts
}
