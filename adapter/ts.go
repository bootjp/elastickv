package adapter

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
)

// snapshotTS picks a safe snapshot timestamp based solely on the store's
// last committed watermark. The HLC must NOT be used here because
// clock.Current() can advance ahead of the committed state (e.g. when a
// concurrent write obtains its commitTS via clock.Next()). Using the HLC
// would give a startTS that does not reflect what was actually read,
// allowing the write-conflict check (latestTS > startTS) to miss
// conflicts when startTS == a concurrent transaction's commitTS.
func snapshotTS(_ *kv.HLC, st store.MVCCStore) uint64 {
	ts := uint64(0)
	if st != nil {
		ts = st.LastCommitTS()
	}
	if ts == 0 {
		ts = ^uint64(0)
	}
	return ts
}
