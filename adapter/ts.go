package adapter

import (
	"context"

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

// globalSnapshotTS is like snapshotTS but uses GlobalLastCommitTS when the
// store supports it (i.e. LeaderRoutedStore). On the leader this is
// equivalent to LastCommitTS(); on a follower it prefers querying the
// leader via RPC for fresher alignment. If leader lookup or the RPC fails
// (nil store, empty leader address, connection error, RPC error, or
// resp.Ts == 0), it falls back to local LastCommitTS() — see
// kv/leader_routed_store.go GlobalLastCommitTS. On the leader path the
// fresher-than-local guarantee is exact; on the follower it holds only
// when the RPC succeeds and the leader has applied the relevant log
// entry, so this best-effort path narrows but does not eliminate stale
// reads triggered by writes that were forwarded from this follower to the
// leader and have not yet been applied locally.
func globalSnapshotTS(ctx context.Context, clk *kv.HLC, st store.MVCCStore) uint64 {
	type globalTS interface {
		GlobalLastCommitTS(ctx context.Context) uint64
	}
	if g, ok := st.(globalTS); ok {
		if ts := g.GlobalLastCommitTS(ctx); ts > 0 {
			return ts
		}
	}
	return snapshotTS(clk, st)
}
