package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// twoGroupReadFixture is two single-node Raft groups behind one
// ShardStore: keys in [a, b) live on group 1, keys from b on group 2.
type twoGroupReadFixture struct {
	shards *ShardStore
	groups map[uint64]*ShardGroup
	stores map[uint64]store.MVCCStore
}

func newTwoGroupReadFixture(t *testing.T) *twoGroupReadFixture {
	t.Helper()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte("a"), End: []byte("b"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("b"), End: nil, GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	f := &twoGroupReadFixture{groups: map[uint64]*ShardGroup{}, stores: map[uint64]store.MVCCStore{}}
	for _, gid := range []uint64{1, 2} {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { _ = st.Close() })
		r, stop := newSingleRaft(t, "g"+string(rune('0'+gid)), NewKvFSMWithHLC(st, NewHLC()))
		t.Cleanup(stop)
		f.groups[gid] = &ShardGroup{Engine: r, Store: st, Txn: NewLeaderProxyWithEngine(r)}
		f.stores[gid] = st
	}
	f.shards = NewShardStore(engine, f.groups)
	return f
}

func (f *twoGroupReadFixture) onePhase(t *testing.T, gid, start, commit uint64, key, val string, readKeys ...string) error {
	t.Helper()
	var rk [][]byte
	for _, k := range readKeys {
		rk = append(rk, []byte(k))
	}
	req := onePhaseTxnRequestWithPrevCommit(start, commit, 0, []byte(key),
		[]*Elem[OP]{{Op: Put, Key: []byte(key), Value: []byte(val)}}, rk, 0, nil)
	_, err := f.groups[gid].Txn.Commit(context.Background(), []*pb.Request{req})
	return err
}

// A one-phase write whose commit timestamp was allocated before a newer
// commit on another group, and which applies on a lagging group after a
// reader took the cross-group snapshot, must not land below that snapshot
// (G0 across groups, serializable isolation audit "Multi-group
// deployments"). The read at s raises the lagging group's watermark to s,
// so the late write is fenced and the snapshot stays stable.
func TestShardStoreSnapshotReadAdvancesLaggingGroupWatermark(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newTwoGroupReadFixture(t)

	require.NoError(t, f.onePhase(t, 2, 5, 10, "b-k", "old"))
	// W_A at c2 = 30 applies first on group 1; W_B, allocated c1 = 20
	// earlier, is still in flight for group 2.
	require.NoError(t, f.onePhase(t, 1, 10, 30, "a-k", "x"))

	s := f.shards.LastCommitTS()
	require.Equal(t, uint64(30), s)
	v1, err := f.shards.GetAt(ctx, []byte("b-k"), s)
	require.NoError(t, err)
	require.Equal(t, "old", string(v1))
	require.GreaterOrEqual(t, f.stores[2].LastCommitTS(), s,
		"the read at s must raise the lagging group's watermark to s")

	// W_B lands now: c1 = 20 is at or below the advanced watermark, so the
	// commit-ts fence rejects it and the writer retries above s.
	errWB := f.onePhase(t, 2, 10, 20, "b-k", "new")
	require.ErrorIs(t, errWB, store.ErrWriteConflict)

	v2, err := f.shards.GetAt(ctx, []byte("b-k"), s)
	require.NoError(t, err)
	require.Equal(t, "old", string(v2), "the snapshot at s changed after the read")

	// T commits f(read) with b-k in its read set; W_B's write was never
	// acknowledged, so nothing is lost.
	require.NoError(t, f.onePhase(t, 2, s, 40, "b-k", "old+1", "b-k"))
}

// The same advance covers range reads.
func TestShardStoreSnapshotScanAdvancesLaggingGroupWatermark(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newTwoGroupReadFixture(t)

	require.NoError(t, f.onePhase(t, 2, 5, 10, "b-k", "old"))
	require.NoError(t, f.onePhase(t, 1, 10, 30, "a-k", "x"))

	s := f.shards.LastCommitTS()
	kvs, err := f.shards.ScanAt(ctx, []byte("b"), nil, 10, s)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.GreaterOrEqual(t, f.stores[2].LastCommitTS(), s)
	require.ErrorIs(t, f.onePhase(t, 2, 10, 20, "b-k", "new"), store.ErrWriteConflict)
}

// A read timestamp above every watermark this node has applied did not
// come from a cluster snapshot (a client-chosen Ts, or the empty-store
// sentinel), so it is served as before and advances nothing.
func TestShardStoreReadAboveLocalWatermarksDoesNotAdvance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newTwoGroupReadFixture(t)

	require.NoError(t, f.onePhase(t, 2, 5, 10, "b-k", "old"))
	require.NoError(t, f.onePhase(t, 1, 10, 30, "a-k", "x"))

	for _, ts := range []uint64{31, 1 << 40, ^uint64(0)} {
		_, err := f.shards.GetAt(ctx, []byte("b-k"), ts)
		require.NoError(t, err)
		require.Equal(t, uint64(10), f.stores[2].LastCommitTS(), "ts %d advanced the watermark", ts)
	}
}
