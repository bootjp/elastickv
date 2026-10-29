package kv

import (
	"context"
	"testing"
	"time"

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

func TestWatermarkAdvanceCodec(t *testing.T) {
	t.Parallel()
	ts, ok, err := DecodeWatermarkAdvance(EncodeWatermarkAdvance(42))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(42), ts)

	cases := []struct {
		name    string
		payload []byte
		ok      bool
	}{
		{name: "empty", payload: nil, ok: false},
		{name: "other opcode", payload: []byte{raftEncodeBackup, 1}, ok: false},
		{name: "short body", payload: []byte{raftEncodeWatermarkAdvance, 1, 2}, ok: true},
		{name: "zero", payload: EncodeWatermarkAdvance(0), ok: true},
		{name: "read-latest sentinel", payload: EncodeWatermarkAdvance(^uint64(0)), ok: true},
	}
	for _, tc := range cases {
		_, ok, err := DecodeWatermarkAdvance(tc.payload)
		require.Equal(t, tc.ok, ok, tc.name)
		if tc.ok {
			require.ErrorIs(t, err, ErrInvalidWatermarkAdvance, tc.name)
		} else {
			require.NoError(t, err, tc.name)
		}
	}
}

func TestValidateForwardedAdminPayloadAcceptsWatermarkAdvance(t *testing.T) {
	t.Parallel()
	require.NoError(t, ValidateForwardedAdminPayload(EncodeWatermarkAdvance(7)))
	require.ErrorIs(t, ValidateForwardedAdminPayload(EncodeWatermarkAdvance(0)), ErrInvalidWatermarkAdvance)
}

// The advance raises the watermark without writing a version, never lowers
// it, and makes the HLC issue above the advanced value.
func TestApplyWatermarkAdvance(t *testing.T) {
	t.Parallel()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	clock := NewHLC()
	fsm := NewKvFSMWithHLC(st, clock)

	ts := clock.Current() + 1<<hlcLogicalBits
	require.Nil(t, fsm.Apply(EncodeWatermarkAdvance(ts)))
	require.Equal(t, ts, st.LastCommitTS())
	require.Greater(t, clock.Next(), ts)

	// Replay and a lower advance leave the watermark where it is.
	require.Nil(t, fsm.Apply(EncodeWatermarkAdvance(ts)))
	require.Nil(t, fsm.Apply(EncodeWatermarkAdvance(ts-1)))
	require.Equal(t, ts, st.LastCommitTS())

	resp := fsm.Apply(EncodeWatermarkAdvance(0))
	respErr, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, respErr, ErrInvalidWatermarkAdvance)
	require.Equal(t, ts, st.LastCommitTS())
}

func TestReadWatermarkAdvanceAdmissible(t *testing.T) {
	t.Parallel()
	clock := NewHLC()
	now := clock.Current() >> hlcLogicalBits
	window := uint64(hlcPhysicalWindowMs)
	require.True(t, ReadWatermarkAdvanceAdmissible(clock, now<<hlcLogicalBits))
	require.True(t, ReadWatermarkAdvanceAdmissible(clock, (now+window)<<hlcLogicalBits))
	require.False(t, ReadWatermarkAdvanceAdmissible(clock, (now+window+1000)<<hlcLogicalBits))
	require.False(t, ReadWatermarkAdvanceAdmissible(nil, 1))

	// A raised physical ceiling widens the bound by the same amount.
	ahead := now + 10*window
	clock.SetPhysicalCeiling(clampUint64ToInt64(ahead))
	require.True(t, ReadWatermarkAdvanceAdmissible(clock, (ahead+window)<<hlcLogicalBits))
}

// An advance from a group whose clock runs ahead must not leave the HLC
// above its physical ceiling: fenced allocations floor at the ceiling and
// refuse anything above it, so observing the advance without raising the
// ceiling first would refuse every persistence timestamp on this replica
// until a lease renewal overtook it.
func TestApplyWatermarkAdvanceRaisesCeilingBeforeObserving(t *testing.T) {
	t.Parallel()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	clock := NewHLC()
	fsm := NewKvFSMWithHLC(st, clock)

	clock.SetPhysicalCeiling(time.Now().UnixMilli() + hlcPhysicalWindowMs)
	ts := (clock.Next()>>hlcLogicalBits+2*uint64(hlcPhysicalWindowMs))<<hlcLogicalBits | 5
	require.Nil(t, fsm.Apply(EncodeWatermarkAdvance(ts)))

	next, err := clock.NextFenced()
	require.NoError(t, err)
	require.Greater(t, next, ts)
}
