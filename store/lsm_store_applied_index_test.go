package store

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// pebbleStoreApplied is the local narrowed view of pebbleStore that
// the applied-index tests reach for. We keep the type assertion
// behind a helper so renames of the unexported struct don't cascade
// into every test.
func pebbleStoreApplied(t *testing.T, st MVCCStore) *pebbleStore {
	t.Helper()
	ps, ok := st.(*pebbleStore)
	require.True(t, ok, "expected *pebbleStore, got %T", st)
	return ps
}

// newApplyIndexPebbleStore creates a fresh pebbleStore in a temp dir
// for one applied-index test. Caller closes via t.Cleanup.
func newApplyIndexPebbleStore(t *testing.T) MVCCStore {
	t.Helper()
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, st.Close())
		require.NoError(t, os.RemoveAll(dir))
	})
	return st
}

// TestLastAppliedIndex_MissingMetaKey covers the "(0, false, nil)"
// branch — the strictly-additive fallback that lets the cold-start
// skip gate degrade to full restore for fsm.db files that have not
// yet been mutated through a raft-Apply (pre-upgrade fsm.db, freshly
// restored store).
func TestLastAppliedIndex_MissingMetaKey(t *testing.T) {
	st := newApplyIndexPebbleStore(t)
	ps := pebbleStoreApplied(t, st)

	idx, present, err := ps.LastAppliedIndex()
	require.NoError(t, err)
	require.False(t, present, "fresh store must report (false) for missing meta key")
	require.Equal(t, uint64(0), idx, "missing key must surface as 0")
}

// TestSetDurableAppliedIndex_RoundTrip pins the meta key via the
// snapshot-persist writer seam and verifies LastAppliedIndex returns
// the same value with present=true. Confirms the
// pebble.Sync-unconditionally commit and the little-endian encoding
// round-trip.
func TestSetDurableAppliedIndex_RoundTrip(t *testing.T) {
	st := newApplyIndexPebbleStore(t)
	ps := pebbleStoreApplied(t, st)

	const wantIdx uint64 = 0xDEAD_BEEF_CAFE_F00D
	require.NoError(t, ps.SetDurableAppliedIndex(wantIdx))

	got, present, err := ps.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present, "after SetDurableAppliedIndex the key must be present")
	require.Equal(t, wantIdx, got, "round-trip must preserve the full uint64")
}

// TestApplyMutationsRaftAt_BundlesMetaAppliedIndex verifies that a
// raft-Apply data mutation bundles metaAppliedIndex in the SAME
// pebble.Batch as the data (and metaLastCommitTS). After the apply,
// LastAppliedIndex must reflect the entry index passed in.
func TestApplyMutationsRaftAt_BundlesMetaAppliedIndex(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	ps := pebbleStoreApplied(t, st)

	const entryIdx uint64 = 42
	muts := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v1")},
		{Op: OpTypePut, Key: []byte("k2"), Value: []byte("v2")},
	}
	const ts uint64 = 100
	require.NoError(t, ps.ApplyMutationsRaftAt(ctx, muts, nil, ts, ts, entryIdx))

	got, present, err := ps.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present, "ApplyMutationsRaftAt must persist metaAppliedIndex")
	require.Equal(t, entryIdx, got)

	// Confirm the data also landed (sanity check the bundling did not
	// drop the data path).
	val, err := ps.GetAt(ctx, []byte("k1"), ts)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

// TestApplyMutationsRaftAt_ZeroIndexLeavesMetaKey covers the
// appliedIndex==0 escape hatch — callers without a raft entry index
// (test fakes, legacy ApplyMutationsRaft) MUST NOT bump the meta key.
func TestApplyMutationsRaftAt_ZeroIndexLeavesMetaKey(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	ps := pebbleStoreApplied(t, st)

	// Seed metaAppliedIndex via SetDurableAppliedIndex.
	const seeded uint64 = 7
	require.NoError(t, ps.SetDurableAppliedIndex(seeded))

	muts := []*KVPairMutation{{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v1")}}
	// appliedIndex=0 → leaf must NOT touch metaAppliedIndex.
	require.NoError(t, ps.ApplyMutationsRaftAt(ctx, muts, nil, 100, 100, 0))

	got, present, err := ps.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, seeded, got, "appliedIndex=0 must leave the meta key at its previous value")
}

// TestDeletePrefixAtRaftAt_BundlesMetaAppliedIndex is the analogous
// round-trip for the DEL_PREFIX leaf (handleDelPrefix builds its own
// pebble.Batch separate from applyMutationsWithOpts). Without this
// hook, DEL_PREFIX entries silently leave LastAppliedIndex behind.
func TestDeletePrefixAtRaftAt_BundlesMetaAppliedIndex(t *testing.T) {
	ctx := context.Background()
	st := newApplyIndexPebbleStore(t)
	ps := pebbleStoreApplied(t, st)

	// Pre-populate so the DEL_PREFIX actually iterates over something.
	const seedTS uint64 = 50
	require.NoError(t, ps.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("p/k1"), Value: []byte("v")},
		{Op: OpTypePut, Key: []byte("p/k2"), Value: []byte("v")},
	}, nil, seedTS, seedTS))

	const entryIdx uint64 = 99
	require.NoError(t, ps.DeletePrefixAtRaftAt(ctx, []byte("p/"), nil, 200, entryIdx))

	got, present, err := ps.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present, "DeletePrefixAtRaftAt must persist metaAppliedIndex")
	require.Equal(t, entryIdx, got)
}

// TestSetDurableAppliedIndex_UsesPebbleSync exercises the
// nosync-mode independence claim — even when ELASTICKV_FSM_SYNC_MODE
// is nosync, the checkpoint must use pebble.Sync. We can't directly
// observe the sync option from a black-box test, so we settle for
// "the value persists across a store close + reopen". A NoSync write
// would not necessarily survive (Pebble's WAL flush is opportunistic),
// so a deterministic round-trip after Close()+Open() is a useful
// regression guard for the sync-mode flip.
func TestSetDurableAppliedIndex_UsesPebbleSync(t *testing.T) {
	t.Setenv("ELASTICKV_FSM_SYNC_MODE", "nosync")
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)

	ps := pebbleStoreApplied(t, st)
	const idx uint64 = 12345
	require.NoError(t, ps.SetDurableAppliedIndex(idx))
	require.NoError(t, st.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	got, present, err := pebbleStoreApplied(t, reopened).LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present, "after Close+Open metaAppliedIndex must persist (pebble.Sync forced regardless of nosync)")
	require.Equal(t, idx, got)
}

// TestLastAppliedIndex_CorruptValue exercises the "len(val) <
// timestampSize" fallback — a truncated meta key surfaces as missing
// (NOT as an error) so the caller falls back to full restore rather
// than crashing the cold start.
func TestLastAppliedIndex_CorruptValue(t *testing.T) {
	st := newApplyIndexPebbleStore(t)
	ps := pebbleStoreApplied(t, st)

	// Write a too-short payload directly under the meta key to simulate
	// a partial write. Use s.db.Set so we bypass setPebbleUint64InBatch.
	require.NoError(t, ps.db.Set(metaAppliedIndexBytes, []byte{0x01, 0x02}, pebble.Sync))

	got, present, err := ps.LastAppliedIndex()
	require.NoError(t, err, "corrupt value must NOT propagate as an error")
	require.False(t, present, "truncated meta key must collapse to missing")
	require.Equal(t, uint64(0), got)
}
