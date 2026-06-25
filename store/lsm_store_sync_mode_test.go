package store

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyMutationsRaft_NoSyncFunctionalEquivalence verifies that the
// NoSync FSM-apply commit mode produces the same observable state as the
// default Sync mode for a well-behaved (no-crash) workload. This is
// the "happy-path" contract: operators turning on NoSync must not see
// correctness regressions in steady-state operation; the durability
// trade-off only surfaces on crash.
//
// The crash-recovery half of the contract (raft-log replay re-applying
// any entries lost from Pebble after fsync was skipped) is handled by
// kv/fsm.applyCommitWithIdempotencyFallback, which treats
// already-applied keys (LatestCommitTS >= commitTS) as idempotent
// retries. At this layer we do not exercise the raft engine; we only
// verify that switching write options does not change the visible
// store state.
//
// NOTE: tests exercise ApplyMutationsRaft — the raft-apply entry point —
// because ApplyMutations is unconditionally pebble.Sync and therefore
// independent of ELASTICKV_FSM_SYNC_MODE.
func TestApplyMutationsRaft_NoSyncFunctionalEquivalence(t *testing.T) {
	dir := t.TempDir()
	s := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)
	defer s.Close()

	ctx := context.Background()

	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v1")},
		{Op: OpTypePut, Key: []byte("k2"), Value: []byte("v2")},
	}
	require.NoError(t, s.ApplyMutationsRaft(ctx, mutations, nil, 0, 10))

	val, err := s.GetAt(ctx, []byte("k1"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	val, err = s.GetAt(ctx, []byte("k2"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

// TestApplyMutations_NoSyncReopenVisibility verifies that after a
// clean Close and reopen of the Pebble DB in NoSync mode, writes that
// completed before Close remain visible. A clean Close drives a final
// Pebble flush, so this exercises the common graceful-shutdown path:
// NoSync only impacts crash recovery of un-fsynced tail WAL entries,
// not orderly shutdowns.
//
// The deliberately-unfsynced crash case (kill -9 mid-apply, where
// Pebble's WAL tail is lost and must be reconstructed from raft) is
// inherently an OS-level scenario; it lives in the Jepsen / integration
// suite (see JEPSEN_TODO.md) rather than as a Go unit test, because
// simulating an fsync loss without actually yanking power requires a
// custom VFS shim that is not currently wired into Pebble at this
// layer.
func TestApplyMutationsRaft_NoSyncReopenVisibility(t *testing.T) {
	dir := t.TempDir()
	s := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)

	ctx := context.Background()
	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("keep"), Value: []byte("after-reopen")},
	}
	require.NoError(t, s.ApplyMutationsRaft(ctx, mutations, nil, 0, 10))
	require.NoError(t, s.Close())

	reopened := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)
	defer reopened.Close()

	val, err := reopened.GetAt(ctx, []byte("keep"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("after-reopen"), val)
}

// TestDirectApplyWriteOpts_AlwaysSync verifies the correctness contract
// introduced by the ApplyMutations / ApplyMutationsRaft split: the
// direct (non-raft) commit path must always use pebble.Sync so callers
// that bypass raft (catalog bootstrap via CatalogStore.Save,
// EnsureCatalogSnapshot, admin snapshots, migrations) are never
// affected by ELASTICKV_FSM_SYNC_MODE=nosync. The raft-apply path
// (ApplyMutationsRaft / DeletePrefixAtRaft) observes the knob as
// before; the direct path must not.
func TestDirectApplyWriteOpts_AlwaysSync(t *testing.T) {
	t.Run("nosync-configured store still syncs direct ApplyMutations", func(t *testing.T) {
		ps := newPebbleStoreWithFSMApplyWriteOptsForTest(t, t.TempDir(), pebble.NoSync, fsmSyncModeNoSync)
		defer ps.Close()

		// Raft-apply path observes the NoSync opt-in.
		require.Same(t, pebble.NoSync, ps.raftApplyWriteOpts(),
			"raft-apply path must observe ELASTICKV_FSM_SYNC_MODE=nosync")

		// Direct (non-raft) path must remain Sync regardless of the
		// FSM-apply mode. This is the correctness guarantee: callers
		// without a raft-log durability backstop must never have
		// their fsync elided by the FSM-apply knob.
		require.Same(t, pebble.Sync, ps.directApplyWriteOpts(),
			"direct commit path must be pebble.Sync regardless of FSM-apply mode")
	})

	t.Run("sync-configured store: both paths use Sync", func(t *testing.T) {
		ps := newPebbleStoreWithFSMApplyWriteOptsForTest(t, t.TempDir(), pebble.Sync, fsmSyncModeSync)
		defer ps.Close()

		require.Same(t, pebble.Sync, ps.raftApplyWriteOpts())
		require.Same(t, pebble.Sync, ps.directApplyWriteOpts())
	})
}

// TestDirectApplyMutations_NoSyncConfigured_StillWritesDurably is the
// functional twin of TestDirectApplyWriteOpts_AlwaysSync: it exercises
// the public ApplyMutations and DeletePrefixAt entry points with a
// NoSync-configured FSM-apply mode and asserts the data is visible
// after a clean reopen. This does not assert fsync at the syscall
// level (that requires a VFS shim; see JEPSEN_TODO.md), but it
// guarantees the public direct-path API still behaves correctly
// under the split.
func TestDirectApplyMutations_NoSyncConfigured_StillWritesDurably(t *testing.T) {
	dir := t.TempDir()
	s := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)

	ctx := context.Background()
	// Direct ApplyMutations (the path CatalogStore.Save takes).
	require.NoError(t, s.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("direct-key"), Value: []byte("direct-val")},
	}, nil, 0, 5))
	// Direct DeletePrefixAt.
	require.NoError(t, s.DeletePrefixAt(ctx, []byte("nothing-here"), nil, 6))

	require.NoError(t, s.Close())

	reopened := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)
	defer reopened.Close()

	val, err := reopened.GetAt(ctx, []byte("direct-key"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("direct-val"), val)
}
