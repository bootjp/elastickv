package store

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyMutations_NoSyncFunctionalEquivalence verifies that the
// NoSync FSM commit mode produces the same observable state as the
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
func TestApplyMutations_NoSyncFunctionalEquivalence(t *testing.T) {
	dir := t.TempDir()
	s := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)
	defer s.Close()

	ctx := context.Background()

	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k1"), Value: []byte("v1")},
		{Op: OpTypePut, Key: []byte("k2"), Value: []byte("v2")},
	}
	require.NoError(t, s.ApplyMutations(ctx, mutations, nil, 0, 10))

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
func TestApplyMutations_NoSyncReopenVisibility(t *testing.T) {
	dir := t.TempDir()
	s := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)

	ctx := context.Background()
	mutations := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("keep"), Value: []byte("after-reopen")},
	}
	require.NoError(t, s.ApplyMutations(ctx, mutations, nil, 0, 10))
	require.NoError(t, s.Close())

	reopened := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)
	defer reopened.Close()

	val, err := reopened.GetAt(ctx, []byte("keep"), 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("after-reopen"), val)
}
