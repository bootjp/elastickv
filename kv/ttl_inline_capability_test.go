package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func capabilityPayload(t *testing.T, marker ttlInlineCapabilityMarker) []byte {
	t.Helper()
	raw, err := json.Marshal(marker)
	require.NoError(t, err)
	return append([]byte{raftEncodeTTLInlineCapability}, raw...)
}

func applyCapability(t *testing.T, f *kvFSM, index uint64, native raftengine.NativeConfiguration, payload []byte) any {
	t.Helper()
	f.SetApplyIndex(index)
	f.SetApplyNativeConfiguration(native)
	return f.Apply(payload)
}

func newTTLInlineFSM(t *testing.T, st store.MVCCStore) *kvFSM {
	t.Helper()
	f, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
	require.True(t, ok)
	return f
}

func TestTTLInlineCapabilityStaleConfigurationAndMissingProof(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	f := newTTLInlineFSM(t, st)
	marker := ttlInlineCapabilityMarker{Version: 1, Index: 5, Voters: []uint64{1, 2}, Learners: []uint64{3}}
	payload := capabilityPayload(t, marker)
	native := raftengine.NativeConfiguration{Index: 5, Voters: []uint64{2, 1}, Learners: []uint64{3}}
	require.Nil(t, applyCapability(t, f, 6, native, payload))
	require.False(t, f.TTLInlineCapabilityActive(), "command alone is not member proof")
	f.ttlInlineMemberProof = func(raftengine.NativeConfiguration) bool { return true }
	stale := native
	stale.Learners = []uint64{3, 4}
	require.Nil(t, applyCapability(t, f, 7, stale, payload))
	require.False(t, f.TTLInlineCapabilityActive())
	stale = native
	stale.Index = 6
	require.Nil(t, applyCapability(t, f, 8, stale, payload))
	require.False(t, f.TTLInlineCapabilityActive())
	_, err := st.GetAt(t.Context(), ttlInlineCapabilityKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Nil(t, applyCapability(t, f, 9, native, payload))
	require.True(t, f.TTLInlineCapabilityActive())
}

func TestTTLInlineCapabilityRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	f := newTTLInlineFSM(t, st)
	f.ttlInlineMemberProof = func(raftengine.NativeConfiguration) bool { return true }
	native := raftengine.NativeConfiguration{Index: 2, Voters: []uint64{1}, Learners: []uint64{2}}
	payload := capabilityPayload(t, ttlInlineCapabilityMarker{Version: 1, Index: 2, Voters: []uint64{1}, Learners: []uint64{2}})
	require.Nil(t, applyCapability(t, f, 3, native, payload))
	require.True(t, f.TTLInlineCapabilityActive())
	snap, err := f.Snapshot()
	require.NoError(t, err)
	var image bytes.Buffer
	_, err = snap.WriteTo(&image)
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	require.NoError(t, st.Close())
	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	replayed := newTTLInlineFSM(t, reopened)
	require.False(t, replayed.TTLInlineCapabilityActive(), "persisted marker is not local-fence or recovered-ConfState proof")
	replayed.ttlInlineRecoveryProof = func() (bool, raftengine.NativeConfiguration) {
		return true, raftengine.NativeConfiguration{Index: 3, Voters: []uint64{1}, Learners: []uint64{2}}
	}
	require.NoError(t, replayed.reloadTTLInlineCapability(t.Context()))
	require.False(t, replayed.TTLInlineCapabilityActive(), "a different recovered ConfChange index cannot reopen activation")
	replayed.ttlInlineRecoveryProof = func() (bool, raftengine.NativeConfiguration) { return false, native }
	require.NoError(t, replayed.reloadTTLInlineCapability(t.Context()))
	require.False(t, replayed.TTLInlineCapabilityActive(), "an unfenced directory cannot reopen activation")
	replayed.ttlInlineRecoveryProof = func() (bool, raftengine.NativeConfiguration) { return true, native }
	require.NoError(t, replayed.reloadTTLInlineCapability(t.Context()))
	require.True(t, replayed.TTLInlineCapabilityActive(), "matching independent evidence reopens activation")
	target, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, target.Close()) })
	restored := newTTLInlineFSM(t, target)
	require.NoError(t, restored.Restore(bytes.NewReader(image.Bytes())))
	require.False(t, restored.TTLInlineCapabilityActive(), "snapshot marker cannot reopen activation")
}

func TestTTLInlineCapabilityUnknownRecordFailsClosed(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	writer, ok := st.(store.ReplicatedControlStore)
	require.True(t, ok)
	_, err := writer.ApplyReplicatedControlState(context.Background(), ttlInlineCapabilityKey,
		[]byte(`{"version":2,"index":1,"voters":[1]}`), 1)
	require.NoError(t, err)
	f := newTTLInlineFSM(t, st)
	require.False(t, f.TTLInlineCapabilityActive())
	require.ErrorIs(t, f.ttlInlineCapabilityLoadErr, ErrTTLInlineCapabilityMarker)
	require.ErrorIs(t, haltApplyOf(f.Apply([]byte{0xff})), ErrTTLInlineCapabilityMarker)
}
