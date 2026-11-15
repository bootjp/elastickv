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

func TestTTLInlineCapabilityStaleConfiguration(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	f := newTTLInlineFSM(t, st)
	marker := ttlInlineCapabilityMarker{Version: 1, Index: 5, Voters: []uint64{1, 2}, Learners: []uint64{3}}
	payload := capabilityPayload(t, marker)
	native := raftengine.NativeConfiguration{Index: 5, Voters: []uint64{2, 1}, Learners: []uint64{3}}
	stale := native
	stale.Learners = []uint64{3, 4}
	require.Nil(t, applyCapability(t, f, 6, stale, payload))
	require.False(t, f.TTLInlineCapabilityActive())
	stale = native
	stale.Index = 6
	require.Nil(t, applyCapability(t, f, 7, stale, payload))
	require.False(t, f.TTLInlineCapabilityActive())
	_, err := st.GetAt(t.Context(), ttlInlineCapabilityKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Nil(t, applyCapability(t, f, 8, native, payload))
	require.False(t, f.TTLInlineCapabilityActive(), "matching marker is not independent all-member proof")
	_, err = st.GetAt(t.Context(), ttlInlineCapabilityKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestTTLInlineCapabilityApplyIsReplicaDeterministic(t *testing.T) {
	t.Parallel()
	native := raftengine.NativeConfiguration{Index: 5, Voters: []uint64{1, 2}, Learners: []uint64{3}}
	payload := capabilityPayload(t, ttlInlineCapabilityMarker{Version: 1, Index: 5, Voters: []uint64{2, 1}, Learners: []uint64{3}})
	for replica := 0; replica < 2; replica++ {
		st := store.NewMVCCStore()
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		f := newTTLInlineFSM(t, st)
		require.Nil(t, applyCapability(t, f, 6, native, payload))
		require.False(t, f.TTLInlineCapabilityActive())
		require.False(t, f.TTLInlineCapabilityRecorded())
		_, err := st.GetAt(t.Context(), ttlInlineCapabilityKey, ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

func TestTTLInlineCapabilityRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	f := newTTLInlineFSM(t, st)
	native := raftengine.NativeConfiguration{Index: 2, Voters: []uint64{1}, Learners: []uint64{2}}
	payload := capabilityPayload(t, ttlInlineCapabilityMarker{Version: 1, Index: 2, Voters: []uint64{1}, Learners: []uint64{2}})
	require.Nil(t, applyCapability(t, f, 3, native, payload))
	require.False(t, f.TTLInlineCapabilityActive())
	writer, ok := st.(store.ReplicatedControlStore)
	require.True(t, ok)
	_, err = writer.ApplyReplicatedControlState(t.Context(), ttlInlineCapabilityKey, payload[1:], 3)
	require.NoError(t, err)
	require.NoError(t, f.reloadTTLInlineCapability(t.Context()))
	require.True(t, f.TTLInlineCapabilityRecorded())
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
	require.True(t, replayed.TTLInlineCapabilityRecorded())
	require.NoError(t, replayed.InstallTTLInlineRecoveryProof(t.Context(), true,
		raftengine.NativeConfiguration{Index: 3, Voters: []uint64{1}, Learners: []uint64{2}}))
	require.False(t, replayed.TTLInlineCapabilityActive(), "a different recovered ConfChange index cannot reopen activation")
	require.True(t, replayed.TTLInlineCapabilityRecorded(), "configuration mismatch does not erase durable marker evidence")
	require.NoError(t, replayed.InstallTTLInlineRecoveryProof(t.Context(), false, native))
	require.False(t, replayed.TTLInlineCapabilityActive(), "an unfenced directory cannot reopen activation")
	require.NoError(t, replayed.InstallTTLInlineRecoveryProof(t.Context(), true, native))
	require.True(t, replayed.TTLInlineCapabilityActive(), "matching independent evidence reopens activation")
	require.True(t, replayed.TTLInlineFenceReady())
	replayed.InvalidateTTLInlineRecoveryProof()
	require.False(t, replayed.TTLInlineCapabilityActive())
	require.False(t, replayed.TTLInlineFenceReady())
	target, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, target.Close()) })
	restored := newTTLInlineFSM(t, target)
	require.NoError(t, restored.Restore(bytes.NewReader(image.Bytes())))
	require.False(t, restored.TTLInlineCapabilityActive(), "snapshot marker cannot reopen activation")
	require.True(t, restored.TTLInlineCapabilityRecorded(), "restored marker remains recorded without activation")
	require.NoError(t, restored.InstallTTLInlineRecoveryProof(t.Context(), true, native))
	require.True(t, restored.TTLInlineCapabilityActive())
	require.NoError(t, restored.Restore(bytes.NewReader(image.Bytes())))
	require.False(t, restored.TTLInlineCapabilityActive(), "a new snapshot invalidates the previous native proof")
	require.False(t, restored.TTLInlineFenceReady())
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
	require.False(t, f.TTLInlineCapabilityRecorded())
	require.ErrorIs(t, f.ttlInlineCapabilityLoadErr, ErrTTLInlineCapabilityMarker)
	require.ErrorIs(t, haltApplyOf(f.Apply([]byte{0xff})), ErrTTLInlineCapabilityMarker)
}

func TestTTLInlineCapabilityRecordedSurvivesMissingMarkerReload(t *testing.T) {
	t.Parallel()
	withMarker := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, withMarker.Close()) })
	writer, ok := withMarker.(store.ReplicatedControlStore)
	require.True(t, ok)
	_, err := writer.ApplyReplicatedControlState(t.Context(), ttlInlineCapabilityKey,
		[]byte(`{"version":1,"index":2,"voters":[1]}`), 3)
	require.NoError(t, err)
	f := newTTLInlineFSM(t, withMarker)
	require.True(t, f.TTLInlineCapabilityRecorded())
	require.False(t, f.TTLInlineCapabilityActive())

	withoutMarker := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, withoutMarker.Close()) })
	f.store = withoutMarker
	require.NoError(t, f.reloadTTLInlineCapability(t.Context()))
	require.True(t, f.TTLInlineCapabilityRecorded(), "reload must not reopen membership admission after a marker was observed")
	require.False(t, f.TTLInlineCapabilityActive())
}

func TestTTLInlineCapabilityDisabledHaltsOnRecoveryMarker(t *testing.T) {
	t.Parallel()
	for _, raw := range [][]byte{
		[]byte(`{"version":1,"index":2,"voters":[1]}`),
		[]byte(`not-json`),
	} {
		st := store.NewMVCCStore()
		writer, ok := st.(store.ReplicatedControlStore)
		require.True(t, ok)
		_, err := writer.ApplyReplicatedControlState(t.Context(), ttlInlineCapabilityKey, raw, 3)
		require.NoError(t, err)
		f, ok := NewKvFSMWithHLC(st, nil, WithTTLInlineCapabilityDisabled()).(*kvFSM)
		require.True(t, ok)
		require.False(t, f.TTLInlineCapabilityActive())
		require.False(t, f.TTLInlineCapabilityRecorded())
		require.ErrorIs(t, f.ttlInlineCapabilityLoadErr, ErrTTLInlineCapabilityDisabled)
		require.ErrorIs(t, haltApplyOf(f.Apply([]byte{0xff})), ErrTTLInlineCapabilityDisabled)
		require.NoError(t, st.Close())
	}
}

func TestTTLInlineCapabilityDisabledHaltsOnAppliedMarker(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	f, ok := NewKvFSMWithHLC(st, nil, WithTTLInlineCapabilityDisabled()).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, f.ttlInlineCapabilityLoadErr)
	native := raftengine.NativeConfiguration{Index: 2, Voters: []uint64{1}}
	for _, payload := range [][]byte{
		capabilityPayload(t, ttlInlineCapabilityMarker{Version: 1, Index: 2, Voters: []uint64{1}}),
		{raftEncodeTTLInlineCapability, 0xff},
	} {
		require.ErrorIs(t, haltApplyOf(applyCapability(t, f, 4, native, payload)), ErrTTLInlineCapabilityDisabled)
		require.False(t, f.TTLInlineCapabilityActive())
	}
	_, err := st.GetAt(t.Context(), ttlInlineCapabilityKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.False(t, f.TTLInlineCapabilityRecorded())
	f.ttlInlineCapability.Store(true)
	require.False(t, f.TTLInlineCapabilityActive(), "disabled admission must ignore a stale activation bit")
	require.ErrorIs(t, f.validateAbsenceRequest(absenceRequest([]byte("key"), true)), ErrTTLInlineCapabilityRequired)
}
