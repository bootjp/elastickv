package kv

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type restoreOriginStore struct {
	store.MVCCStore
	legacyCalls   int
	externalCalls int
	externalErr   error
}

func (s *restoreOriginStore) Restore(r io.Reader) error {
	s.legacyCalls++
	return s.MVCCStore.Restore(r)
}

func (s *restoreOriginStore) RestoreExternal(_ context.Context, r io.Reader) error {
	s.externalCalls++
	if s.externalErr != nil {
		return s.externalErr
	}
	return s.MVCCStore.Restore(r)
}

type legacyRestoreOnlyStore struct {
	store.MVCCStore
	calls int
}

func (s *legacyRestoreOnlyStore) Restore(r io.Reader) error {
	s.calls++
	return s.MVCCStore.Restore(r)
}

func restoreOriginPayload(t *testing.T) []byte {
	t.Helper()
	snapshot, err := store.NewMVCCStore().Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, snapshot.Close()) })
	fsmSnapshot := &kvFSMSnapshot{snapshot: snapshot, ceilingMs: 9000, useV2: true, cutover: 77}
	var buf bytes.Buffer
	_, err = fsmSnapshot.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

func TestRestoreWithContextDispatchesValidatedOrigin(t *testing.T) {
	payload := restoreOriginPayload(t)
	for _, external := range []bool{false, true} {
		t.Run(map[bool]string{false: "raft", true: "external"}[external], func(t *testing.T) {
			st := &restoreOriginStore{MVCCStore: store.NewMVCCStore()}
			fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
			require.True(t, ok)
			origin := raftengine.SnapshotRestoreRaft
			if external {
				origin = raftengine.SnapshotRestoreExternalRebase
			}
			require.NoError(t, fsm.RestoreWithContext(context.Background(), bytes.NewReader(payload), raftengine.SnapshotRestoreOptions{Origin: origin}))
			require.Equal(t, 1, st.legacyCalls+st.externalCalls)
			require.Equal(t, external, st.externalCalls == 1)
			require.Equal(t, int64(9000), fsm.hlc.PhysicalCeiling())
			require.Equal(t, uint64(77), fsm.RestoredCutover())
		})
	}
}

func TestExternalRestoreNeverFallsBackOrPublishesRejectedHeader(t *testing.T) {
	payload := restoreOriginPayload(t)
	st := &restoreOriginStore{MVCCStore: store.NewMVCCStore(), externalErr: errors.New("rejected migration state")}
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	fsm.restoredCutover = 33
	before := fsm.hlc.PhysicalCeiling()
	opts := raftengine.SnapshotRestoreOptions{Origin: raftengine.SnapshotRestoreExternalRebase}
	require.ErrorIs(t, fsm.RestoreWithContext(context.Background(), bytes.NewReader(payload), opts), st.externalErr)
	require.Equal(t, 1, st.externalCalls)
	require.Zero(t, st.legacyCalls)
	require.Equal(t, before, fsm.hlc.PhysicalCeiling())
	require.Equal(t, uint64(33), fsm.RestoredCutover())

	legacy := &legacyRestoreOnlyStore{MVCCStore: store.NewMVCCStore()}
	fsm, ok = NewKvFSMWithHLC(legacy, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.ErrorIs(t, fsm.RestoreWithContext(context.Background(), bytes.NewReader(payload), opts), ErrExternalSnapshotRestoreUnsupported)
	require.Zero(t, legacy.calls)
}

func TestRestoreWithContextRejectsCanceledOrUnknownOrigin(t *testing.T) {
	st := &restoreOriginStore{MVCCStore: store.NewMVCCStore()}
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, fsm.RestoreWithContext(ctx, bytes.NewReader(nil), raftengine.SnapshotRestoreOptions{}), context.Canceled)
	require.Error(t, fsm.RestoreWithContext(context.Background(), bytes.NewReader(nil), raftengine.SnapshotRestoreOptions{Origin: 255}))
	require.Zero(t, st.legacyCalls+st.externalCalls)
}
