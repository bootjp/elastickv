package kv

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func restoreOriginRealStore(t *testing.T, format string) store.MVCCStore {
	t.Helper()
	if format == "mvcc" {
		return store.NewMVCCStore()
	}
	st, err := store.NewPebbleStore(t.TempDir(), store.WithSSTIngestSnapshots(format == "sst"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func restoreOriginRealPayload(t *testing.T, format string, bearing bool) []byte {
	t.Helper()
	st := restoreOriginRealStore(t, format)
	key := store.MigrationConfigurationIndexKey()
	require.NoError(t, st.PutAt(t.Context(), key, binary.BigEndian.AppendUint64(nil, 11), 10, 0))
	require.NoError(t, st.PutAt(t.Context(), key, binary.BigEndian.AppendUint64(nil, 22), 20, 0))
	require.NoError(t, st.PutAt(t.Context(), []byte("ordinary"), []byte("literal !migstage|ownership"), 30, 0))
	if bearing {
		writer, ok := st.(store.MigrationTargetReadinessWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadiness(t.Context(), store.TargetStagedReadinessState{
			JobID: 7, MigrationJobID: 7, Armed: true, ExpectedCutoverVersion: 10,
			MinWriteTSExclusive: 100, RouteStart: []byte("a"), RouteEnd: []byte("z"),
		}))
	}
	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	fsmSnapshot := &kvFSMSnapshot{snapshot: snapshot, ceilingMs: 9000, useV2: true, cutover: 77}
	var buf bytes.Buffer
	_, err = fsmSnapshot.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	return buf.Bytes()
}

func TestExternalRestoreRealStoreLineage(t *testing.T) {
	for _, format := range []string{"mvcc", "native", "sst"} {
		for _, bearing := range []bool{false, true} {
			name := format + map[bool]string{false: "/epoch-only", true: "/migration-bearing"}[bearing]
			t.Run(name, func(t *testing.T) {
				raw := restoreOriginRealPayload(t, format, bearing)
				verifyKVExternalRestoreLineage(t, format, bearing, raw)
				tso := NewTSOStateMachine(nil)
				require.Nil(t, tso.Apply(marshalTSOAllocationFloor(50)))
				err := tso.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{
					Origin: raftengine.SnapshotRestoreExternalRebase,
				})
				if bearing {
					require.ErrorIs(t, err, store.ErrExternalSnapshotMigrationState)
					require.Equal(t, uint64(50), tso.AllocationFloor())
					require.Zero(t, tso.ceilingMs.Load())
				} else {
					require.NoError(t, err)
					require.Equal(t, int64(9000), tso.ceilingMs.Load())
				}
			})
		}
	}
}

func verifyKVExternalRestoreLineage(t *testing.T, format string, bearing bool, raw []byte) {
	t.Helper()
	st := restoreOriginRealStore(t, format)
	require.NoError(t, st.PutAt(t.Context(), store.MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 5), 5, 0))
	require.NoError(t, st.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 10, 0))
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	require.Equal(t, uint64(5), fsm.AppliedConfigurationIndex())
	err := fsm.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{
		Origin: raftengine.SnapshotRestoreExternalRebase,
	})
	if bearing {
		require.ErrorIs(t, err, store.ErrExternalSnapshotMigrationState)
		require.Equal(t, uint64(5), fsm.AppliedConfigurationIndex())
		require.Zero(t, fsm.hlc.PhysicalCeiling())
		require.Zero(t, fsm.RestoredCutover())
		value, readErr := st.GetAt(t.Context(), []byte("sentinel"), 10)
		require.NoError(t, readErr)
		require.Equal(t, []byte("unchanged"), value)
	} else {
		require.NoError(t, err)
		require.Zero(t, fsm.AppliedConfigurationIndex(), "a copied epoch is not a new configuration proof")
		value, readErr := st.GetAt(t.Context(), []byte("ordinary"), 30)
		require.NoError(t, readErr)
		require.Equal(t, []byte("literal !migstage|ownership"), value)
	}
	require.NoError(t, fsm.Restore(bytes.NewReader(raw)), "same-lineage restore retains its control evidence")
	require.Equal(t, uint64(22), fsm.AppliedConfigurationIndex())
	require.Equal(t, uint64(77), fsm.RestoredCutover())
}
