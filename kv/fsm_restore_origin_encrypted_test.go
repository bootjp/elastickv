package kv

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func encryptedRestoreStore(t *testing.T, cipher *encryption.Cipher, sst bool, nonceEpoch uint16) store.MVCCStore {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir(), store.WithSSTIngestSnapshots(sst),
		store.WithEncryption(cipher, encryption.NewDeterministicNonceFactory(1, nonceEpoch), func() (uint32, bool) { return 1, true }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func encryptedRestoreSnapshot(t *testing.T, st store.MVCCStore) []byte {
	t.Helper()
	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	var body bytes.Buffer
	_, err = snapshot.WriteTo(&body)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	return body.Bytes()
}

func encryptedRestorePayload(t *testing.T, st store.MVCCStore, mode string) []byte {
	t.Helper()
	require.NoError(t, st.PutAt(t.Context(), []byte("ordinary"), []byte("encrypted !migwrite| payload"), 30, 0))
	if mode == "epoch-only" {
		for ts, epoch := range map[uint64]uint64{10: 11, 20: 22} {
			require.NoError(t, st.PutAt(t.Context(), store.MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, epoch), ts, 0))
		}
	}
	if mode == "migration-bearing" {
		writer, ok := st.(store.MigrationTargetReadinessWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadiness(t.Context(), store.TargetStagedReadinessState{
			JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true,
			SourceWriteFence: true, SourceReadFence: true,
			ExpectedCutoverVersion: 10, MinWriteTSExclusive: 100,
			RouteStart: []byte("a"), RouteEnd: []byte("z"), RetentionPinTS: 1,
		}))
	}
	return encryptedRestoreSnapshot(t, st)
}

func TestExternalRestoreEncryptedKVAndTSOLineage(t *testing.T) {
	for _, format := range []string{"native", "sst"} {
		for _, mode := range []string{"proof-free", "epoch-only", "migration-bearing"} {
			t.Run(format+"/"+mode, func(t *testing.T) {
				keys := encryption.NewKeystore()
				require.NoError(t, keys.Set(1, bytes.Repeat([]byte{0x37}, encryption.KeySize)))
				cipher, err := encryption.NewCipher(keys)
				require.NoError(t, err)
				source := encryptedRestoreStore(t, cipher, format == "sst", 1)
				body := encryptedRestorePayload(t, source, mode)
				assertEncryptedRestoreNeedsCipher(t, body)
				for _, header := range []bool{false, true} {
					t.Run(map[bool]string{false: "headerless", true: "header"}[header], func(t *testing.T) {
						raw := bytes.Clone(body)
						if header {
							prefix := make([]byte, hlcSnapshotHeaderLen)
							copy(prefix, hlcSnapshotMagic[:])
							binary.BigEndian.PutUint64(prefix[len(hlcSnapshotMagic):], 9000)
							raw = bytes.Join([][]byte{prefix, body}, nil)
						}
						destination := encryptedRestoreStore(t, cipher, format == "sst", 2)
						verifyEncryptedKVRestore(t, destination, mode, header, raw)
						verifyEncryptedTSORestore(t, mode, header, raw)
					})
				}
			})
		}
	}
}

// A plain store can install the raw snapshot but cannot read its ordinary
// values. TSO validation must therefore work without decrypting user payloads.
func assertEncryptedRestoreNeedsCipher(t *testing.T, body []byte) {
	t.Helper()
	plain, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer func() { require.NoError(t, plain.Close()) }()
	require.NoError(t, plain.Restore(bytes.NewReader(body)))
	value, err := plain.GetAt(t.Context(), []byte("ordinary"), 30)
	require.ErrorContains(t, err, "encrypted value present but no cipher configured")
	require.Empty(t, value)
}

func verifyEncryptedKVRestore(t *testing.T, st store.MVCCStore, mode string, header bool, raw []byte) {
	t.Helper()
	require.NoError(t, st.PutAt(t.Context(), store.MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 5), 5, 0))
	require.NoError(t, st.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 10, 0))
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)
	fsm.restoredCutover = 33
	before := encryptedRestoreSnapshot(t, st)
	err := fsm.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{
		Origin: raftengine.SnapshotRestoreExternalRebase,
	})
	if mode == "migration-bearing" {
		require.ErrorIs(t, err, store.ErrExternalSnapshotMigrationState)
		require.Equal(t, before, encryptedRestoreSnapshot(t, st))
		require.Equal(t, uint64(5), fsm.AppliedConfigurationIndex())
		require.Zero(t, fsm.hlc.PhysicalCeiling())
		require.Equal(t, uint64(33), fsm.RestoredCutover())
		value, readErr := st.GetAt(t.Context(), []byte("sentinel"), 10)
		require.NoError(t, readErr)
		require.Equal(t, []byte("unchanged"), value)
		return
	}
	require.NoError(t, err)
	value, err := st.GetAt(t.Context(), []byte("ordinary"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("encrypted !migwrite| payload"), value)
	for _, ts := range []uint64{10, 20, ^uint64(0)} {
		_, err = st.GetAt(t.Context(), store.MigrationConfigurationIndexKey(), ts)
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
	require.Zero(t, fsm.AppliedConfigurationIndex())
	require.Zero(t, fsm.RestoredCutover())
	if header {
		require.Equal(t, int64(9000), fsm.hlc.PhysicalCeiling())
	}
}

func verifyEncryptedTSORestore(t *testing.T, mode string, header bool, raw []byte) {
	t.Helper()
	fsm := NewTSOStateMachine(nil)
	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(50)))
	before := encryptedTSOState(t, fsm)
	err := fsm.RestoreWithContext(t.Context(), bytes.NewReader(raw), raftengine.SnapshotRestoreOptions{
		Origin: raftengine.SnapshotRestoreExternalRebase,
	})
	if mode == "migration-bearing" {
		require.ErrorIs(t, err, store.ErrExternalSnapshotMigrationState)
		require.Equal(t, before, encryptedTSOState(t, fsm))
		return
	}
	require.NoError(t, err, "TSO's cipher-free validator must inspect raw migration metadata, not decrypt user values")
	if header {
		require.Equal(t, int64(9000), fsm.ceilingMs.Load())
		require.Equal(t, tsoLeaseAllocationFloor(9000), fsm.AllocationFloor())
	} else {
		require.Equal(t, before, encryptedTSOState(t, fsm))
	}
}

func encryptedTSOState(t *testing.T, fsm *TSOStateMachine) []byte {
	t.Helper()
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	return payload.Bytes()
}
