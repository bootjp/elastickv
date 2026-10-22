package kv

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func legacyPrepareEvidenceUserKeys() [][]byte {
	return [][]byte{[]byte("!txn|backup|prepare_evidence_enabled"),
		[]byte("!txn|backup|prepare_evidence_gate"), []byte("!txn|backup|prepare_evidence_timestamp_floor")}
}

func prepareNamespaceStore(t *testing.T, pebble bool, dir string) (store.MVCCStore, func()) {
	t.Helper()
	var st store.MVCCStore
	if pebble {
		var err error
		st, err = store.NewPebbleStore(dir)
		require.NoError(t, err)
	} else {
		st = store.NewMVCCStore()
	}
	closeStore := sync.OnceFunc(func() { require.NoError(t, st.Close()) })
	t.Cleanup(closeStore)
	return st, closeStore
}

func prepareNamespaceHistory(t *testing.T, st store.MVCCStore, key []byte) []store.MVCCVersion {
	t.Helper()
	exported, err := st.ExportVersions(t.Context(), store.ExportVersionsOptions{
		StartKey: key, EndKey: append(bytes.Clone(key), 0), MaxVersions: 10,
	})
	require.NoError(t, err)
	return exported.Versions
}

func assertPrepareNamespaceHistory(t *testing.T, want, got []store.MVCCVersion) {
	t.Helper()
	require.Len(t, got, len(want))
	for n, version := range want {
		require.Equal(t, version.Key, got[n].Key)
		require.Equal(t, version.CommitTS, got[n].CommitTS)
		require.Equal(t, version.Tombstone, got[n].Tombstone)
		require.Equal(t, version.ExpireAt, got[n].ExpireAt)
		require.Equal(t, version.KeyFamily, got[n].KeyFamily)
		// Snapshot encoding need not distinguish nil from empty tombstone bytes.
		require.True(t, bytes.Equal(version.Value, got[n].Value))
	}
}

func TestPrepareEvidenceUpgradePreservesPreviouslyValidUserNames(t *testing.T) {
	for _, pebble := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "pebble"}[pebble], func(t *testing.T) {
			for _, profile := range []string{"ordinary-values", "wire-shaped-values", "tombstones"} {
				t.Run(profile, func(t *testing.T) { assertPrepareNamespaceUpgrade(t, pebble, profile) })
			}
		})
	}
}

func assertPrepareNamespaceUpgrade(t *testing.T, pebble bool, profile string) {
	t.Helper()
	dir := t.TempDir()
	st, closeSource := prepareNamespaceStore(t, pebble, dir)
	keys := legacyPrepareEvidenceUserKeys()
	values := [][]byte{[]byte("user-enabled"), []byte("user-gate"), []byte("user-floor")}
	if profile != "ordinary-values" {
		values = [][]byte{{1}, binary.BigEndian.AppendUint64([]byte{prepareCreationReleased}, 100),
			binary.BigEndian.AppendUint64(nil, 100)}
	}
	histories := make(map[string][]store.MVCCVersion)
	for n, key := range keys {
		require.NoError(t, st.PutAt(t.Context(), key, []byte("previous-user-version"), 1000, 0))
		require.NoError(t, st.PutAt(t.Context(), key, values[n], 2000, ^uint64(0)))
		if profile == "tombstones" {
			require.NoError(t, st.DeleteAt(t.Context(), key, 3000))
		}
		histories[string(key)] = prepareNamespaceHistory(t, st, key)
	}
	fsm := newPrepareSafetyFSM(t, st)
	require.NoError(t, fsm.backupFloorLoadErr)
	enabled, err := PrepareEvidenceEnabled(t.Context(), st)
	require.NoError(t, err)
	require.False(t, enabled, "user bytes must not manufacture an enable marker")
	sealed, released, err := readPrepareEvidenceCreationGate(t.Context(), st)
	require.NoError(t, err)
	require.False(t, sealed)
	require.Zero(t, released)
	floor, err := readPrepareEvidenceFenceFloor(t.Context(), st)
	require.NoError(t, err)
	require.Zero(t, floor)
	for _, command := range [][]byte{EncodePrepareEvidenceFence(0), EncodePrepareEvidenceSeal(),
		EncodePrepareEvidenceFence(100), EncodePrepareEvidenceRelease(100)} {
		require.Nil(t, fsm.Apply(command))
	}
	image := jointRecoverySnapshot(t, fsm)
	closeSource()
	reopened, _ := prepareNamespaceStore(t, pebble, dir)
	restarted := newPrepareSafetyFSM(t, reopened)
	require.NoError(t, restarted.backupFloorLoadErr)
	require.NoError(t, restarted.Restore(bytes.NewReader(image)))
	assertJointRecoveryDataGate(t, restarted, 100, true)
	for n, key := range keys {
		assertPrepareNamespaceHistory(t, histories[string(key)], prepareNamespaceHistory(t, reopened, key))
		value, err := reopened.GetAt(t.Context(), key, 2000)
		require.NoError(t, err)
		require.Equal(t, values[n], value)
		require.False(t, isTxnInternalKey(key))
		require.False(t, distribution.IsMigrationKnownInternalKey(key))
	}
	assertPrepareNamespaceMigration(t, pebble, histories)
	for _, key := range keys {
		require.NoError(t, applyFSMRequest(t, restarted, &pb.Request{Ts: 4000, TimestampAdmissionFloor: 100,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("updated-user-value")}}}))
		value, err := reopened.GetAt(t.Context(), key, ^uint64(0))
		require.NoError(t, err)
		require.Equal(t, []byte("updated-user-value"), value)
	}
}

func assertPrepareNamespaceMigration(t *testing.T, pebble bool, histories map[string][]store.MVCCVersion) {
	t.Helper()
	target, _ := prepareNamespaceStore(t, pebble, t.TempDir())
	fsm := newPrepareSafetyFSM(t, target)
	request := &pb.ImportRangeVersionsRequest{JobId: 7, BracketId: 1, BatchSeq: 1, Cursor: []byte("end")}
	for _, key := range legacyPrepareEvidenceUserKeys() {
		for _, version := range histories[string(key)] {
			request.Versions = append(request.Versions, &pb.MVCCVersion{Key: version.Key, CommitTs: version.CommitTS,
				Value: version.Value, Tombstone: version.Tombstone, ExpireAt: version.ExpireAt, KeyFamily: version.KeyFamily})
		}
	}
	command, err := MarshalMigrationImportCommand(request)
	require.NoError(t, err)
	require.IsType(t, store.ImportVersionsResult{}, fsm.Apply(command))
	command, err = MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 7, MaxVersions: 100})
	require.NoError(t, err)
	require.IsType(t, store.PromoteVersionsResult{}, fsm.Apply(command))
	for key, history := range histories {
		assertPrepareNamespaceHistory(t, history, prepareNamespaceHistory(t, target, []byte(key)))
	}
	enabled, err := PrepareEvidenceEnabled(t.Context(), target)
	require.NoError(t, err)
	require.False(t, enabled, "migrated user bytes must not enable the protocol")
}
