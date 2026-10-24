package kv

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrationPromoteRestoredStagingPreservesLocalControl(t *testing.T) {
	for _, key := range [][]byte{backupTimestampFloorKey, prepareEvidenceEnabledKey, prepareEvidenceGateKey, prepareEvidenceTimestampFloorKey, replicatedPinStateKey} {
		for _, tombstone := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s-delete-%t", key, tombstone), func(t *testing.T) {
				assertRestoredPromotionPreservesControl(t, key, tombstone)
			})
		}
	}
}

func assertRestoredPromotionPreservesControl(t *testing.T, key []byte, tombstone bool) {
	t.Helper()
	ctx := t.Context()
	source, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Close()) }()
	if bytes.Equal(key, replicatedPinStateKey) {
		seedActivatedPinJournal(t, source)
	}
	fsm := newPrepareSafetyFSM(t, source)
	require.NoError(t, fsm.persistBackupTimestampFloor(ctx, 100))
	for _, command := range [][]byte{EncodePrepareEvidenceFence(0), EncodePrepareEvidenceSeal(), EncodePrepareEvidenceFence(100), EncodePrepareEvidenceRelease(100)} {
		require.Nil(t, fsm.Apply(command))
	}
	before, err := source.GetAt(ctx, key, ^uint64(0))
	require.NoError(t, err)
	// Directly seed the pre-upgrade staging representation, not the new import path.
	userKey := []byte("!a-ordinary-user")
	stagedUser := distribution.MigrationStagedDataKey(7, userKey)
	stagedControl := distribution.MigrationStagedDataKey(7, key)
	require.NoError(t, source.PutAt(ctx, stagedUser, []byte("user-value"), 1000, 0))
	if tombstone {
		require.NoError(t, source.DeleteAt(ctx, stagedControl, 1000))
	} else {
		require.NoError(t, source.PutAt(ctx, stagedControl, []byte("old-peer-state"), 1000, 0))
	}
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	dir := t.TempDir()
	restoredStore, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, restoredStore.Close())
		}
	})
	restored := newPrepareSafetyFSM(t, restoredStore)
	require.NoError(t, restored.Restore(bytes.NewReader(raw.Bytes())))
	command, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 7, MaxVersions: 10})
	require.NoError(t, err)
	response := restored.Apply(command)
	require.Nil(t, haltApplyOf(response), "old staging must be refused without halting replication")
	applyErr, ok := response.(error)
	require.True(t, ok, "unexpected promotion result %T: %v", response, response)
	require.ErrorIs(t, applyErr, ErrInvalidRequest)
	after, err := restoredStore.GetAt(ctx, key, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = restoredStore.GetAt(ctx, userKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "the rejected chunk must not partially promote ordinary rows")
	value, err := restoredStore.GetAt(ctx, stagedUser, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("user-value"), value)
	stateReader, ok := restoredStore.(store.MigrationPromotionStateReader)
	require.True(t, ok)
	_, exists, err := stateReader.MigrationPromotionState(ctx, 7)
	require.NoError(t, err)
	require.False(t, exists, "rejected staging must not advance migration progress")
	require.NoError(t, restoredStore.Close())
	closed = true
	reopenedStore, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopenedStore.Close()) })
	reopened := newPrepareSafetyFSM(t, reopenedStore)
	after, err = reopenedStore.GetAt(ctx, key, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, applyFSMRequest(t, reopened, prepareSafetyAdmittedCreation("raw", 2000, 100)))
}
