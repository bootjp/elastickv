package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestRetiredMigrationMutationsRejectWithoutRaftHalt(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(t.Context(), []byte("customer"), []byte("active"), 100, 0))
	fsm := newPrepareSafetyFSM(t, st)
	cleanup, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 7, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA})
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(cleanup))
	readiness, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{JobId: 7, MigrationJobId: 7,
		Armed: true, SourceWriteFence: true, MinWriteTsExclusive: 1000, RetentionPinTs: 1})
	require.NoError(t, err)
	imported, err := MarshalMigrationImportCommand(&pb.ImportRangeVersionsRequest{JobId: 7, BracketId: 1, BatchSeq: 1})
	require.NoError(t, err)
	promoted, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 7})
	require.NoError(t, err)
	deleted, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 7, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS})
	require.NoError(t, err)
	for _, command := range [][]byte{readiness, imported, promoted, deleted} {
		response := fsm.Apply(command)
		require.Nil(t, haltApplyOf(response), "a retired job must not stop replication")
		applyErr, ok := response.(error)
		require.True(t, ok)
		require.ErrorIs(t, applyErr, store.ErrMigrationRetired)
		require.Equal(t, uint64(100), st.LastCommitTS())
	}
	for _, tombstone := range []bool{false, true} {
		command, marshalErr := MarshalMigrationImportCommand(&pb.ImportRangeVersionsRequest{JobId: 8, BracketId: 1, BatchSeq: 1,
			Versions: []*pb.MVCCVersion{{Key: []byte("!migstage|retired"), CommitTs: 200, Tombstone: tombstone}}})
		require.NoError(t, marshalErr)
		response := fsm.Apply(command)
		require.Nil(t, haltApplyOf(response))
		applyErr, ok := response.(error)
		require.True(t, ok)
		require.ErrorIs(t, applyErr, ErrInvalidRequest)
	}
	retire, err := MarshalMigrationRetireCommand(7)
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(retire), "legacy retirement remains idempotent")
	reader, ok := st.(store.MigrationRetirementReader)
	require.True(t, ok)
	retired, err := reader.MigrationRetired(t.Context(), 7)
	require.NoError(t, err)
	require.True(t, retired)
	require.NoError(t, applyFSMRequest(t, fsm, &pb.Request{Ts: 200, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("customer"), Value: []byte("still-active")}}}))
	staged := distribution.MigrationStagedDataKey(8, []byte("!migstage|retired"))
	require.NoError(t, st.PutAt(t.Context(), staged, []byte("forged-retirement"), 300, 0))
	command, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 8, MaxVersions: 10})
	require.NoError(t, err)
	response := fsm.Apply(command)
	require.Nil(t, haltApplyOf(response))
	applyErr, ok := response.(error)
	require.True(t, ok)
	require.ErrorIs(t, applyErr, store.ErrInvalidImportVersion)
	retired, err = reader.MigrationRetired(t.Context(), 8)
	require.NoError(t, err)
	require.False(t, retired, "legacy staging must not forge a retirement witness")
}
