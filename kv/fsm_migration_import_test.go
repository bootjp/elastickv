package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestMigrationStoreVersionsFromProtoStagesKeys(t *testing.T) {
	t.Parallel()

	rawKey := []byte("user|k")
	value := []byte("value")
	got := migrationStoreVersionsFromProto(7, []*pb.MVCCVersion{
		nil,
		{
			Key:       rawKey,
			CommitTs:  11,
			Value:     value,
			KeyFamily: distribution.MigrationFamilyUser,
			ExpireAt:  123,
		},
	})

	require.Len(t, got, 1)
	require.Equal(t, distribution.MigrationStagedDataKey(7, []byte("user|k")), got[0].Key)
	require.Equal(t, uint64(11), got[0].CommitTS)
	require.Equal(t, []byte("value"), got[0].Value)
	require.Equal(t, distribution.MigrationFamilyUser, got[0].KeyFamily)
	require.Equal(t, uint64(123), got[0].ExpireAt)

	rawKey[0] = 'X'
	value[0] = 'X'
	require.Equal(t, distribution.MigrationStagedDataKey(7, []byte("user|k")), got[0].Key)
	require.Equal(t, []byte("value"), got[0].Value)
}

func TestApplyMigrationImportWritesOnlyStagedKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	hlc := NewHLC()
	fsm := &kvFSM{store: st, hlc: hlc}
	req := &pb.ImportRangeVersionsRequest{
		JobId:     9,
		BracketId: 1,
		BatchSeq:  1,
		Cursor:    []byte("cursor"),
		Versions: []*pb.MVCCVersion{
			{Key: []byte("user|k"), CommitTs: 10, Value: []byte("v")},
		},
	}
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	applied := fsm.applyMigrationImport(ctx, data)
	result, ok := applied.(store.ImportVersionsResult)
	require.True(t, ok, "got %T: %v", applied, applied)
	require.Equal(t, []byte("cursor"), result.AckedCursor)
	require.Equal(t, uint64(10), result.MaxImportedTS)
	require.GreaterOrEqual(t, hlc.Current(), uint64(10))

	staged := distribution.MigrationStagedDataKey(9, []byte("user|k"))
	got, err := st.GetAt(ctx, staged, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	_, err = st.GetAt(ctx, []byte("user|k"), 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

type captureMigrationImportStore struct {
	store.MVCCStore
	opts store.ImportVersionsOptions
}

func (s *captureMigrationImportStore) ImportVersionsRaft(_ context.Context, opts store.ImportVersionsOptions) (store.ImportVersionsResult, error) {
	s.opts = opts
	return store.ImportVersionsResult{AckedCursor: opts.Cursor, MaxImportedTS: 10}, nil
}

func TestApplyMigrationImportThreadsPendingApplyIndex(t *testing.T) {
	t.Parallel()

	capturing := &captureMigrationImportStore{}
	fsm := &kvFSM{store: capturing, pendingApplyIdx: 1234}
	req := &pb.ImportRangeVersionsRequest{
		JobId:     9,
		BracketId: 1,
		BatchSeq:  1,
		Cursor:    []byte("cursor"),
		Versions: []*pb.MVCCVersion{
			{Key: []byte("user|k"), CommitTs: 10, Value: []byte("v")},
		},
	}
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	applied := fsm.applyMigrationImport(context.Background(), data)
	result, ok := applied.(store.ImportVersionsResult)
	require.True(t, ok, "got %T: %v", applied, applied)
	require.Equal(t, []byte("cursor"), result.AckedCursor)
	require.Equal(t, uint64(1234), capturing.opts.AppliedIndex)
	require.Equal(t, uint64(9), capturing.opts.JobID)
	require.Len(t, capturing.opts.Versions, 1)
	require.Equal(t, distribution.MigrationStagedDataKey(9, []byte("user|k")), capturing.opts.Versions[0].Key)
}
