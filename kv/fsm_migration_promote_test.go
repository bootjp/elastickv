package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrationPromoteTargetKeyRestoresRawKey(t *testing.T) {
	t.Parallel()

	targetKey := migrationPromoteTargetKey(9)
	raw, ok := targetKey(distribution.MigrationStagedDataKey(9, []byte("user|k")))
	require.True(t, ok)
	require.Equal(t, []byte("user|k"), raw)

	_, ok = targetKey(distribution.MigrationStagedDataKey(10, []byte("user|k")))
	require.False(t, ok)
}

func TestApplyMigrationPromoteMovesStagedVersions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	hlc := NewHLC()
	fsm := &kvFSM{store: st, hlc: hlc}
	staged := distribution.MigrationStagedDataKey(9, []byte("user|k"))
	require.NoError(t, st.PutAt(ctx, staged, []byte("v10"), 10, 0))
	require.NoError(t, st.DeleteAt(ctx, staged, 20))

	cmd, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{
		JobId:       9,
		MaxVersions: 10,
	})
	require.NoError(t, err)
	applied := fsm.Apply(cmd)
	result, ok := applied.(store.PromoteVersionsResult)
	require.True(t, ok, "got %T: %v", applied, applied)
	require.True(t, result.Done)
	require.Equal(t, uint64(2), result.PromotedRows)
	require.Equal(t, uint64(2), result.TotalPromotedRows)
	require.Equal(t, uint64(20), result.MaxPromotedTS)
	require.GreaterOrEqual(t, hlc.Current(), uint64(20))
	stateReader, ok := st.(store.MigrationPromotionStateReader)
	require.True(t, ok)
	state, ok, err := stateReader.MigrationPromotionState(ctx, 9)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, state.Done)
	require.Equal(t, uint64(2), state.PromotedRows)

	got, err := st.GetAt(ctx, []byte("user|k"), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("v10"), got)
	_, err = st.GetAt(ctx, []byte("user|k"), 20)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	left, err := st.ExportVersions(ctx, store.ExportVersionsOptions{
		StartKey:    distribution.MigrationStagedDataKeyPrefix(9),
		EndKey:      store.PrefixScanEnd(distribution.MigrationStagedDataKeyPrefix(9)),
		MaxVersions: 10,
	})
	require.NoError(t, err)
	require.Empty(t, left.Versions)
}
