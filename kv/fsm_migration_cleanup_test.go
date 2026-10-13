package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestApplyMigrationCleanupCommandDeletesBoundedVersions(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("old"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("new"), 20, 0))
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	cmd, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
		JobId:           1,
		Mode:            pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		RangeStart:      []byte("a"),
		RangeEnd:        []byte("b"),
		MaxCommitTs:     10,
		MaxVersions:     16,
		MaxBytes:        1 << 20,
		MaxScannedBytes: 1 << 20,
		KeyFamily:       1,
	})
	require.NoError(t, err)
	result, ok := fsm.applyMigrationCleanup(ctx, cmd[1:]).(store.CleanupVersionsResult)
	require.True(t, ok)
	require.True(t, result.Done)
	require.Equal(t, uint64(1), result.DeletedRows)

	value, err := st.GetAt(ctx, []byte("a"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), value)
	_, err = st.GetAt(ctx, []byte("a"), 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}
