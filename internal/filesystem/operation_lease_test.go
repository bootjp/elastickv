package filesystem

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/stretchr/testify/require"
)

func TestValidateOpenHandleLeaseDoesNotRenewExpiredHandle(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(100, 0)
	svc := newTestServiceWithOptions(t, []uint64{2, 3},
		WithClock(func() time.Time { return now }), WithOpenHandleLeaseTTL(time.Second))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	clientID := []byte("mount")
	fh, err := svc.Open(ctx, file.Inode, clientID)
	require.NoError(t, err)
	require.Equal(t, time.Second, svc.OpenHandleLeaseTTL())
	require.NoError(t, svc.ValidateOpenHandleLease(ctx, file.Inode, fh, clientID))
	now = now.Add(2 * time.Second)
	require.ErrorIs(t, svc.ValidateOpenHandleLease(ctx, file.Inode, fh, clientID), ErrNotFound)
	require.ErrorIs(t, svc.ValidateOpenHandleLease(ctx, file.Inode, fh+1, clientID), ErrNotFound)
}

func TestLeaseOperationCannotResurrectReleasedHandle(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	clientID := []byte("mount")
	fh, err := svc.Open(ctx, file.Inode, clientID)
	require.NoError(t, err)
	firstID := testWriteOperationID(t, svc, 1)
	require.NoError(t, svc.RefreshOpenHandleLeaseWithOperation(ctx, firstID, file.Inode, fh, clientID))
	require.NoError(t, svc.Release(ctx, file.Inode, fh, clientID))
	require.NoError(t, svc.RefreshOpenHandleLeaseWithOperation(ctx, firstID, file.Inode, fh, clientID))
	missingID := firstID
	missingID.Sequence = 2
	require.ErrorIs(t, svc.RefreshOpenHandleLeaseWithOperation(ctx, missingID, file.Inode, fh, clientID), ErrNotFound)
	require.ErrorIs(t, svc.RefreshOpenHandleLeaseWithOperation(ctx, missingID, file.Inode, fh, clientID), ErrNotFound)
	ts, err := svc.readTS(ctx)
	require.NoError(t, err)
	_, exists, err := svc.openHandleLeaseAt(ctx, fskeys.RefKey(file.Inode, clientID, fh), ts)
	require.NoError(t, err)
	require.False(t, exists)
}
