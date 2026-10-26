package filesystem

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/stretchr/testify/require"
)

func TestReleaseWithOperationDoesNotRemoveNewHandle(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3, 4})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	clientID := []byte("mount")
	fh, err := svc.Open(ctx, file.Inode, clientID)
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	require.NoError(t, svc.ReleaseWithOperation(ctx, id, file.Inode, fh, clientID))
	otherFH, err := svc.Open(ctx, file.Inode, clientID)
	require.NoError(t, err)
	require.NoError(t, svc.ReleaseWithOperation(ctx, id, file.Inode, fh, clientID))
	require.NoError(t, svc.VerifyOperationReceipt(ctx, id))
	ts, err := svc.readTS(ctx)
	require.NoError(t, err)
	_, exists, err := svc.openHandleLeaseAt(ctx, fskeys.RefKey(file.Inode, clientID, otherFH), ts)
	require.NoError(t, err)
	require.True(t, exists)
	_, exists, err = svc.openHandleLeaseAt(ctx, fskeys.RefKey(file.Inode, clientID, fh), ts)
	require.NoError(t, err)
	require.False(t, exists)
	// A delayed lease refresh cannot revive the terminal release.
	require.ErrorIs(t, svc.RefreshOpenHandleLease(ctx, file.Inode, fh, clientID), ErrNotFound)
}

func TestReleaseWithOperationResolvesLostCommitReply(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	clientID := []byte("mount")
	fh, err := svc.Open(ctx, file.Inode, clientID)
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	svc.dispatch = &writeOperationFaultDispatcher{inner: inner,
		key: fskeys.RefKey(file.Inode, clientID, fh), loseAck: true}
	require.NoError(t, svc.ReleaseWithOperation(ctx, id, file.Inode, fh, clientID))
	require.NoError(t, svc.ReleaseWithOperation(ctx, id, file.Inode, fh, clientID))
	require.NoError(t, svc.VerifyOperationReceipt(ctx, id))
}
