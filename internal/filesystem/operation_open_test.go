package filesystem

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/stretchr/testify/require"
)

func TestOpenWithOperationRetainsOneHandle(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3, 4})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	clientID := []byte("mount")
	fh, err := svc.OpenWithOperation(ctx, id, file.Inode, clientID)
	require.NoError(t, err)
	otherFH, err := svc.Open(ctx, file.Inode, clientID)
	require.NoError(t, err)
	replayed, err := svc.OpenWithOperation(ctx, id, file.Inode, clientID)
	require.NoError(t, err)
	require.Equal(t, fh, replayed)
	require.NotEqual(t, fh, otherFH)
	require.NoError(t, svc.VerifyOperationReceipt(ctx, id))
	ts, err := svc.readTS(ctx)
	require.NoError(t, err)
	_, exists, err := svc.openHandleLeaseAt(ctx, fskeys.RefKey(file.Inode, clientID, fh), ts)
	require.NoError(t, err)
	require.True(t, exists)
	_, err = svc.OpenWithOperation(ctx, id, file.Inode, []byte("other"))
	require.ErrorIs(t, err, ErrOperationIDReused)
}

func TestOpenWithOperationRetainsMissingInode(t *testing.T) {
	svc := newTestServiceWithOptions(t, nil)
	require.NoError(t, svc.InitializeRoot(t.Context(), testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	for range 2 {
		_, err := svc.OpenWithOperation(t.Context(), id, 999, []byte("mount"))
		require.ErrorIs(t, err, ErrNotFound)
	}
}

func TestOpenWithOperationResolvesLostCommitReply(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	svc.dispatch = &writeOperationFaultDispatcher{inner: inner,
		key: fskeys.RefKey(file.Inode, []byte("mount"), 3), loseAck: true}
	fh, err := svc.OpenWithOperation(ctx, id, file.Inode, []byte("mount"))
	require.NoError(t, err)
	require.EqualValues(t, 3, fh)
	replayed, err := svc.OpenWithOperation(ctx, id, file.Inode, []byte("mount"))
	require.NoError(t, err)
	require.Equal(t, fh, replayed)
}
