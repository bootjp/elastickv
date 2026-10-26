package filesystem

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/stretchr/testify/require"
)

func TestCreateWithOperationSupportsLongClientID(t *testing.T) {
	ctx := t.Context()
	svc := newTestServiceWithOptions(t, []uint64{2, 3})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	clientID := bytes.Repeat([]byte("a"), 4096)
	result, err := svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), CreateOptions{
		Mode: testFileMode, ClientID: clientID,
	})
	require.NoError(t, err)
	require.NotZero(t, result.FH)
	require.NoError(t, svc.VerifyOperationReceipt(ctx, id))
	resolved, err := svc.Resolve(ctx, RootInode, []byte("file"))
	require.NoError(t, err)
	require.Equal(t, result.Inode, resolved)
}

func TestCreateWithOperationRetainsOriginalInodeAndHandle(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3, 4})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	opts := CreateOptions{Mode: testFileMode, ClientID: []byte("client")}
	original, err := svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), opts)
	require.NoError(t, err)
	require.NotZero(t, original.FH)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("file")))
	replacement, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	replayed, err := svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), opts)
	require.NoError(t, err)
	require.Equal(t, original, replayed)
	require.NotEqual(t, replacement.Inode, replayed.Inode)
	_, err = svc.CreateWithOperation(ctx, id, RootInode, []byte("other"), opts)
	require.ErrorIs(t, err, ErrOperationIDReused)
}

func TestMkdirWithOperationDoesNotCreateReplacement(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	original, err := svc.MkdirWithOperation(ctx, id, RootInode, []byte("dir"), CreateOptions{Mode: testRootMode})
	require.NoError(t, err)
	require.NoError(t, svc.Rmdir(ctx, RootInode, []byte("dir")))
	replayed, err := svc.MkdirWithOperation(ctx, id, RootInode, []byte("dir"), CreateOptions{Mode: testRootMode})
	require.NoError(t, err)
	require.Equal(t, original, replayed)
	_, err = svc.Resolve(ctx, RootInode, []byte("dir"))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestCreateWithOperationRetainsExistingNameRejection(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	original, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	for range 2 {
		_, err = svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
		require.ErrorIs(t, err, ErrExists)
	}
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("file")))
	_, err = svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.ErrorIs(t, err, ErrExists)
	_, err = svc.Resolve(ctx, RootInode, []byte("file"))
	require.ErrorIs(t, err, ErrNotFound)
	require.NotZero(t, original.Inode)
}

func TestCreateWithOperationResolvesLostCommitReply(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	svc.dispatch = &writeOperationFaultDispatcher{inner: inner,
		key: fskeys.DirEntryKey(RootInode, []byte("file")), loseAck: true}
	result, err := svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), CreateOptions{Mode: testFileMode, ClientID: []byte("mount")})
	require.NoError(t, err)
	require.NotZero(t, result.FH)
	replayed, err := svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), CreateOptions{Mode: testFileMode, ClientID: []byte("mount")})
	require.NoError(t, err)
	require.Equal(t, result, replayed)
	resolved, err := svc.Resolve(ctx, RootInode, []byte("file"))
	require.NoError(t, err)
	require.Equal(t, result.Inode, resolved)
}

func TestCreateWithOperationReplansDefinitiveRejectionConflict(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	_, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	svc.dispatch = &writeOperationFaultDispatcher{inner: inner, rejectConflict: true}
	_, err = svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.ErrorIs(t, err, ErrExists)
	_, err = svc.CreateWithOperation(ctx, id, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.ErrorIs(t, err, ErrExists)
}
