package filesystem

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestVerifyOperationReceiptAfterPebbleReopen(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "pebble")
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	_, err = svc.WriteWithOperation(ctx, id, file.Inode, 0, []byte("durable"))
	require.NoError(t, err)
	require.NoError(t, svc.VerifyOperationReceipt(ctx, id))
	require.NoError(t, st.Close())

	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	restarted, err := NewService(reopened, &testCoordinator{st: reopened})
	require.NoError(t, err)
	require.NoError(t, restarted.VerifyOperationReceipt(ctx, id))
	data, err := restarted.Read(ctx, file.Inode, 0, 0, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("durable"), data)
}

func TestNamespaceAndMetadataReceiptsAfterPebbleReopen(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "pebble")
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("source"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	removed, err := svc.Create(ctx, RootInode, []byte("removed"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Mkdir(ctx, RootInode, []byte("empty"), CreateOptions{Mode: testRootMode})
	require.NoError(t, err)

	ids := make([]OperationID, 5)
	firstID := testWriteOperationID(t, svc, 1)
	for i := range ids {
		ids[i] = firstID
		ids[i].Sequence = uint64(i + 1)
	}
	_, err = svc.WriteWithOperation(ctx, ids[0], file.Inode, 0, []byte("durable"))
	require.NoError(t, err)
	_, err = svc.SetAttrWithOperation(ctx, ids[1], file.Inode, SetAttrMask{Size: true}, SetAttr{Size: 3})
	require.NoError(t, err)
	require.NoError(t, svc.RenameWithOperation(ctx, ids[2], RootInode, []byte("source"), RootInode, []byte("target")))
	require.NoError(t, svc.UnlinkWithOperation(ctx, ids[3], RootInode, []byte("removed")))
	require.NoError(t, svc.RmdirWithOperation(ctx, ids[4], RootInode, []byte("empty")))
	for _, id := range ids {
		require.NoError(t, svc.VerifyOperationReceipt(ctx, id))
	}
	require.NoError(t, st.Close())

	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	restarted, err := NewService(reopened, &testCoordinator{st: reopened})
	require.NoError(t, err)
	for _, id := range ids {
		require.NoError(t, restarted.VerifyOperationReceipt(ctx, id))
	}
	inode, err := restarted.Resolve(ctx, RootInode, []byte("target"))
	require.NoError(t, err)
	require.Equal(t, file.Inode, inode)
	data, err := restarted.Read(ctx, inode, 0, 0, 3)
	require.NoError(t, err)
	require.Equal(t, []byte("dur"), data)
	_, err = restarted.Resolve(ctx, RootInode, []byte("source"))
	require.ErrorIs(t, err, ErrNotFound)
	_, err = restarted.Resolve(ctx, RootInode, []byte("removed"))
	require.ErrorIs(t, err, ErrNotFound)
	_, err = restarted.Resolve(ctx, RootInode, []byte("empty"))
	require.ErrorIs(t, err, ErrNotFound)
	require.NotEqual(t, file.Inode, removed.Inode)
}

func TestCreateReceiptsAfterPebbleReopen(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "pebble")
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	fileID := testWriteOperationID(t, svc, 1)
	dirID := fileID
	dirID.Sequence = 2
	openID := fileID
	openID.Sequence = 3
	file, err := svc.CreateWithOperation(ctx, fileID, RootInode, []byte("file"), CreateOptions{Mode: testFileMode, ClientID: []byte("mount")})
	require.NoError(t, err)
	createdDir, err := svc.MkdirWithOperation(ctx, dirID, RootInode, []byte("dir"), CreateOptions{Mode: testRootMode})
	require.NoError(t, err)
	fh, err := svc.OpenWithOperation(ctx, openID, file.Inode, []byte("mount"))
	require.NoError(t, err)
	require.NoError(t, svc.VerifyOperationReceipt(ctx, fileID))
	require.NoError(t, svc.VerifyOperationReceipt(ctx, dirID))
	require.NoError(t, svc.VerifyOperationReceipt(ctx, openID))
	require.NoError(t, st.Close())

	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	restarted, err := NewService(reopened, &testCoordinator{st: reopened})
	require.NoError(t, err)
	require.NoError(t, restarted.VerifyOperationReceipt(ctx, fileID))
	require.NoError(t, restarted.VerifyOperationReceipt(ctx, dirID))
	require.NoError(t, restarted.VerifyOperationReceipt(ctx, openID))
	replayed, err := restarted.CreateWithOperation(ctx, fileID, RootInode, []byte("file"), CreateOptions{Mode: testFileMode, ClientID: []byte("mount")})
	require.NoError(t, err)
	require.Equal(t, file, replayed)
	replayedDir, err := restarted.MkdirWithOperation(ctx, dirID, RootInode, []byte("dir"), CreateOptions{Mode: testRootMode})
	require.NoError(t, err)
	require.Equal(t, createdDir, replayedDir)
	replayedFH, err := restarted.OpenWithOperation(ctx, openID, file.Inode, []byte("mount"))
	require.NoError(t, err)
	require.Equal(t, fh, replayedFH)
}
