package filesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnlinkWithOperationDoesNotRemoveRecreatedName(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	old, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	require.NoError(t, svc.UnlinkWithOperation(ctx, id, RootInode, []byte("file")))
	newFile, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.NotEqual(t, old.Inode, newFile.Inode)
	require.NoError(t, svc.UnlinkWithOperation(ctx, id, RootInode, []byte("file")))
	resolved, err := svc.Resolve(ctx, RootInode, []byte("file"))
	require.NoError(t, err)
	require.Equal(t, newFile.Inode, resolved)
	require.ErrorIs(t, svc.UnlinkWithOperation(ctx, id, RootInode, []byte("other")), ErrOperationIDReused)
}

func TestUnlinkWithOperationRetainsMissingName(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	require.ErrorIs(t, svc.UnlinkWithOperation(ctx, id, RootInode, []byte("file")), ErrNotFound)
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.ErrorIs(t, svc.UnlinkWithOperation(ctx, id, RootInode, []byte("file")), ErrNotFound)
	resolved, err := svc.Resolve(ctx, RootInode, []byte("file"))
	require.NoError(t, err)
	require.Equal(t, file.Inode, resolved)
}

func TestUnlinkWithOperationRetainsDirectoryRejection(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	_, err := svc.Mkdir(ctx, RootInode, []byte("dir"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	for range 2 {
		require.ErrorIs(t, svc.UnlinkWithOperation(ctx, id, RootInode, []byte("dir")), ErrIsDir)
	}
}
