package filesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenameWithOperationDoesNotMoveRecreatedSource(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	old, err := svc.Create(ctx, RootInode, []byte("source"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	require.NoError(t, svc.RenameWithOperation(ctx, id, RootInode, []byte("source"), RootInode, []byte("target")))
	newFile, err := svc.Create(ctx, RootInode, []byte("source"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.NoError(t, svc.RenameWithOperation(ctx, id, RootInode, []byte("source"), RootInode, []byte("target")))
	source, err := svc.Resolve(ctx, RootInode, []byte("source"))
	require.NoError(t, err)
	require.Equal(t, newFile.Inode, source)
	target, err := svc.Resolve(ctx, RootInode, []byte("target"))
	require.NoError(t, err)
	require.Equal(t, old.Inode, target)
}

func TestRenameWithOperationRetainsMissingSource(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	require.ErrorIs(t, svc.RenameWithOperation(ctx, id, RootInode, []byte("source"), RootInode, []byte("target")), ErrNotFound)
	file, err := svc.Create(ctx, RootInode, []byte("source"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.ErrorIs(t, svc.RenameWithOperation(ctx, id, RootInode, []byte("source"), RootInode, []byte("target")), ErrNotFound)
	source, err := svc.Resolve(ctx, RootInode, []byte("source"))
	require.NoError(t, err)
	require.Equal(t, file.Inode, source)
}

func TestRenameWithOperationNoopIsRetained(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	_, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	require.NoError(t, svc.RenameWithOperation(ctx, id, RootInode, []byte("file"), RootInode, []byte("file")))
	require.NoError(t, svc.RenameWithOperation(ctx, id, RootInode, []byte("file"), RootInode, []byte("file")))
}
