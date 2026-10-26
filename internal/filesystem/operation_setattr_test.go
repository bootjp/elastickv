package filesystem

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetAttrWithOperationRetainsOriginalStat(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	mask := SetAttrMask{Mode: true, UID: true}
	attrs := SetAttr{Mode: 0o600, UID: 42}
	original, err := svc.SetAttrWithOperation(ctx, id, file.Inode, mask, attrs)
	require.NoError(t, err)
	require.EqualValues(t, 0o600, original.Mode)
	require.EqualValues(t, 42, original.UID)
	_, err = svc.SetAttr(ctx, file.Inode, SetAttrMask{Mode: true}, SetAttr{Mode: 0o644})
	require.NoError(t, err)
	replayed, err := svc.SetAttrWithOperation(ctx, id, file.Inode, mask, attrs)
	require.NoError(t, err)
	require.Equal(t, original, replayed)
	current, err := svc.GetAttr(ctx, file.Inode)
	require.NoError(t, err)
	require.EqualValues(t, 0o644, current.Mode)
	_, err = svc.SetAttrWithOperation(ctx, id, file.Inode, mask, SetAttr{Mode: 0o640, UID: 42})
	require.ErrorIs(t, err, ErrOperationIDReused)
}

func TestSetAttrWithOperationRetainsMissingInode(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, nil)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	for range 2 {
		_, err := svc.SetAttrWithOperation(ctx, id, 999, SetAttrMask{Mode: true}, SetAttr{Mode: 0o600})
		require.ErrorIs(t, err, ErrNotFound)
	}
}

func TestSetAttrWithOperationDoesNotRepeatShrinkAfterExtension(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcdefghijkl"))
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	mask := SetAttrMask{Size: true, Mode: true}
	attrs := SetAttr{Size: 4, Mode: 0o600}
	original, err := svc.SetAttrWithOperation(ctx, id, file.Inode, mask, attrs)
	require.NoError(t, err)
	require.EqualValues(t, 4, original.Size)
	require.EqualValues(t, 0o600, original.Mode)
	_, err = svc.Write(ctx, file.Inode, 0, 4, []byte("xyz"))
	require.NoError(t, err)
	replayed, err := svc.SetAttrWithOperation(ctx, id, file.Inode, mask, attrs)
	require.NoError(t, err)
	require.Equal(t, original, replayed)
	current, err := svc.GetAttr(ctx, file.Inode)
	require.NoError(t, err)
	require.EqualValues(t, 7, current.Size)
	data, err := svc.Read(ctx, file.Inode, 0, 0, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("abcdxyz"), data)
}

func TestSetAttrWithOperationRetainsDirectorySizeRejection(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, nil)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	for range 2 {
		_, err := svc.SetAttrWithOperation(ctx, id, RootInode, SetAttrMask{Size: true}, SetAttr{Size: 1})
		require.ErrorIs(t, err, ErrIsDir)
	}
}

func TestSetAttrWithOperationFinishesPagedChunkDeletion(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	data := bytes.Repeat([]byte("x"), (chunkDeleteTxnPageSize+5)*int(testChunkSize))
	_, err = svc.Write(ctx, file.Inode, 0, 0, data)
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	stat, err := svc.SetAttrWithOperation(ctx, id, file.Inode, SetAttrMask{Size: true}, SetAttr{Size: 0})
	require.NoError(t, err)
	require.Zero(t, stat.Size)
	ts, err := svc.readTS(ctx)
	require.NoError(t, err)
	meta, err := svc.inodeAt(ctx, file.Inode, ts)
	require.NoError(t, err)
	page, err := svc.scanExistingChunkPage(ctx, meta.HomeSlot, file.Inode, ts, nil, chunkDeleteTxnPageSize+1)
	require.NoError(t, err)
	require.Empty(t, page)
}
