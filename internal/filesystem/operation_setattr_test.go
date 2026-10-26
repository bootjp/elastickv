package filesystem

import (
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

func TestSetAttrWithOperationRequiresSeparateSizePlanner(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, nil)
	_, err := svc.SetAttrWithOperation(ctx, OperationID{}, RootInode, SetAttrMask{Size: true}, SetAttr{Size: 1})
	require.ErrorIs(t, err, ErrUnsupported)
}
