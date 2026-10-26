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
