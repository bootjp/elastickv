package filesystem

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestServiceCreateAndDeleteClearDurableIntents(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	_, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	assertIntentCount(t, ctx, svc, 0)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("file")))
	assertIntentCount(t, ctx, svc, 0)
}

func TestServiceRecoverIntentsAbortsPreparedNamespaceMutation(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))

	intent := IntentState{
		ID:        []byte("create-crash"),
		Kind:      IntentKindCreate,
		Phase:     "prepared",
		Parent:    RootInode,
		Name:      []byte("never-committed"),
		CreatedAt: svc.now().UnixNano(),
		UpdatedAt: svc.now().UnixNano(),
	}
	key := fskeys.IntentKey(intent.ID)
	elem, err := putElem(key, intent)
	require.NoError(t, err)
	ts := svc.store.LastCommitTS()
	require.NoError(t, svc.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{elem}, [][]byte{key}))
	assertIntentCount(t, ctx, svc, 1)

	stats, err := svc.RecoverIntents(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.IntentsCleared)
	assertIntentCount(t, ctx, svc, 0)
	_, err = svc.Resolve(ctx, RootInode, []byte("never-committed"))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestServiceRecoverIntentsHonorsScanLimit(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))

	for _, id := range []string{"create-a", "create-b", "create-c"} {
		intent := IntentState{
			ID:        []byte(id),
			Kind:      IntentKindCreate,
			Phase:     "prepared",
			Parent:    RootInode,
			Name:      []byte(id),
			CreatedAt: svc.now().UnixNano(),
			UpdatedAt: svc.now().UnixNano(),
		}
		key := fskeys.IntentKey(intent.ID)
		elem, err := putElem(key, intent)
		require.NoError(t, err)
		require.NoError(t, svc.dispatchTxn(ctx, svc.store.LastCommitTS(), []*kv.Elem[kv.OP]{elem}, [][]byte{key}))
	}

	stats, err := svc.RecoverIntents(ctx, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.IntentsCleared)
	assertIntentCount(t, ctx, svc, 1)

	stats, err = svc.RecoverIntents(ctx, 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.IntentsCleared)
	assertIntentCount(t, ctx, svc, 0)
}

func assertIntentCount(t *testing.T, ctx context.Context, svc *Service, want int) {
	t.Helper()
	prefix := fskeys.IntentAllPrefix()
	page, err := svc.store.ScanAt(ctx, prefix, prefixEnd(prefix), want+1, svc.store.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, page, want)
}
