package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestPendingTxnLocksInRouteFiltersLocksByRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	tableSegment := "tenant-a"
	routeStart := []byte(dynamoRoutePrefix + tableSegment)
	routeEnd := append(bytes.Clone(routeStart), 0xff)
	itemKey := append([]byte(DynamoItemPrefix+tableSegment+"|7|"), []byte("pk\x00\x01")...)
	outsideKey := []byte("outside")

	require.Less(t, bytes.Compare(itemKey, routeStart), 0,
		"the red-control key must sort outside a txnLockKey(routeStart)/txnLockKey(routeEnd) bracket")
	lock := encodeTxnLock(txnLock{
		StartTS:      11,
		TTLExpireAt:  99,
		PrimaryKey:   itemKey,
		IsPrimaryKey: true,
		CommitTS:     101,
	})
	require.NoError(t, st.PutAt(ctx, txnLockKey(itemKey), lock, 1, 0))
	require.NoError(t, st.PutAt(ctx, txnLockKey(outsideKey), encodeTxnLock(txnLock{
		StartTS:    12,
		PrimaryKey: outsideKey,
	}), 2, 0))

	pending, err := PendingTxnLocksInRoute(ctx, st, routeStart, routeEnd, ^uint64(0), 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, itemKey, pending[0].UserKey)
	require.Equal(t, uint64(11), pending[0].StartTS)
	require.Equal(t, uint64(99), pending[0].TTLExpireAt)
	require.Equal(t, uint64(101), pending[0].CommitTS)
	require.True(t, pending[0].IsPrimaryKey)
	require.Equal(t, txnLockKey(itemKey), pending[0].LockKey)
}

func TestPendingTxnLocksInRouteIncludesS3AuxiliaryOwnership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	start := s3keys.RoutePrefixForBucketAnyGeneration("moving-bucket")
	end := append(bytes.Clone(start), 0xff)
	keys := [][]byte{s3keys.BucketMetaKey("moving-bucket"), s3keys.BucketGenerationKey("moving-bucket")}
	for _, key := range append(keys, s3keys.BucketMetaKey("outside-bucket")) {
		require.NoError(t, st.PutAt(ctx, txnLockKey(key), encodeTxnLock(txnLock{StartTS: 11, PrimaryKey: key}), 1, 0))
	}
	for _, key := range keys {
		require.False(t, keyInMigrationRouteRange(routeKey(key), start, end), "raw auxiliary keys must not accidentally satisfy the test")
	}
	pending, err := PendingTxnLocksInRoute(ctx, st, start, end, ^uint64(0), 10)
	require.NoError(t, err)
	var got [][]byte
	for _, entry := range pending {
		got = append(got, entry.UserKey)
	}
	require.ElementsMatch(t, keys, got)
}

func TestPendingTxnLocksInRouteHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pending, err := PendingTxnLocksInRoute(ctx, store.NewMVCCStore(), nil, nil, ^uint64(0), 10)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, pending)
}
