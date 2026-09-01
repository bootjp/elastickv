package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestBackupScannerRoutesS3BucketAuxiliaryThroughSelectedOwner(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		key  []byte
	}{
		{name: "bucket metadata", key: s3keys.BucketMetaKey("bucket-a")},
		{name: "bucket generation", key: s3keys.BucketGenerationKey("bucket-a")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			st := newBackupScannerS3AuxiliaryStore(t)

			raw := st.groups[1]
			owner := st.groups[2]
			require.NoError(t, raw.Store.PutAt(ctx, tc.key, []byte("stale-raw"), 10, 0))
			require.NoError(t, owner.Store.PutAt(ctx, tc.key, []byte("owner"), 20, 0))
			require.NoError(t, raw.Store.PutAt(ctx, s3keys.BucketMetaKey("bucket-z"), []byte("raw-owned"), 15, 0))

			scanner := NewBackupScannerAtSnapshot(st, st.CaptureBackupRouteSnapshot(nil, nil), 30, 10)
			got := collectBackupScannerValues(t, scanner)

			require.Len(t, got, 2)
			require.True(t, bytes.Equal(tc.key, got[0].Key), "expected selected owner key first, got %q", got[0].Key)
			require.Equal(t, []byte("owner"), got[0].Value)
			require.Equal(t, s3keys.BucketMetaKey("bucket-z"), got[1].Key)
			require.Equal(t, []byte("raw-owned"), got[1].Value)
		})
	}
}

func TestBackupScannerDropsStaleS3BucketAuxiliaryAfterOwnerTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := s3keys.BucketMetaKey("bucket-a")
	st := newBackupScannerS3AuxiliaryStore(t)

	raw := st.groups[1]
	owner := st.groups[2]
	require.NoError(t, raw.Store.PutAt(ctx, key, []byte("stale-raw"), 10, 0))
	require.NoError(t, owner.Store.DeleteAt(ctx, key, 20))
	require.NoError(t, raw.Store.PutAt(ctx, s3keys.BucketMetaKey("bucket-z"), []byte("raw-owned"), 15, 0))

	scanner := NewBackupScannerAtSnapshot(st, st.CaptureBackupRouteSnapshot(nil, nil), 30, 10)
	got := collectBackupScannerValues(t, scanner)

	require.Len(t, got, 1)
	require.Equal(t, s3keys.BucketMetaKey("bucket-z"), got[0].Key)
	require.Equal(t, []byte("raw-owned"), got[0].Value)
}

func newBackupScannerS3AuxiliaryStore(t *testing.T) *ShardStore {
	t.Helper()
	engine := distribution.NewEngine()
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  s3BucketAuxiliaryPromotedRoutes(),
	}))
	return NewShardStore(engine, groups)
}

func collectBackupScannerValues(t *testing.T, scanner BackupScanner) []*store.KVPair {
	t.Helper()
	ctx := context.Background()
	var got []*store.KVPair
	for {
		pair, ok, err := scanner.Next(ctx)
		require.NoError(t, err)
		if !ok {
			return got
		}
		got = append(got, pair)
	}
}
