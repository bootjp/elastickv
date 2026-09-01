package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// The OCC read-key validation reads the same two namespaces as the store, and
// missing a version here is worse than a stale read: with no commit above
// startTS the transaction commits on a read it should have conflicted with.
// Promotion landing between a live-first pair hides exactly that version.
func TestValidateReadKeysOnShardReadsStagedBeforeLive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1,
			State: distribution.RouteStateActive, StagedVisibilityActive: true,
			MigrationJobID: 9, MinWriteTSExclusive: 100,
		}},
	}))

	rawKey := []byte("k")
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)
	inner := store.NewMVCCStore()
	t.Cleanup(func() { _ = inner.Close() })
	// A staged commit above the transaction's startTS: the read must conflict.
	require.NoError(t, inner.PutAt(ctx, stagedKey, []byte("staged"), 60, 0))

	promoting := &promotingExportStore{MVCCStore: inner}
	promoting.afterFirst = func() {
		promoter, ok := inner.(store.MigrationPromoter)
		require.True(t, ok)
		_, err := promoter.PromoteVersions(ctx, store.PromoteVersionsOptions{
			JobID:       9,
			StartKey:    distribution.MigrationStagedDataKeyPrefix(9),
			EndKey:      prefixScanEnd(distribution.MigrationStagedDataKeyPrefix(9)),
			MaxVersions: 16,
			TargetKey: func(staged []byte) ([]byte, bool) {
				_, raw, ok := distribution.MigrationStagedDataKeyParts(staged)
				return raw, ok
			},
		})
		require.NoError(t, err)
	}

	c := &ShardedCoordinator{
		engine: engine,
		groups: map[uint64]*ShardGroup{1: {Store: promoting}},
	}

	// latestCommitTSForReadKeyOnShard is the unit under test;
	// validateReadKeysOnShard wraps it in a Raft read barrier that needs a
	// live engine, which this case does not depend on. A missed version here
	// is what makes validateReadKeysOnShard let the transaction through.
	ts, exists, err := c.latestCommitTSForReadKeyOnShard(ctx, 1, c.groups[1], rawKey)
	require.NoError(t, err)
	require.True(t, exists,
		"a promotion between the probes must not hide the conflicting commit")
	require.Equal(t, uint64(60), ts)
	require.Greater(t, ts, uint64(50), "the read must still conflict with startTS=50")
}

func TestValidateReadKeysOnShardUsesS3BucketAuxiliaryOwnerForStagedProbe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const bucket = "bucket-a"
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  s3BucketAuxiliaryStagedRoutes(bucket, 1, 2),
	}))

	rawKey := s3keys.BucketMetaKey(bucket)
	stagedKey := distribution.MigrationStagedDataKey(9, rawKey)
	ownerStore := store.NewMVCCStore()
	t.Cleanup(func() { _ = ownerStore.Close() })
	require.NoError(t, ownerStore.PutAt(ctx, stagedKey, []byte("staged"), 60, 0))

	c := &ShardedCoordinator{
		engine: engine,
		groups: map[uint64]*ShardGroup{
			1: {Store: store.NewMVCCStore()},
			2: {Store: ownerStore},
		},
	}

	ts, exists, err := c.latestCommitTSForReadKeyOnShard(ctx, 2, c.groups[2], rawKey)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(60), ts)
}
