package serverwiring

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type splitJobCatalogCoordinator struct {
	kv.Coordinator
	store store.MVCCStore
}

func (c splitJobCatalogCoordinator) IsLeaderForKey([]byte) bool { return true }

func (c splitJobCatalogCoordinator) VerifyLeaderForKey(context.Context, []byte) error { return nil }

func (c splitJobCatalogCoordinator) Dispatch(ctx context.Context, ops *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	commitTS := c.store.LastCommitTS() + 1
	if commitTS <= ops.StartTS {
		commitTS = ops.StartTS + 1
	}
	mutations := make([]*store.KVPairMutation, 0, len(ops.Elems))
	for _, elem := range ops.Elems {
		if elem.Op != kv.Put {
			return nil, kv.ErrInvalidRequest
		}
		mutations = append(mutations, &store.KVPairMutation{
			Op: store.OpTypePut, Key: distribution.CloneBytes(elem.Key), Value: distribution.CloneBytes(elem.Value),
		})
	}
	if err := c.store.ApplyMutations(ctx, mutations, ops.ReadKeys, ops.StartTS, commitTS); err != nil {
		return nil, err
	}
	return &kv.CoordinateResponse{CommitIndex: commitTS, CommitTS: commitTS}, nil
}

func TestDistributionSplitterCrossGroupPersistsPlannedMigration(t *testing.T) {
	ctx := t.Context()
	root := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(root)
	snapshot, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive,
	}})
	require.NoError(t, err)

	server := adapter.NewDistributionServer(distribution.NewEngine(), catalog,
		adapter.WithDistributionCoordinator(splitJobCatalogCoordinator{store: root}),
		adapter.WithDistributionKnownRaftGroups(1, 2),
		adapter.WithSplitMigrationCapabilityGate(func(context.Context) error { return nil }),
	)
	result, err := (DistributionSplitter{Server: server}).SplitRange(ctx, autosplit.SplitRequest{
		ExpectedCatalogVersion: snapshot.Version,
		RouteID:                1,
		SplitKey:               []byte("g"),
		TargetGroupID:          2,
	})
	require.NoError(t, err)
	require.NotZero(t, result.JobID)
	require.Equal(t, snapshot.Version, result.CatalogVersion)

	job, found, err := distribution.NewCatalogStore(root).SplitJob(ctx, result.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, result.JobID, job.JobID)
	require.Equal(t, uint64(1), job.SourceRouteID)
	require.Equal(t, uint64(1), job.SourceGroupID)
	require.Equal(t, uint64(2), job.TargetGroupID)
	require.Equal(t, []byte("g"), job.SplitKey)
	require.Equal(t, distribution.SplitJobPhasePlanned, job.Phase)
}
