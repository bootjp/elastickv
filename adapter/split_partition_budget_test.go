package adapter

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestSplitPartitionBudgetRejectsBeforeGeometryPublication(t *testing.T) {
	for _, working := range []bool{false, true} {
		name := "metadata"
		if working {
			name = "working-source"
		}
		t.Run(name, func(t *testing.T) {
			r := newResplitAcceptanceRuntime(t)
			r.migrate(t, 1, 2, "m")
			before, err := r.server.catalog.Snapshot(t.Context())
			require.NoError(t, err)
			parent := r.ownership(t, 2)[0].RouteID
			key := append([]byte("n"), bytes.Repeat([]byte("k"), 700<<10)...)
			if working {
				job := distribution.SplitJob{JobID: 100, SourceRouteID: parent, SourceGroupID: 2,
					TargetGroupID: 3, SplitKey: key, Phase: distribution.SplitJobPhasePlanned}
				require.NoError(t, r.server.catalog.CreateSplitJob(t.Context(), job))
				_, err = r.server.ensureSplitWorkingSource(t.Context(), job)
				stored, found, loadErr := r.server.catalog.SplitJob(t.Context(), job.JobID)
				require.NoError(t, loadErr)
				require.True(t, found)
				require.Zero(t, stored.WorkingSourceRouteID)
			} else {
				_, err = r.client.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: parent,
					SplitKey: key, ExpectedCatalogVersion: before.Version})
			}
			require.Error(t, err, "the manifest fits, but the full partition control does not")
			after, err := r.server.catalog.Snapshot(t.Context())
			require.NoError(t, err)
			require.Equal(t, before.Version, after.Version)
			require.Equal(t, before.Routes, after.Routes)
			require.NoError(t, r.put(t, 2, "n", "still-writable", 1<<40))
		})
	}
}

func TestSplitRangeWithoutAuthCannotIgnoreDurableOwnership(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	r.migrate(t, 1, 2, "m")
	before, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	parent := r.ownership(t, 2)[0].RouteID
	WithMigrationOwnershipRefresh(r.groups, nil)(r.server)
	require.False(t, r.server.MigrationOwnershipRefreshConfigured(2))
	_, err = r.client.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: parent,
		SplitKey: []byte("t"), ExpectedCatalogVersion: before.Version})
	require.Error(t, err)
	after, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Version, after.Version)
	require.Equal(t, before.Routes, after.Routes)
}

func TestSplitRangeCannotTreatMissingOwnerStoreAsProofFree(t *testing.T) {
	t.Parallel()
	server, version := seededDistributionServer(t)
	before, err := server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	WithMigrationOwnershipRefresh(map[uint64]*kv.ShardGroup{}, nil)(server)
	_, err = server.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: 1,
		SplitKey: []byte("g"), ExpectedCatalogVersion: version})
	require.Error(t, err, "unknown ownership is not an empty ownership frontier")
	after, err := server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Version, after.Version)
	require.Equal(t, before.Routes, after.Routes)
}
