package adapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestRawHandoffSurvivesPromotionDescriptorClear(t *testing.T) {
	for _, retired := range []bool{false, true} {
		t.Run(map[bool]string{false: "descriptor cleared", true: "target guard retired"}[retired], func(t *testing.T) {
			testRawHandoffAfterPromotion(t, retired)
		})
	}
}

func testRawHandoffAfterPromotion(t *testing.T, targetRetired bool) {
	t.Helper()
	r := newResplitAcceptanceRuntime(t)
	checked := false
	r.migrate(t, 1, 2, "m", func(job distribution.SplitJob) {
		if checked || job.Phase != distribution.SplitJobPhaseCleanup || !job.TargetPromotionDone {
			return
		}
		if targetRetired && !bytes.Equal(job.TargetClearedDescriptorAckCursor, splitCleanupDoneCursor) {
			return
		}
		checked = true
		route, found := r.server.engine.GetRoute([]byte("n"))
		require.True(t, found)
		require.False(t, route.StagedVisibilityActive)
		require.Zero(t, route.MigrationJobID)
		readiness, ok := r.groups[2].Store.(store.MigrationTargetReadinessReader)
		require.True(t, ok)
		states, err := readiness.MigrationTargetReadinessStates(t.Context())
		require.NoError(t, err)
		require.Equal(t, !targetRetired, anyArmedHandoffState(states, job.JobID), "exercise both sides of target-guard retirement")
		source, err := store.ReadOnlineMigrationSession(t.Context(), r.groups[1].Store, job.JobID)
		require.NoError(t, err)
		require.True(t, source.Sealed)
		require.False(t, source.Closed, "the old source still guards the transfer interval")
		require.False(t, r.reader.OnlineMigrationHandoffActive(t.Context(), []byte("b")), "an unrelated key must not inherit the source hint")

		// The first attempt already resolved the old fenced route before the
		// descriptor was cleared. Its proof must remain retryable while the
		// old source guard still exists, even though the new route is bare.
		fenced := distribution.NewEngine()
		require.NoError(t, fenced.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateWriteFenced}}}))
		stale := kv.NewShardedCoordinator(fenced, nil, 1, kv.NewHLC(), nil)
		current := kv.NewShardedCoordinator(r.server.engine, r.groups, 1, kv.NewHLC(), nil)
		calls := 0
		server := NewGRPCServer(r.reader, &promotionHandoffCoordinator{dispatch: func(ctx context.Context, request *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
			calls++
			if calls == 1 {
				response, failure := stale.Dispatch(ctx, request)
				require.True(t, kv.IsMutationNotAdmitted(failure))
				return response, failure
			}
			if calls == 2 && !targetRetired {
				require.NoError(t, r.server.RunSplitJobRunnerOnce(ctx), "the independent runner retires the target guard while ingress waits")
			}
			return current.Dispatch(ctx, request)
		}})
		response, err := server.RawPut(t.Context(), &pb.RawPutRequest{Key: []byte("n"), Value: []byte("v")})
		require.NoError(t, err, "a proven old-route rejection must wait through target-guard retirement")
		require.True(t, response.GetSuccess())
		require.Equal(t, 2, calls)
		r.read(t, "n", "v")
	})
	require.True(t, checked)
	require.False(t, r.reader.OnlineMigrationHandoffActive(t.Context(), []byte("n")), "retired jobs must not keep an error-path hint alive")
}

type promotionHandoffCoordinator struct {
	kv.Coordinator
	dispatch func(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error)
}

func (c *promotionHandoffCoordinator) Dispatch(ctx context.Context, request *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return c.dispatch(ctx, request)
}

func anyArmedHandoffState(states []store.TargetStagedReadinessState, job uint64) bool {
	for _, state := range states {
		if state.JobID == job && state.Armed && !state.TrackWrites {
			return true
		}
	}
	return false
}
