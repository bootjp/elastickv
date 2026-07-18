package adapter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	crdberrors "github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDistributionServerGetRoute_HitAndMiss(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s := NewDistributionServer(engine, nil)
	ctx := context.Background()

	hit, err := s.GetRoute(ctx, &pb.GetRouteRequest{Key: []byte("b")})
	require.NoError(t, err)
	require.Equal(t, []byte("a"), hit.Start)
	require.Equal(t, []byte("m"), hit.End)
	require.Equal(t, uint64(1), hit.RaftGroupId)

	miss, err := s.GetRoute(ctx, &pb.GetRouteRequest{Key: []byte("0")})
	require.NoError(t, err)
	require.Equal(t, uint64(0), miss.RaftGroupId)
	require.Nil(t, miss.Start)
	require.Nil(t, miss.End)
}

func TestDistributionServerRouteReadsHonorStartupGate(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte("a"), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	_, err := catalog.Save(context.Background(), 0, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte("a"), End: nil, GroupID: 1, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)

	blocked := true
	s := NewDistributionServer(engine, catalog, WithDistributionReadGate(func() bool { return blocked }))

	_, err = s.GetRoute(context.Background(), &pb.GetRouteRequest{Key: []byte("a")})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))

	_, err = s.ListRoutes(context.Background(), &pb.ListRoutesRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))

	_, err = s.GetRouteOwnership(context.Background(), &pb.GetRouteOwnershipRequest{
		Key:            []byte("a"),
		CatalogVersion: engine.Version(),
	})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))

	_, err = s.GetIntersectingRoutes(context.Background(), &pb.GetIntersectingRoutesRequest{
		Start:          []byte("a"),
		End:            []byte("z"),
		CatalogVersion: engine.Version(),
	})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))

	blocked = false
	_, err = s.GetRoute(context.Background(), &pb.GetRouteRequest{Key: []byte("a")})
	require.NoError(t, err)
	_, err = s.ListRoutes(context.Background(), &pb.ListRoutesRequest{})
	require.NoError(t, err)
	_, err = s.GetRouteOwnership(context.Background(), &pb.GetRouteOwnershipRequest{
		Key:            []byte("a"),
		CatalogVersion: engine.Version(),
	})
	require.NoError(t, err)
	_, err = s.GetIntersectingRoutes(context.Background(), &pb.GetIntersectingRoutesRequest{
		Start:          []byte("a"),
		End:            []byte("z"),
		CatalogVersion: engine.Version(),
	})
	require.NoError(t, err)
}

func TestDistributionServerGetTimestamp_IsMonotonic(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), nil)
	ctx := context.Background()

	first, err := s.GetTimestamp(ctx, &pb.GetTimestampRequest{})
	require.NoError(t, err)

	second, err := s.GetTimestamp(ctx, &pb.GetTimestampRequest{})
	require.NoError(t, err)

	require.Greater(t, second.Timestamp, first.Timestamp)
}

func TestNewDistributionServer_DefaultCatalogReloadRetryPolicy(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), distribution.NewCatalogStore(store.NewMVCCStore()))
	require.Equal(t, defaultCatalogReloadRetryAttempts, s.reloadRetry.attempts)
	require.Equal(t, defaultCatalogReloadRetryInterval, s.reloadRetry.interval)
}

func TestWithCatalogReloadRetryPolicy_OverridesDefaults(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(
		distribution.NewEngine(),
		distribution.NewCatalogStore(store.NewMVCCStore()),
		WithCatalogReloadRetryPolicy(3, 5*time.Millisecond),
	)
	require.Equal(t, 3, s.reloadRetry.attempts)
	require.Equal(t, 5*time.Millisecond, s.reloadRetry.interval)
}

func TestDistributionServerPinReadTSWithoutTrackerReturnsReleasableToken(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), nil)
	token := s.pinReadTS(10)
	require.NotNil(t, token)
	require.NotPanics(t, func() {
		token.Release()
	})

	var nilServer *DistributionServer
	nilToken := nilServer.pinReadTS(10)
	require.NotNil(t, nilToken)
	require.NotPanics(t, func() {
		nilToken.Release()
	})
}

func TestDistributionServerListRoutes_ReadsDurableCatalog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore(), distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:                2,
			Start:                  []byte("m"),
			End:                    nil,
			GroupID:                2,
			State:                  distribution.RouteStateWriteFenced,
			ParentRouteID:          1,
			StagedVisibilityActive: true,
			MigrationJobID:         42,
			MinWriteTSExclusive:    99,
		},
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	s := NewDistributionServer(distribution.NewEngine(), catalog)
	resp, err := s.ListRoutes(ctx, &pb.ListRoutesRequest{})
	require.NoError(t, err)

	require.Equal(t, saved.Version, resp.CatalogVersion)
	require.Len(t, resp.Routes, 2)
	require.Equal(t, uint64(1), resp.Routes[0].RouteId)
	require.Equal(t, []byte(""), resp.Routes[0].Start)
	require.Equal(t, []byte("m"), resp.Routes[0].End)
	require.Equal(t, uint64(1), resp.Routes[0].RaftGroupId)
	require.Equal(t, pb.RouteState_ROUTE_STATE_ACTIVE, resp.Routes[0].State)
	require.Equal(t, uint64(2), resp.Routes[1].RouteId)
	require.Nil(t, resp.Routes[1].End)
	require.Equal(t, pb.RouteState_ROUTE_STATE_WRITE_FENCED, resp.Routes[1].State)
	require.True(t, resp.Routes[1].StagedVisibilityActive)
	require.Equal(t, uint64(42), resp.Routes[1].MigrationJobId)
	require.Equal(t, uint64(99), resp.Routes[1].MinWriteTsExclusive)
}

func TestDistributionServerListRoutes_RequiresCatalog(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), nil)
	_, err := s.ListRoutes(context.Background(), &pb.ListRoutesRequest{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionCatalogNotConfigured.Error())
}

func TestDistributionServerGetSplitMigrationCapabilityReportsNotReadyUntilRunnerReady(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), nil)
	resp, err := s.GetSplitMigrationCapability(context.Background(), &pb.GetSplitMigrationCapabilityRequest{})
	require.NoError(t, err)
	require.False(t, resp.GetMigrationCapable())
	require.NotContains(t, resp.GetCapabilities(), splitMigrationCapabilityV2)
}

func TestDistributionServerGetSplitMigrationCapabilityReportsReadyWhenRunnerReady(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(distribution.NewEngine(), nil, WithSplitJobRunnerReady())
	resp, err := s.GetSplitMigrationCapability(context.Background(), &pb.GetSplitMigrationCapabilityRequest{})
	require.NoError(t, err)
	require.True(t, resp.GetMigrationCapable())
	require.Contains(t, resp.GetCapabilities(), splitMigrationCapabilityV2)
}

func TestDistributionServerGetSplitMigrationCapabilityRespectsReadinessGate(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(
		distribution.NewEngine(),
		nil,
		WithSplitJobRunnerReady(),
		WithSplitJobRunnerReadinessGate(func(context.Context) error {
			return status.Error(codes.FailedPrecondition, "migration opcode disabled")
		}),
	)
	resp, err := s.GetSplitMigrationCapability(context.Background(), &pb.GetSplitMigrationCapabilityRequest{})
	require.NoError(t, err)
	require.False(t, resp.GetMigrationCapable())
	require.NotContains(t, resp.GetCapabilities(), splitMigrationCapabilityV2)
}

func TestDistributionServerStartSplitMigration_FailsClosedUntilCapabilityGate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))

	_, err := s.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: 1,
		RouteId:                1,
		SplitKey:               []byte("g"),
		TargetGroupId:          2,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionClusterNotReady.Error())
	require.Zero(t, coordinator.dispatchCalls)

	jobs, listErr := catalog.ListSplitJobs(ctx)
	require.NoError(t, listErr)
	require.Empty(t, jobs)
}

func TestDistributionServerStartSplitMigrationReturnsCapabilityGateError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithSplitMigrationCapabilityGate(func(context.Context) error {
			return status.Error(codes.Unavailable, "split migration capability not ready")
		}),
	)

	_, err := s.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: 1,
		RouteId:                1,
		SplitKey:               []byte("g"),
		TargetGroupId:          2,
	})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "split migration capability not ready")
	require.Zero(t, coordinator.dispatchCalls)
}

func TestDistributionServerStartSplitMigrationCapabilityGateRunsOutsideCatalogLock(t *testing.T) {
	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID:       1,
		Start:         []byte("a"),
		End:           []byte("m"),
		GroupID:       1,
		State:         distribution.RouteStateActive,
		ParentRouteID: 0,
	}})
	require.NoError(t, err)

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	gateEntered := make(chan struct{})
	releaseGate := make(chan struct{})
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithDistributionKnownRaftGroups(1, 2),
		WithSplitMigrationCapabilityGate(func(context.Context) error {
			close(gateEntered)
			<-releaseGate
			return status.Error(codes.Unavailable, "split migration capability not ready")
		}),
	)

	startDone := make(chan error, 1)
	go func() {
		_, err := s.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
			ExpectedCatalogVersion: saved.Version,
			RouteId:                1,
			SplitKey:               []byte("g"),
			TargetGroupId:          2,
		})
		startDone <- err
	}()
	<-gateEntered

	splitDone := make(chan error, 1)
	go func() {
		_, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
			ExpectedCatalogVersion: saved.Version,
			RouteId:                1,
			SplitKey:               []byte("g"),
		})
		splitDone <- err
	}()
	select {
	case err := <-splitDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SplitRange blocked behind StartSplitMigration capability gate")
	}

	close(releaseGate)
	err = <-startDone
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestDistributionServerStartSplitMigration_CreatesPlannedJobWhenGateOpen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID:       1,
		Start:         []byte("a"),
		End:           []byte("m"),
		GroupID:       1,
		State:         distribution.RouteStateActive,
		ParentRouteID: 0,
	}})
	require.NoError(t, err)

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	gateCalls := 0
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithDistributionKnownRaftGroups(1, 2),
		WithSplitMigrationCapabilityGate(func(context.Context) error {
			gateCalls++
			return nil
		}),
	)

	resp, err := s.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
		TargetGroupId:          2,
	})
	require.NoError(t, err)
	require.Equal(t, saved.Version, resp.CatalogVersion)
	require.Equal(t, uint64(1), resp.JobId)
	require.Equal(t, 1, gateCalls)
	require.Equal(t, 1, coordinator.dispatchCalls)
	require.ElementsMatch(t, []string{
		string(distribution.CatalogSplitJobKey(1)),
		string(distribution.CatalogNextSplitJobIDKey()),
		string(distribution.CatalogVersionKey()),
		string(distribution.CatalogRouteKey(1)),
	}, byteSliceStrings(coordinator.lastReadKeys))

	jobs, err := catalog.ListSplitJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	job := jobs[0]
	require.Equal(t, uint64(1), job.JobID)
	require.Equal(t, uint64(1), job.SourceRouteID)
	require.Equal(t, []byte("g"), job.SplitKey)
	require.Equal(t, uint64(2), job.TargetGroupID)
	require.Equal(t, distribution.SplitJobPhasePlanned, job.Phase)
	require.NotZero(t, job.StartedAtMs)
	require.NotZero(t, job.UpdatedAtMs)
	require.NotEmpty(t, job.BracketProgress)
	next, err := catalog.NextSplitJobID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), next)
}

func TestDistributionServerStartSplitMigration_RejectsUnknownTargetGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID:       1,
		Start:         []byte("a"),
		End:           []byte("m"),
		GroupID:       1,
		State:         distribution.RouteStateActive,
		ParentRouteID: 0,
	}})
	require.NoError(t, err)

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithDistributionKnownRaftGroups(1, 2),
		WithSplitMigrationCapabilityGate(func(context.Context) error { return nil }),
	)

	_, err = s.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
		TargetGroupId:          3,
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, errDistributionUnknownTargetGroup.Error())
	require.Zero(t, coordinator.dispatchCalls)
}

func TestDistributionServerStartSplitMigration_RejectsNonActiveSourceRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		state distribution.RouteState
	}{
		{name: "write_fenced", state: distribution.RouteStateWriteFenced},
		{name: "migrating_source", state: distribution.RouteStateMigratingSource},
		{name: "migrating_target", state: distribution.RouteStateMigratingTarget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseStore := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(baseStore)
			saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
				RouteID:       1,
				Start:         []byte("a"),
				End:           []byte("m"),
				GroupID:       1,
				State:         tc.state,
				ParentRouteID: 0,
			}})
			require.NoError(t, err)

			coordinator := newDistributionCoordinatorStub(baseStore, true)
			s := NewDistributionServer(
				distribution.NewEngine(),
				catalog,
				WithDistributionCoordinator(coordinator),
				WithDistributionKnownRaftGroups(1, 2),
				WithSplitMigrationCapabilityGate(func(context.Context) error { return nil }),
			)

			_, err = s.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
				ExpectedCatalogVersion: saved.Version,
				RouteId:                1,
				SplitKey:               []byte("g"),
				TargetGroupId:          2,
			})
			require.Error(t, err)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.ErrorContains(t, err, errDistributionSourceRouteNotActive.Error())
			require.Zero(t, coordinator.dispatchCalls)

			jobs, listErr := catalog.ListSplitJobs(ctx)
			require.NoError(t, listErr)
			require.Empty(t, jobs)
		})
	}
}

func TestDistributionServerStartSplitMigration_RejectsSecondLiveJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte("a"),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)
	require.NoError(t, catalog.CreateSplitJob(ctx, distribution.SplitJob{
		JobID:         1,
		SourceRouteID: 2,
		SplitKey:      []byte("t"),
		TargetGroupID: 3,
		Phase:         distribution.SplitJobPhaseBackfill,
		StartedAtMs:   1000,
		UpdatedAtMs:   1000,
	}))

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithDistributionKnownRaftGroups(1, 2, 3, 4),
		WithSplitMigrationCapabilityGate(func(context.Context) error { return nil }),
	)

	_, err = s.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
		TargetGroupId:          4,
	})
	require.Error(t, err)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.ErrorContains(t, err, distribution.ErrTooManyInFlightSplitJobs.Error())
	require.Zero(t, coordinator.dispatchCalls)
}

func TestDistributionServerStartSplitMigration_RejectsReservedMigrationRange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		splitKey []byte
		end      []byte
	}{
		{name: "dist catalog", splitKey: []byte("!dist|"), end: nil},
		{name: "dist staged catalog", splitKey: []byte("!dist|migstage|"), end: nil},
		{name: "migration staged", splitKey: []byte("!migstage|ready|1"), end: nil},
		{name: "migration tracker", splitKey: []byte("!migwrite|"), end: nil},
		{name: "migration fence", splitKey: []byte("!migfence|"), end: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			baseStore := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(baseStore)
			saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
				RouteID:       1,
				Start:         []byte(""),
				End:           tc.end,
				GroupID:       1,
				State:         distribution.RouteStateActive,
				ParentRouteID: 0,
			}})
			require.NoError(t, err)

			coordinator := newDistributionCoordinatorStub(baseStore, true)
			s := NewDistributionServer(
				distribution.NewEngine(),
				catalog,
				WithDistributionCoordinator(coordinator),
				WithDistributionKnownRaftGroups(1, 2),
				WithSplitMigrationCapabilityGate(func(context.Context) error { return nil }),
			)

			_, err = s.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
				ExpectedCatalogVersion: saved.Version,
				RouteId:                1,
				SplitKey:               tc.splitKey,
				TargetGroupId:          2,
			})
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.ErrorContains(t, err, distribution.ErrMigrationReservedRange.Error())
			require.Zero(t, coordinator.dispatchCalls)

			jobs, listErr := catalog.ListSplitJobs(ctx)
			require.NoError(t, listErr)
			require.Empty(t, jobs)
		})
	}
}

func TestDistributionServerSplitJobRPCs_ReadAndListCatalogJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	live := sampleDistributionSplitJob(10)
	live.Phase = distribution.SplitJobPhaseDeltaCopy
	require.NoError(t, catalog.CreateSplitJob(ctx, live))
	history := sampleDistributionSplitJob(11)
	history.Phase = distribution.SplitJobPhaseDone
	history.TerminalAtMs = 2000
	require.NoError(t, catalog.CreateSplitJob(ctx, history))
	require.NoError(t, catalog.MoveSplitJobToHistory(ctx, history, history))

	s := NewDistributionServer(distribution.NewEngine(), catalog)
	got, err := s.GetSplitJob(ctx, &pb.GetSplitJobRequest{JobId: live.JobID})
	require.NoError(t, err)
	require.Equal(t, live.JobID, got.Job.JobId)
	require.Equal(t, pb.SplitJobPhase_SPLIT_JOB_PHASE_DELTA_COPY, got.Job.Phase)

	listAll, err := s.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{})
	require.NoError(t, err)
	require.Len(t, listAll.Jobs, 2)
	require.Equal(t, []uint64{live.JobID, history.JobID}, []uint64{listAll.Jobs[0].JobId, listAll.Jobs[1].JobId})

	listDone, err := s.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{Phase: "done"})
	require.NoError(t, err)
	require.Len(t, listDone.Jobs, 1)
	require.Equal(t, history.JobID, listDone.Jobs[0].JobId)

	_, err = s.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{Phase: "not-a-phase"})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDistributionServerListSplitJobs_PaginatesNewestHistory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	for jobID, terminalAtMs := uint64(1), int64(1001); jobID <= 205; jobID, terminalAtMs = jobID+1, terminalAtMs+1 {
		job := sampleDistributionSplitJob(jobID)
		job.Phase = distribution.SplitJobPhaseDone
		job.TerminalAtMs = terminalAtMs
		job.UpdatedAtMs = job.TerminalAtMs
		require.NoError(t, catalog.CreateSplitJob(ctx, job))
		require.NoError(t, catalog.MoveSplitJobToHistory(ctx, job, job))
	}

	s := NewDistributionServer(distribution.NewEngine(), catalog)
	first, err := s.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{})
	require.NoError(t, err)
	require.Len(t, first.Jobs, listSplitJobsDefaultPageSize)
	require.NotEmpty(t, first.NextPageCursor)
	require.Equal(t, uint64(205), first.Jobs[0].JobId)
	require.Equal(t, uint64(6), first.Jobs[len(first.Jobs)-1].JobId)

	second, err := s.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{PageCursor: first.NextPageCursor})
	require.NoError(t, err)
	require.Empty(t, second.NextPageCursor)
	require.Equal(t, []uint64{5, 4, 3, 2, 1}, splitJobIDs(second.Jobs))
}

func TestDistributionServerListSplitJobs_RejectsInvalidCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	job := sampleDistributionSplitJob(1)
	job.Phase = distribution.SplitJobPhaseDone
	job.TerminalAtMs = 1000
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	require.NoError(t, catalog.MoveSplitJobToHistory(ctx, job, job))

	s := NewDistributionServer(distribution.NewEngine(), catalog)
	_, err := s.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{PageCursor: []byte("bad")})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	missing := sampleDistributionSplitJob(999)
	missing.TerminalAtMs = 9999
	_, err = s.ListSplitJobs(ctx, &pb.ListSplitJobsRequest{PageCursor: encodeSplitJobListCursor(missing)})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDistributionServerRetrySplitJob_UsesCoordinatorCAS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	job := sampleDistributionSplitJob(12)
	job.Phase = distribution.SplitJobPhaseFailed
	job.RetryPhase = distribution.SplitJobPhaseFence
	job.LastError = "retry me"
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
	_, err := s.RetrySplitJob(ctx, &pb.RetrySplitJobRequest{JobId: job.JobID})
	require.NoError(t, err)
	require.Equal(t, 1, coordinator.dispatchCalls)

	got, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseFence, got.Phase)
	require.Equal(t, distribution.SplitJobPhaseNone, got.RetryPhase)
	require.Empty(t, got.LastError)
}

func TestDistributionServerRetrySplitJob_MapsDispatchLeadershipLoss(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "leader not found", err: kv.ErrLeaderNotFound},
		{name: "raft not leader", err: raftengine.ErrNotLeader},
		{name: "leadership lost", err: raftengine.ErrLeadershipLost},
		{name: "transfer in progress", err: raftengine.ErrLeadershipTransferInProgress},
		{name: "wrapped grpc detail", err: errors.New("rpc error: code = Unknown desc = raft engine: leadership lost")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			baseStore := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(baseStore)
			job := sampleDistributionSplitJob(15)
			job.Phase = distribution.SplitJobPhaseFailed
			job.RetryPhase = distribution.SplitJobPhaseFence
			require.NoError(t, catalog.CreateSplitJob(ctx, job))

			coordinator := newDistributionCoordinatorStub(baseStore, true)
			coordinator.dispatchErr = tc.err
			s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
			_, err := s.RetrySplitJob(ctx, &pb.RetrySplitJobRequest{JobId: job.JobID})
			require.Error(t, err)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.ErrorContains(t, err, errDistributionNotLeader.Error())
			require.Equal(t, 1, coordinator.dispatchCalls)
		})
	}
}

func TestDistributionServerAbandonSplitJob_RecordsAbandoningViaCoordinator(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	job := sampleDistributionSplitJob(13)
	job.Phase = distribution.SplitJobPhaseFailed
	job.RetryPhase = distribution.SplitJobPhaseBackfill
	job.AbandonFromPhase = distribution.SplitJobPhaseNone
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
	_, err := s.AbandonSplitJob(ctx, &pb.AbandonSplitJobRequest{JobId: job.JobID})
	require.NoError(t, err)
	require.Equal(t, 1, coordinator.dispatchCalls)

	got, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseAbandoning, got.Phase)
	require.Equal(t, distribution.SplitJobPhaseNone, got.RetryPhase)
	require.Equal(t, distribution.SplitJobPhaseBackfill, got.AbandonFromPhase)
}

func TestDistributionServerCompleteSplitJobTargetPromotion_UsesCoordinatorCAS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, promotionCompleteDistributionRoutes())
	require.NoError(t, err)
	job := promotionCompleteDistributionJob()
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	before, err := catalog.Snapshot(ctx)
	require.NoError(t, err)

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	coordinator.timestampNext = before.ReadTS + 10
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))

	snapshot, completed, err := s.completeSplitJobTargetPromotionViaCoordinator(ctx, saved.Version, job, 2000)
	require.NoError(t, err)
	require.Equal(t, saved.Version+1, snapshot.Version)
	require.True(t, completed.TargetPromotionDone)
	require.Equal(t, distribution.SplitJobPhaseCleanup, completed.Phase)
	require.Equal(t, before.ReadTS+10, completed.PromotionCompletedTS)
	require.Equal(t, int64(2000), completed.UpdatedAtMs)
	require.Zero(t, completed.TerminalAtMs)
	require.Equal(t, 1, coordinator.dispatchCalls)
	require.Equal(t, 1, coordinator.timestampCalls)
	require.Equal(t, before.ReadTS, coordinator.lastStartTS)
	require.Equal(t, completed.PromotionCompletedTS, coordinator.lastCommitTS)
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogVersionKey())
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogSplitJobKey(job.JobID))

	loadedJob, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, completed, loadedJob)

	loaded, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	target := distributionRouteByID(t, loaded.Routes, 3)
	require.False(t, target.StagedVisibilityActive)
	require.Equal(t, uint64(0), target.MigrationJobID)
	require.Equal(t, uint64(777), target.MinWriteTSExclusive)
}

func TestDistributionServerCompleteSplitJobTargetPromotion_RetryAfterCommitIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, promotionCompleteDistributionRoutes())
	require.NoError(t, err)
	job := promotionCompleteDistributionJob()
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))

	firstSnapshot, firstCompleted, err := s.completeSplitJobTargetPromotionViaCoordinator(ctx, saved.Version, job, 2000)
	require.NoError(t, err)
	retrySnapshot, retryCompleted, err := s.completeSplitJobTargetPromotionViaCoordinator(ctx, saved.Version, job, 3000)
	require.NoError(t, err)
	require.Equal(t, firstSnapshot.Version, retrySnapshot.Version)
	require.Equal(t, firstCompleted, retryCompleted)
	require.Equal(t, 1, coordinator.dispatchCalls)
}

func TestDistributionServerRunSplitJobRunnerOnce_PromotesCleanupJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, promotionCompleteDistributionRoutes())
	require.NoError(t, err)
	job := promotionCompleteDistributionJob()
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	client := &splitPromotionClientStub{
		responses: []*pb.PromoteStagedVersionsResponse{
			{NextCursor: []byte("cursor-1")},
			{Done: true},
		},
	}
	sourceMigration := &splitMigrationClientStub{}
	targetMigration := &splitMigrationClientStub{}
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			return client, nil
		}),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return sourceMigration, targetMigration, nil
		}),
	)

	require.NoError(t, s.RunSplitJobRunnerOnce(ctx))
	require.Equal(t, 2, client.calls)
	require.Equal(t, job.JobID, client.requests[0].GetJobId())
	require.Empty(t, client.requests[0].GetCursor())
	require.Equal(t, defaultSplitPromotionMaxVersions, client.requests[0].GetMaxVersions())
	require.Equal(t, defaultSplitPromotionMaxBytes, client.requests[0].GetMaxBytes())
	require.Equal(t, defaultSplitPromotionMaxScanned, client.requests[0].GetMaxScannedBytes())
	require.Equal(t, []byte("cursor-1"), client.requests[1].GetCursor())
	require.Equal(t, 1, coordinator.dispatchCalls)

	loadedJob, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, loadedJob.TargetPromotionDone)
	require.Equal(t, distribution.SplitJobPhaseCleanup, loadedJob.Phase)
	require.NotZero(t, loadedJob.PromotionCompletedTS)
	require.Zero(t, loadedJob.TerminalAtMs)

	loaded, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, saved.Version+1, loaded.Version)
	target := distributionRouteByID(t, loaded.Routes, 3)
	require.False(t, target.StagedVisibilityActive)
	require.Equal(t, uint64(0), target.MigrationJobID)
	require.Equal(t, uint64(777), target.MinWriteTSExclusive)

	route, ok := s.engine.GetRoute([]byte("z"))
	require.True(t, ok)
	require.False(t, route.StagedVisibilityActive)
	require.Equal(t, uint64(0), route.MigrationJobID)
	require.Equal(t, saved.Version+1, s.engine.Version())
}

func TestDistributionServerRunSplitJobRunnerOnce_TerminalizesPromotedCleanupJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	routes := promotionCompleteDistributionRoutes()
	routes[1].StagedVisibilityActive = false
	routes[1].MigrationJobID = 0
	saved, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	job := promotionCompleteDistributionJob()
	job.TargetPromotionDone = true
	job.PromotionCompletedTS = saved.ReadTS + 1
	job.UpdatedAtMs = 2000
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	source := &splitMigrationClientStub{}
	target := &splitMigrationClientStub{}
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			return target, nil
		}),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return source, target, nil
		}),
	)

	for range 200 {
		require.NoError(t, s.RunSplitJobRunnerOnce(ctx))
		current, found, loadErr := catalog.SplitJob(ctx, job.JobID)
		require.NoError(t, loadErr)
		require.True(t, found)
		if current.Phase == distribution.SplitJobPhaseDone {
			break
		}
	}
	require.Zero(t, target.calls)

	loadedJob, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseDone, loadedJob.Phase)
	require.True(t, loadedJob.TargetPromotionDone)
	require.Equal(t, job.PromotionCompletedTS, loadedJob.PromotionCompletedTS)

	loaded, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, saved.Version, loaded.Version)
}

func TestDistributionServerRunSplitJobRunnerOnce_CompletesCrossGroupMigration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	require.NoError(t, err)
	job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{
		JobID:         1,
		SourceRouteID: 1,
		SplitKey:      []byte("m"),
		TargetGroupID: 2,
	}, saved.Routes[0], time.Now().UnixMilli())
	require.NoError(t, err)
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(saved))
	source := &splitMigrationClientStub{}
	target := &splitMigrationClientStub{}
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(
		engine,
		catalog,
		WithDistributionCoordinator(coordinator),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			return target, nil
		}),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return source, target, nil
		}),
	)

	for range 200 {
		require.NoError(t, s.RunSplitJobRunnerOnce(ctx))
		current, found, loadErr := catalog.SplitJob(ctx, job.JobID)
		require.NoError(t, loadErr)
		require.True(t, found)
		if current.Phase == distribution.SplitJobPhaseDone {
			break
		}
	}

	completed, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseDone, completed.Phase)
	require.True(t, completed.WriteTrackerArmed)
	require.True(t, completed.PostFenceDrainCompleted)
	require.NotZero(t, completed.SnapshotTS)
	require.NotZero(t, completed.FenceTS)
	require.NotZero(t, completed.CutoverVersion)
	require.True(t, completed.TargetPromotionDone)
	require.NotEmpty(t, source.controls)
	require.NotEmpty(t, target.controls)
	require.NotEmpty(t, source.exports)
	require.Len(t, target.imports, len(source.exports))
	require.GreaterOrEqual(t, len(source.events), 2)
	require.Equal(t, []string{"control", "timestamp"}, source.events[:2], "the tracker barrier must precede snapshot timestamp selection")

	finalSnapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Len(t, finalSnapshot.Routes, 2)
	right := distributionRouteByID(t, finalSnapshot.Routes, 3)
	require.Equal(t, uint64(2), right.GroupID)
	require.Equal(t, distribution.RouteStateActive, right.State)
	require.False(t, right.StagedVisibilityActive)
	require.Equal(t, completed.FenceTS, right.MinWriteTSExclusive)
}

func TestDistributionServerRunSplitJobRunnerOnce_CompletesAbandonCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	require.NoError(t, err)
	job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{
		JobID:         7,
		SourceRouteID: 1,
		SplitKey:      []byte("m"),
		TargetGroupID: 2,
	}, saved.Routes[0], time.Now().UnixMilli())
	require.NoError(t, err)
	job.Phase = distribution.SplitJobPhaseAbandoning
	job.AbandonFromPhase = distribution.SplitJobPhaseBackfill
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	source := &splitMigrationClientStub{}
	target := &splitMigrationClientStub{}
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			return target, nil
		}),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return source, target, nil
		}),
	)

	for range 10 {
		require.NoError(t, s.RunSplitJobRunnerOnce(ctx))
		current, found, loadErr := catalog.SplitJob(ctx, job.JobID)
		require.NoError(t, loadErr)
		require.True(t, found)
		if current.Phase == distribution.SplitJobPhaseAbandoned {
			break
		}
	}
	completed, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseAbandoned, completed.Phase)
	require.Positive(t, completed.TerminalAtMs)
	require.NotEmpty(t, source.cleanups)
	require.NotEmpty(t, target.cleanups)
	require.Equal(t, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS, target.cleanups[0].GetMode())
}

func TestDistributionServerRunSplitJobRunnerOnce_PausesOnCapabilityRegression(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{
		RouteID: 1,
		Start:   []byte("a"),
		End:     []byte("z"),
		GroupID: 1,
		State:   distribution.RouteStateActive,
	}})
	require.NoError(t, err)
	job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{
		JobID:         9,
		SourceRouteID: 1,
		SplitKey:      []byte("m"),
		TargetGroupID: 2,
	}, saved.Routes[0], time.Now().UnixMilli())
	require.NoError(t, err)
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	source := &splitMigrationClientStub{}
	target := &splitMigrationClientStub{}
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	capabilityErr := errors.New("node capability missing")
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithSplitMigrationCapabilityGate(func(context.Context) error { return capabilityErr }),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			return target, nil
		}),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return source, target, nil
		}),
	)

	require.NoError(t, s.RunSplitJobRunnerOnce(ctx))
	paused, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, paused.CapabilityRegressed)
	require.Equal(t, distribution.SplitJobPhasePlanned, paused.Phase)
	require.Empty(t, source.controls)

	s.migrationCapabilityGate = func(context.Context) error { return nil }
	require.NoError(t, s.RunSplitJobRunnerOnce(ctx))
	resumed, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, resumed.CapabilityRegressed)
	require.Equal(t, distribution.SplitJobPhasePlanned, resumed.Phase)
	require.NoError(t, s.RunSplitJobRunnerOnce(ctx))
	armed, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, distribution.SplitJobPhaseBackfill, armed.Phase)
	require.NotZero(t, armed.SnapshotTS)
}

func TestSplitJobHistoryGCKeysAppliesTTLAndCountBound(t *testing.T) {
	t.Parallel()

	now := time.UnixMilli(10 * splitJobHistoryTTL.Milliseconds())
	jobs := make([]distribution.SplitJob, 0, splitJobHistoryLimit+3)
	jobs = append(jobs, distribution.SplitJob{
		JobID:        1,
		Phase:        distribution.SplitJobPhaseDone,
		TerminalAtMs: now.Add(-splitJobHistoryTTL - time.Second).UnixMilli(),
	})
	for i := 0; i < splitJobHistoryLimit+2; i++ {
		jobs = append(jobs, distribution.SplitJob{
			JobID:        uint64(i) + 2, //nolint:gosec // loop bound is splitJobHistoryLimit.
			Phase:        distribution.SplitJobPhaseDone,
			TerminalAtMs: now.Add(-time.Hour).UnixMilli() + int64(i),
		})
	}

	keys := splitJobHistoryGCKeys(jobs, now)
	require.Len(t, keys, 3)
	require.Equal(t, distribution.CatalogSplitJobHistoryKey(jobs[0].TerminalAtMs, jobs[0].JobID), keys[0])
}

func TestDistributionServerRunSplitJobRunnerOnce_SkipsFollower(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	_, err := catalog.Save(ctx, 0, promotionCompleteDistributionRoutes())
	require.NoError(t, err)
	require.NoError(t, catalog.CreateSplitJob(ctx, promotionCompleteDistributionJob()))

	client := &splitPromotionClientStub{
		responses: []*pb.PromoteStagedVersionsResponse{{Done: true}},
	}
	coordinator := newDistributionCoordinatorStub(baseStore, false)
	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(coordinator),
		WithSplitPromotionClientFactory(func(context.Context, distribution.SplitJob) (SplitPromotionClient, error) {
			return client, nil
		}),
	)

	require.NoError(t, s.RunSplitJobRunnerOnce(ctx))
	require.Zero(t, client.calls)
	require.Zero(t, coordinator.dispatchCalls)
}

func TestSplitJobRunnerContextDoneRecognizesWrappedGRPCStatus(t *testing.T) {
	t.Parallel()

	require.True(t, splitJobRunnerContextDone(context.Canceled))
	require.True(t, splitJobRunnerContextDone(context.DeadlineExceeded))
	require.True(t, splitJobRunnerContextDone(crdberrors.WithStack(status.Error(codes.Canceled, "stopping"))))
	require.True(t, splitJobRunnerContextDone(crdberrors.WithStack(status.Error(codes.DeadlineExceeded, "stopping"))))
	require.False(t, splitJobRunnerContextDone(crdberrors.WithStack(status.Error(codes.Unavailable, "not leader"))))
}

func TestPromoteSplitJobTargetRejectsNoCursorProgress(t *testing.T) {
	t.Parallel()

	client := &splitPromotionClientStub{
		responses: []*pb.PromoteStagedVersionsResponse{{NextCursor: []byte("same")}},
	}
	err := promoteSplitJobTarget(context.Background(), client, promotionCompleteDistributionJob())
	require.NoError(t, err)

	client = &splitPromotionClientStub{
		responses: []*pb.PromoteStagedVersionsResponse{
			{NextCursor: []byte("same")},
			{NextCursor: []byte("same")},
		},
	}
	err = promoteSplitJobTarget(context.Background(), client, promotionCompleteDistributionJob())
	require.Error(t, err)
	require.ErrorContains(t, err, "split promotion made no cursor progress")
}

func TestDistributionServerRetrySplitJob_RequiresCatalogLeader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	job := sampleDistributionSplitJob(14)
	job.Phase = distribution.SplitJobPhaseFailed
	job.RetryPhase = distribution.SplitJobPhaseBackfill
	require.NoError(t, catalog.CreateSplitJob(ctx, job))

	coordinator := newDistributionCoordinatorStub(baseStore, false)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
	_, err := s.RetrySplitJob(ctx, &pb.RetrySplitJobRequest{JobId: job.JobID})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, coordinator.dispatchCalls)
}

func TestDistributionServerGetRouteOwnership_UsesExactVersionSnapshot(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 7,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{
				RouteID:                2,
				Start:                  []byte("m"),
				End:                    nil,
				GroupID:                2,
				State:                  distribution.RouteStateMigratingTarget,
				StagedVisibilityActive: true,
				MigrationJobID:         44,
				MinWriteTSExclusive:    55,
			},
		},
	}))

	s := NewDistributionServer(engine, nil)
	resp, err := s.GetRouteOwnership(context.Background(), &pb.GetRouteOwnershipRequest{
		Key:            []byte("t"),
		CatalogVersion: 7,
	})
	require.NoError(t, err)
	require.True(t, resp.Found)
	require.Equal(t, uint64(7), resp.CatalogVersion)
	require.Equal(t, uint64(2), resp.Route.RouteId)
	require.Equal(t, uint64(2), resp.Route.RaftGroupId)
	require.Equal(t, pb.RouteState_ROUTE_STATE_MIGRATING_TARGET, resp.Route.State)
	require.True(t, resp.Route.StagedVisibilityActive)
	require.Equal(t, uint64(44), resp.Route.MigrationJobId)
	require.Equal(t, uint64(55), resp.Route.MinWriteTsExclusive)

	miss, err := s.GetRouteOwnership(context.Background(), &pb.GetRouteOwnershipRequest{
		Key:            []byte("0"),
		CatalogVersion: 7,
	})
	require.NoError(t, err)
	require.False(t, miss.Found)
	require.Equal(t, uint64(7), miss.CatalogVersion)
	require.Nil(t, miss.Route)
}

func TestDistributionServerGetIntersectingRoutes_UsesExactVersionSnapshot(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 9,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: []byte("g"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("g"), End: []byte("m"), GroupID: 2, State: distribution.RouteStateWriteFenced},
			{RouteID: 3, Start: []byte("m"), End: nil, GroupID: 3, State: distribution.RouteStateActive},
		},
	}))

	s := NewDistributionServer(engine, nil)
	resp, err := s.GetIntersectingRoutes(context.Background(), &pb.GetIntersectingRoutesRequest{
		Start:          []byte("f"),
		End:            []byte("z"),
		CatalogVersion: 9,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(9), resp.CatalogVersion)
	require.Len(t, resp.Routes, 3)
	require.Equal(t, []uint64{1, 2, 3}, []uint64{resp.Routes[0].RouteId, resp.Routes[1].RouteId, resp.Routes[2].RouteId})

	rightOpen, err := s.GetIntersectingRoutes(context.Background(), &pb.GetIntersectingRoutesRequest{
		Start:          []byte("m"),
		End:            nil,
		CatalogVersion: 9,
	})
	require.NoError(t, err)
	require.Len(t, rightOpen.Routes, 1)
	require.Equal(t, uint64(3), rightOpen.Routes[0].RouteId)
}

func TestDistributionServerOwnershipRPCs_RejectUnknownCatalogVersion(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	s := NewDistributionServer(engine, nil)
	_, err := s.GetRouteOwnership(context.Background(), &pb.GetRouteOwnershipRequest{
		Key:            []byte("a"),
		CatalogVersion: 2,
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.ErrorContains(t, err, errDistributionCatalogVersionNotFound.Error())

	_, err = s.GetIntersectingRoutes(context.Background(), &pb.GetIntersectingRoutesRequest{
		Start:          []byte(""),
		CatalogVersion: 2,
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.ErrorContains(t, err, errDistributionCatalogVersionNotFound.Error())
}

func TestDistributionServerOwnershipRPCs_RequireEngine(t *testing.T) {
	t.Parallel()

	s := NewDistributionServer(nil, nil)
	_, err := s.GetRouteOwnership(context.Background(), &pb.GetRouteOwnershipRequest{CatalogVersion: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionEngineNotConfigured.Error())

	_, err = s.GetIntersectingRoutes(context.Background(), &pb.GetIntersectingRoutesRequest{CatalogVersion: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionEngineNotConfigured.Error())
}

func TestDistributionServerSplitRange_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:             1,
			Start:               []byte(""),
			End:                 []byte("m"),
			GroupID:             1,
			State:               distribution.RouteStateActive,
			ParentRouteID:       0,
			MinWriteTSExclusive: 99,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	s := NewDistributionServer(
		engine,
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
	)

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)

	require.Equal(t, uint64(2), resp.CatalogVersion)
	require.Equal(t, uint64(3), resp.Left.RouteId)
	require.Equal(t, []byte(""), resp.Left.Start)
	require.Equal(t, []byte("g"), resp.Left.End)
	require.Equal(t, uint64(1), resp.Left.RaftGroupId)
	require.Equal(t, uint64(1), resp.Left.ParentRouteId)
	require.Equal(t, uint64(99), resp.Left.MinWriteTsExclusive)
	require.Equal(t, uint64(4), resp.Right.RouteId)
	require.Equal(t, []byte("g"), resp.Right.Start)
	require.Equal(t, []byte("m"), resp.Right.End)
	require.Equal(t, uint64(1), resp.Right.RaftGroupId)
	require.Equal(t, uint64(1), resp.Right.ParentRouteId)
	require.Equal(t, uint64(99), resp.Right.MinWriteTsExclusive)

	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), snapshot.Version)
	require.Len(t, snapshot.Routes, 3)
	// Catalog snapshots are sorted by range start key.
	require.Equal(t, uint64(3), snapshot.Routes[0].RouteID)
	require.Equal(t, uint64(99), snapshot.Routes[0].MinWriteTSExclusive)
	require.Equal(t, uint64(4), snapshot.Routes[1].RouteID)
	require.Equal(t, uint64(99), snapshot.Routes[1].MinWriteTSExclusive)
	require.Equal(t, uint64(2), snapshot.Routes[2].RouteID)

	require.Equal(t, uint64(2), engine.Version())
	leftRoute, ok := engine.GetRoute([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(3), leftRoute.RouteID)
	require.Equal(t, uint64(99), leftRoute.MinWriteTSExclusive)
	rightRoute, ok := engine.GetRoute([]byte("h"))
	require.True(t, ok)
	require.Equal(t, uint64(4), rightRoute.RouteID)
	require.Equal(t, uint64(99), rightRoute.MinWriteTSExclusive)
}

func TestDistributionServerSplitRange_RequiresCoordinator(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServerWithoutCoordinator(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionCoordinatorRequired.Error())
}

func TestDistributionServerSplitRange_UnknownRoute(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServer(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version,
		RouteId:                999,
		SplitKey:               []byte("g"),
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.ErrorContains(t, err, errDistributionUnknownRoute.Error())
}

func TestDistributionServerSplitRange_InvalidSplitKey(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServer(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version,
		RouteId:                1,
		SplitKey:               []byte("z"),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, errDistributionInvalidSplitKey.Error())
}

func TestDistributionServerSplitRange_SplitKeyAtBoundary(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServer(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version,
		RouteId:                1,
		SplitKey:               []byte("a"),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, errDistributionSplitKeyAtBoundary.Error())
}

func TestDistributionServerSplitRange_VersionConflict(t *testing.T) {
	t.Parallel()

	s, version := seededDistributionServer(t)
	_, err := s.SplitRange(context.Background(), &pb.SplitRangeRequest{
		ExpectedCatalogVersion: version - 1,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.Error(t, err)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.ErrorContains(t, err, errDistributionCatalogConflict.Error())
}

func TestDistributionServerSplitRange_RejectsLiveSplitJobOverlap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	require.NoError(t, catalog.CreateSplitJob(ctx, distribution.SplitJob{
		JobID:         10,
		SourceRouteID: 1,
		SplitKey:      []byte("g"),
		TargetGroupID: 8,
		Phase:         distribution.SplitJobPhaseBackfill,
	}))

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
	_, err = s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("c"),
	})
	require.Error(t, err)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.ErrorContains(t, err, distribution.ErrSplitJobOverlap.Error())
	require.Zero(t, coordinator.dispatchCalls)
}

func TestDistributionServerSplitRange_AllowsDisjointRouteWhileSplitJobLive(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{RouteID: 3, Start: []byte("a"), End: []byte("g"), GroupID: 1, State: distribution.RouteStateActive, ParentRouteID: 1},
		{RouteID: 4, Start: []byte("g"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateWriteFenced, ParentRouteID: 1},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	require.NoError(t, catalog.CreateSplitJob(ctx, distribution.SplitJob{
		JobID:         10,
		SourceRouteID: 1,
		SplitKey:      []byte("g"),
		TargetGroupID: 8,
		Phase:         distribution.SplitJobPhaseFence,
	}))

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                3,
		SplitKey:               []byte("c"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.CatalogVersion)
	require.Equal(t, 1, coordinator.dispatchCalls)
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogNextSplitJobIDKey())
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogSplitJobKey(10))
}

func TestSplitJobReadFenceKeysExcludesTerminalHistory(t *testing.T) {
	t.Parallel()

	readKeys := splitJobReadFenceKeys([]distribution.SplitJob{
		{
			JobID: 10,
			Phase: distribution.SplitJobPhaseBackfill,
		},
		{
			JobID:        11,
			Phase:        distribution.SplitJobPhaseDone,
			TerminalAtMs: 1000,
		},
		{
			JobID:        12,
			Phase:        distribution.SplitJobPhaseAbandoned,
			TerminalAtMs: 1001,
		},
	})

	require.Len(t, readKeys, 2)
	requireReadKeysContain(t, readKeys, distribution.CatalogNextSplitJobIDKey())
	requireReadKeysContain(t, readKeys, distribution.CatalogSplitJobKey(10))
	requireReadKeysNotContain(t, readKeys, distribution.CatalogSplitJobHistoryKey(1000, 11))
	requireReadKeysNotContain(t, readKeys, distribution.CatalogSplitJobHistoryKey(1001, 12))
}

func TestDistributionServerSplitRange_ConflictsWhenSplitJobCreatedAfterOverlapScan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)

	coordinator := newDistributionCoordinatorStub(baseStore, true)
	coordinator.beforeApply = func(ctx context.Context, _ store.MVCCStore) error {
		return catalog.CreateSplitJob(ctx, distribution.SplitJob{
			JobID:         10,
			SourceRouteID: 1,
			SplitKey:      []byte("g"),
			TargetGroupID: 8,
			Phase:         distribution.SplitJobPhaseBackfill,
		})
	}
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))

	_, err = s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("c"),
	})
	require.Error(t, err)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.ErrorContains(t, err, errDistributionCatalogConflict.Error())
	require.Equal(t, 1, coordinator.dispatchCalls)

	snapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, saved.Version, snapshot.Version)
}

func TestDistributionServerSplitRange_UsesCoordinatorForCatalogWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))
	readSnapshot, err := catalog.Snapshot(ctx)
	require.NoError(t, err)

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.CatalogVersion)
	require.Equal(t, 1, coordinator.dispatchCalls)
	require.Equal(t, readSnapshot.ReadTS, coordinator.lastStartTS)
}

func TestDistributionServerSplitRange_UsesPersistentNextRouteID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))

	first, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), first.Left.RouteId)
	require.Equal(t, uint64(4), first.Right.RouteId)

	nextRouteID, err := catalog.NextRouteID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(5), nextRouteID)

	afterDelete, err := catalog.Save(ctx, first.CatalogVersion, []distribution.RouteDescriptor{
		{
			RouteID:       3,
			Start:         []byte(""),
			End:           []byte("g"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 1,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), afterDelete.Version)

	second, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: afterDelete.Version,
		RouteId:                3,
		SplitKey:               []byte("c"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), second.Left.RouteId)
	require.Equal(t, uint64(6), second.Right.RouteId)
}

func TestDistributionServerSplitRange_AllowsVersionAdvanceAfterCommit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	coordinator.afterDispatch = func(ctx context.Context, st store.MVCCStore, commitTS uint64) error {
		return st.PutAt(ctx, distribution.CatalogVersionKey(), distribution.EncodeCatalogVersion(3), commitTS+1, 0)
	}
	s := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), resp.CatalogVersion)
	require.Equal(t, uint64(3), engine.Version())
}

func TestDistributionServerSplitRange_RetriesCatalogReloadUntilVisible(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
		{
			RouteID:       2,
			Start:         []byte("m"),
			End:           nil,
			GroupID:       2,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	engine := distribution.NewEngine()
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	coordinator.asyncApplyDelay = 15 * time.Millisecond
	coordinator.asyncApplyDone = make(chan error, 1)
	s := NewDistributionServer(engine, catalog, WithDistributionCoordinator(coordinator))

	resp, err := s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.CatalogVersion)

	select {
	case applyErr := <-coordinator.asyncApplyDone:
		require.NoError(t, applyErr)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async split apply")
	}
}

func TestDistributionServerSplitRange_RequiresCatalogLeaderWhenCoordinatorConfigured(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte(""),
			End:           nil,
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	s := NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, false)),
	)
	_, err = s.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: saved.Version,
		RouteId:                1,
		SplitKey:               []byte("g"),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, errDistributionNotLeader.Error())
}

func TestBuildCatalogSplitOps_UsesSurgicalSplitMutations(t *testing.T) {
	t.Parallel()

	left := distribution.RouteDescriptor{
		RouteID:       3,
		Start:         []byte(""),
		End:           []byte("g"),
		GroupID:       1,
		State:         distribution.RouteStateActive,
		ParentRouteID: 1,
	}
	right := distribution.RouteDescriptor{
		RouteID:       4,
		Start:         []byte("g"),
		End:           []byte("m"),
		GroupID:       1,
		State:         distribution.RouteStateActive,
		ParentRouteID: 1,
	}

	ops, err := buildCatalogSplitOps(1, left, right, 2, 5, false)
	require.NoError(t, err)
	require.Len(t, ops, 5)
	require.Equal(t, kv.Del, ops[0].Op)
	require.Equal(t, distribution.CatalogRouteKey(1), ops[0].Key)
	require.Equal(t, kv.Put, ops[1].Op)
	require.Equal(t, distribution.CatalogRouteKey(3), ops[1].Key)
	require.Equal(t, kv.Put, ops[2].Op)
	require.Equal(t, distribution.CatalogRouteKey(4), ops[2].Key)
	require.Equal(t, kv.Put, ops[3].Op)
	require.Equal(t, distribution.CatalogVersionKey(), ops[3].Key)
	require.Equal(t, kv.Put, ops[4].Op)
	require.Equal(t, distribution.CatalogNextRouteIDKey(), ops[4].Key)
	nextRouteID, err := distribution.DecodeCatalogNextRouteID(ops[4].Value)
	require.NoError(t, err)
	require.Equal(t, uint64(5), nextRouteID)
}

func seededDistributionServer(t *testing.T) (*DistributionServer, uint64) {
	t.Helper()

	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore)
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte("a"),
			End:           []byte("m"),
			GroupID:       7,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	return NewDistributionServer(
		distribution.NewEngine(),
		catalog,
		WithDistributionCoordinator(newDistributionCoordinatorStub(baseStore, true)),
	), saved.Version
}

func seededDistributionServerWithoutCoordinator(t *testing.T) (*DistributionServer, uint64) {
	t.Helper()

	ctx := context.Background()
	catalog := distribution.NewCatalogStore(store.NewMVCCStore())
	saved, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{
		{
			RouteID:       1,
			Start:         []byte("a"),
			End:           []byte("m"),
			GroupID:       7,
			State:         distribution.RouteStateActive,
			ParentRouteID: 0,
		},
	})
	require.NoError(t, err)

	return NewDistributionServer(distribution.NewEngine(), catalog), saved.Version
}

func sampleDistributionSplitJob(jobID uint64) distribution.SplitJob {
	return distribution.SplitJob{
		JobID:         jobID,
		SourceRouteID: 1,
		SplitKey:      []byte("g"),
		TargetGroupID: 2,
		Phase:         distribution.SplitJobPhaseBackfill,
		RetryPhase:    distribution.SplitJobPhaseNone,
		StartedAtMs:   1000,
		UpdatedAtMs:   1000,
	}
}

func promotionCompleteDistributionJob() distribution.SplitJob {
	return distribution.SplitJob{
		JobID:         99,
		SourceRouteID: 42,
		SplitKey:      []byte("m"),
		TargetGroupID: 2,
		Phase:         distribution.SplitJobPhaseCleanup,
	}
}

func promotionCompleteDistributionRoutes() []distribution.RouteDescriptor {
	return []distribution.RouteDescriptor{
		{
			RouteID:       2,
			Start:         []byte(""),
			End:           []byte("m"),
			GroupID:       1,
			State:         distribution.RouteStateActive,
			ParentRouteID: 42,
		},
		{
			RouteID:                3,
			Start:                  []byte("m"),
			End:                    nil,
			GroupID:                2,
			State:                  distribution.RouteStateActive,
			ParentRouteID:          42,
			StagedVisibilityActive: true,
			MigrationJobID:         99,
			MinWriteTSExclusive:    777,
		},
	}
}

func distributionRouteByID(t *testing.T, routes []distribution.RouteDescriptor, routeID uint64) distribution.RouteDescriptor {
	t.Helper()
	for _, route := range routes {
		if route.RouteID == routeID {
			return route
		}
	}
	t.Fatalf("route %d not found in %+v", routeID, routes)
	return distribution.RouteDescriptor{}
}

func splitJobIDs(jobs []*pb.SplitJob) []uint64 {
	ids := make([]uint64, 0, len(jobs))
	for _, job := range jobs {
		ids = append(ids, job.GetJobId())
	}
	return ids
}

func byteSliceStrings(in [][]byte) []string {
	out := make([]string, 0, len(in))
	for _, item := range in {
		out = append(out, string(item))
	}
	return out
}

type splitPromotionClientStub struct {
	responses []*pb.PromoteStagedVersionsResponse
	err       error
	requests  []*pb.PromoteStagedVersionsRequest
	calls     int
}

type splitMigrationClientStub struct {
	splitPromotionClientStub
	exports  []*pb.ExportRangeVersionsRequest
	imports  []*pb.ImportRangeVersionsRequest
	controls []*pb.TargetStagedReadinessRequest
	cleanups []*pb.CleanupMigrationRequest
	probes   []*pb.ProbeMigrationStateRequest
	probeFn  func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error)
	nextTS   uint64
	events   []string
}

func (s *splitMigrationClientStub) ExportRangeVersions(
	_ context.Context,
	req *pb.ExportRangeVersionsRequest,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
	cloned := &pb.ExportRangeVersionsRequest{
		RangeStart:           distribution.CloneBytes(req.GetRangeStart()),
		RangeEnd:             distribution.CloneBytes(req.GetRangeEnd()),
		MaxCommitTs:          req.GetMaxCommitTs(),
		MinCommitTs:          req.GetMinCommitTs(),
		Cursor:               distribution.CloneBytes(req.GetCursor()),
		ChunkBytes:           req.GetChunkBytes(),
		RouteStart:           distribution.CloneBytes(req.GetRouteStart()),
		RouteEnd:             distribution.CloneBytes(req.GetRouteEnd()),
		MaxScannedBytes:      req.GetMaxScannedBytes(),
		KeyFamily:            req.GetKeyFamily(),
		ExcludeKnownInternal: req.GetExcludeKnownInternal(),
		ExcludePrefixes:      req.GetExcludePrefixes(),
	}
	s.exports = append(s.exports, cloned)
	phaseByte := byte(0)
	if req.GetMinCommitTs() != 0 {
		phaseByte = 1
	}
	cursor := []byte{byte(req.GetKeyFamily()), phaseByte}
	return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{
		Versions: []*pb.MVCCVersion{{
			Key:       []byte("n"),
			CommitTs:  10,
			Value:     []byte("v"),
			KeyFamily: req.GetKeyFamily(),
		}},
		NextCursor: cursor,
		Done:       true,
	}}}, nil
}

func (s *splitMigrationClientStub) ImportRangeVersions(
	_ context.Context,
	req *pb.ImportRangeVersionsRequest,
	_ ...grpc.CallOption,
) (*pb.ImportRangeVersionsResponse, error) {
	s.imports = append(s.imports, req)
	return &pb.ImportRangeVersionsResponse{AckedCursor: distribution.CloneBytes(req.GetCursor())}, nil
}

func (s *splitMigrationClientStub) ApplyTargetStagedReadiness(
	_ context.Context,
	req *pb.TargetStagedReadinessRequest,
	_ ...grpc.CallOption,
) (*pb.TargetStagedReadinessResponse, error) {
	s.controls = append(s.controls, req)
	s.events = append(s.events, "control")
	minAdmittedTS := uint64(0)
	if req.GetTrackWrites() {
		minAdmittedTS = 10
	}
	return &pb.TargetStagedReadinessResponse{MinAdmittedTs: minAdmittedTS}, nil
}

func (s *splitMigrationClientStub) ProbeMigrationLocks(
	context.Context,
	*pb.ProbeMigrationLocksRequest,
	...grpc.CallOption,
) (*pb.ProbeMigrationLocksResponse, error) {
	return &pb.ProbeMigrationLocksResponse{}, nil
}

func (s *splitMigrationClientStub) CleanupMigration(
	_ context.Context,
	req *pb.CleanupMigrationRequest,
	_ ...grpc.CallOption,
) (*pb.CleanupMigrationResponse, error) {
	s.cleanups = append(s.cleanups, req)
	return &pb.CleanupMigrationResponse{Done: true, NextCursor: distribution.CloneBytes(req.GetCursor())}, nil
}

func (s *splitMigrationClientStub) ProbeMigrationState(
	_ context.Context,
	req *pb.ProbeMigrationStateRequest,
	_ ...grpc.CallOption,
) (*pb.ProbeMigrationStateResponse, error) {
	s.probes = append(s.probes, req)
	if s.probeFn != nil {
		return s.probeFn(req)
	}
	return &pb.ProbeMigrationStateResponse{Ready: true, CatalogVersion: req.GetExpectedCatalogVersion()}, nil
}

func (s *splitMigrationClientStub) IssueMigrationTimestamp(
	context.Context,
	*pb.IssueMigrationTimestampRequest,
	...grpc.CallOption,
) (*pb.IssueMigrationTimestampResponse, error) {
	s.events = append(s.events, "timestamp")
	if s.nextTS == 0 {
		s.nextTS = 100
	}
	s.nextTS++
	return &pb.IssueMigrationTimestampResponse{Timestamp: s.nextTS, LastCommitTs: s.nextTS - 1}, nil
}

type splitMigrationStreamStub struct {
	grpc.ClientStream
	responses []*pb.ExportRangeVersionsResponse
}

func (s *splitMigrationStreamStub) Recv() (*pb.ExportRangeVersionsResponse, error) {
	if len(s.responses) == 0 {
		return nil, io.EOF
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

func (s *splitPromotionClientStub) PromoteStagedVersions(
	_ context.Context,
	req *pb.PromoteStagedVersionsRequest,
	_ ...grpc.CallOption,
) (*pb.PromoteStagedVersionsResponse, error) {
	s.calls++
	s.requests = append(s.requests, &pb.PromoteStagedVersionsRequest{
		JobId:           req.GetJobId(),
		Cursor:          distribution.CloneBytes(req.GetCursor()),
		MaxVersions:     req.GetMaxVersions(),
		MaxBytes:        req.GetMaxBytes(),
		MaxScannedBytes: req.GetMaxScannedBytes(),
	})
	if s.err != nil {
		return nil, s.err
	}
	if len(s.responses) == 0 {
		return &pb.PromoteStagedVersionsResponse{Done: true}, nil
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

type distributionCoordinatorStub struct {
	store           store.MVCCStore
	leader          bool
	dispatchErr     error
	nextTS          uint64
	timestampNext   uint64
	timestampErr    error
	timestampCalls  int
	lastStartTS     uint64
	lastCommitTS    uint64
	lastReadKeys    [][]byte
	beforeApply     func(context.Context, store.MVCCStore) error
	afterDispatch   func(context.Context, store.MVCCStore, uint64) error
	asyncApplyDone  chan error
	asyncApplyDelay time.Duration
	dispatchCalls   int
}

func newDistributionCoordinatorStub(st store.MVCCStore, leader bool) *distributionCoordinatorStub {
	return &distributionCoordinatorStub{
		store:  st,
		leader: leader,
	}
}

func (s *distributionCoordinatorStub) Dispatch(ctx context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if err := s.validateDispatch(reqs); err != nil {
		return nil, err
	}
	s.dispatchCalls++
	if s.dispatchErr != nil {
		return nil, s.dispatchErr
	}
	startTS, commitTS := s.nextTimestamps(reqs.StartTS, reqs.CommitTS)
	s.lastStartTS = startTS
	s.lastCommitTS = commitTS
	readKeys := cloneDistributionReadKeys(reqs.ReadKeys)
	s.lastReadKeys = readKeys

	mutations, err := coordinatorStubMutations(reqs.Elems)
	if err != nil {
		return nil, err
	}
	if s.asyncApplyDelay > 0 {
		done := s.asyncApplyDone
		delay := s.asyncApplyDelay
		go func() {
			time.Sleep(delay)
			err := s.applyDispatch(ctx, mutations, readKeys, startTS, commitTS)
			if done != nil {
				done <- err
			}
		}()
		return &kv.CoordinateResponse{CommitIndex: commitTS}, nil
	}
	if err := s.applyDispatch(ctx, mutations, readKeys, startTS, commitTS); err != nil {
		return nil, err
	}
	return &kv.CoordinateResponse{CommitIndex: commitTS}, nil
}

func (s *distributionCoordinatorStub) validateDispatch(reqs *kv.OperationGroup[kv.OP]) error {
	if !s.leader {
		return kv.ErrLeaderNotFound
	}
	if reqs == nil || len(reqs.Elems) == 0 || !reqs.IsTxn {
		return kv.ErrInvalidRequest
	}
	return nil
}

func (s *distributionCoordinatorStub) nextTimestamps(startTS uint64, requestedCommitTS uint64) (uint64, uint64) {
	if requestedCommitTS != 0 {
		if s.nextTS <= requestedCommitTS {
			s.nextTS = requestedCommitTS + 1
		}
		return startTS, requestedCommitTS
	}
	if s.nextTS == 0 {
		s.nextTS = s.store.LastCommitTS() + 1
	}
	commitTS := s.nextTS
	if startTS > 0 && commitTS <= startTS {
		commitTS = startTS + 1
	}
	s.nextTS = commitTS + 1
	return startTS, commitTS
}

func (s *distributionCoordinatorStub) applyDispatch(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	if s.beforeApply != nil {
		if err := s.beforeApply(ctx, s.store); err != nil {
			return err
		}
	}
	if err := s.store.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS); err != nil {
		return err
	}
	if s.afterDispatch != nil {
		if err := s.afterDispatch(ctx, s.store, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func cloneDistributionReadKeys(in [][]byte) [][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = distribution.CloneBytes(in[i])
	}
	return out
}

func requireReadKeysContain(t *testing.T, readKeys [][]byte, want []byte) {
	t.Helper()
	for _, key := range readKeys {
		if bytes.Equal(key, want) {
			return
		}
	}
	t.Fatalf("expected read keys to contain %q, got %q", want, readKeys)
}

func requireReadKeysNotContain(t *testing.T, readKeys [][]byte, want []byte) {
	t.Helper()
	for _, key := range readKeys {
		if bytes.Equal(key, want) {
			t.Fatalf("expected read keys not to contain %q, got %q", want, readKeys)
		}
	}
}

func coordinatorStubMutations(elems []*kv.Elem[kv.OP]) ([]*store.KVPairMutation, error) {
	mutations := make([]*store.KVPairMutation, 0, len(elems))
	for _, elem := range elems {
		mutation, err := coordinatorStubMutation(elem)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, mutation)
	}
	return mutations, nil
}

func coordinatorStubMutation(elem *kv.Elem[kv.OP]) (*store.KVPairMutation, error) {
	if elem == nil {
		return nil, kv.ErrInvalidRequest
	}
	switch elem.Op {
	case kv.Put:
		return &store.KVPairMutation{
			Op:    store.OpTypePut,
			Key:   distribution.CloneBytes(elem.Key),
			Value: distribution.CloneBytes(elem.Value),
		}, nil
	case kv.Del:
		return &store.KVPairMutation{
			Op:  store.OpTypeDelete,
			Key: distribution.CloneBytes(elem.Key),
		}, nil
	case kv.DelPrefix:
		return nil, kv.ErrInvalidRequest
	default:
		return nil, kv.ErrInvalidRequest
	}
}

func (s *distributionCoordinatorStub) IsLeader() bool {
	return s.leader
}

func (s *distributionCoordinatorStub) VerifyLeader(context.Context) error {
	if !s.leader {
		return kv.ErrLeaderNotFound
	}
	return nil
}

func (s *distributionCoordinatorStub) RaftLeader() string {
	return ""
}

func (s *distributionCoordinatorStub) IsLeaderForKey(_ []byte) bool {
	return s.leader
}

func (s *distributionCoordinatorStub) VerifyLeaderForKey(_ context.Context, _ []byte) error {
	if !s.leader {
		return kv.ErrLeaderNotFound
	}
	return nil
}

func (s *distributionCoordinatorStub) RaftLeaderForKey(_ []byte) string {
	return ""
}

func (s *distributionCoordinatorStub) Clock() *kv.HLC {
	return nil
}

func (s *distributionCoordinatorStub) Next(ctx context.Context) (uint64, error) {
	return s.NextAfter(ctx, 0)
}

func (s *distributionCoordinatorStub) NextAfter(_ context.Context, min uint64) (uint64, error) {
	s.timestampCalls++
	if s.timestampErr != nil {
		return 0, s.timestampErr
	}
	next := s.timestampNext
	if next == 0 {
		next = s.store.LastCommitTS() + 1
	}
	if next <= min {
		next = min + 1
	}
	s.timestampNext = next + 1
	return next, nil
}

func (s *distributionCoordinatorStub) LinearizableRead(_ context.Context) (uint64, error) {
	return 0, nil
}

func (s *distributionCoordinatorStub) LeaseRead(ctx context.Context) (uint64, error) {
	return s.LinearizableRead(ctx)
}

func (s *distributionCoordinatorStub) LeaseReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return s.LinearizableRead(ctx)
}
