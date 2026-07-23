package autosplit

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommittedWindowsFromColumnsDerivesDurations(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0)
	cols := []keyviz.MatrixColumn{
		{At: base.Add(2 * time.Minute)},
		{At: base},
		{At: base.Add(time.Minute)},
	}

	windows, _, skipped := CommittedWindowsFromColumns(cols, time.Time{})

	require.Equal(t, 1, skipped)
	require.Len(t, windows, 3)
	require.Equal(t, base, windows[0].Column.At)
	require.Zero(t, windows[0].Duration)
	require.Equal(t, base.Add(time.Minute), windows[1].Column.At)
	require.Equal(t, time.Minute, windows[1].Duration)
	require.Equal(t, base.Add(2*time.Minute), windows[2].Column.At)
	require.Equal(t, time.Minute, windows[2].Duration)
}

func TestCommittedWindowsUsesStoredWindowStart(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0)
	cols := []keyviz.MatrixColumn{{
		WindowStart: base,
		At:          base.Add(90 * time.Second),
	}}

	windows, _, skipped := CommittedWindowsFromColumns(cols, time.Time{})

	require.Zero(t, skipped)
	require.Len(t, windows, 1)
	require.Equal(t, 90*time.Second, windows[0].Duration)
}

func TestSchedulerTickSchedulesSplitRange(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	splitter := &fakeSplitter{}
	scheduler := newTestScheduler(source, splitter, hotColumns(now, 2), nil)
	scheduler.wasLeader = true

	result, err := scheduler.Tick(context.Background(), now.Add(2*time.Minute))

	require.NoError(t, err)
	require.Equal(t, 1, result.Scheduled)
	require.Len(t, splitter.requests, 1)
	require.Equal(t, uint64(7), splitter.requests[0].ExpectedCatalogVersion)
	require.Equal(t, uint64(1), splitter.requests[0].RouteID)
	require.Equal(t, []byte("m"), splitter.requests[0].SplitKey)
	require.Equal(t, uint64(0), splitter.requests[0].TargetGroupID)
}

func TestSchedulerUsesCommittedCatalogVersionBetweenSplits(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes: []distribution.RouteDescriptor{
			testRoute(1, 1, "a", "m"),
			testRoute(2, 1, "m", "z"),
		},
	}}
	splitter := &fakeSplitter{}
	scheduler := newTestScheduler(source, splitter, dualRouteHotColumns(now, 2), nil)
	scheduler.wasLeader = true
	scheduler.cfg.Detector.MaxSplitsPerCycle = 2

	result, err := scheduler.Tick(context.Background(), now.Add(2*time.Minute))

	require.NoError(t, err)
	require.Equal(t, 2, result.Scheduled)
	require.Len(t, splitter.requests, 2)
	require.Equal(t, uint64(7), splitter.requests[0].ExpectedCatalogVersion)
	require.Equal(t, uint64(8), splitter.requests[1].ExpectedCatalogVersion)
	require.Equal(t, uint64(9), result.CatalogVersion)
}

func TestSchedulerKillSwitchSkipsSplitRange(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	splitter := &fakeSplitter{}
	scheduler := newTestScheduler(source, splitter, hotColumns(now, 2), func(context.Context) bool {
		return true
	})
	registry := prometheus.NewRegistry()
	observer, ok := NewPrometheusObserver(registry).(*prometheusObserver)
	require.True(t, ok)
	scheduler.cfg.Observer = observer
	scheduler.wasLeader = true

	result, err := scheduler.Tick(context.Background(), now.Add(2*time.Minute))

	require.NoError(t, err)
	require.True(t, result.KillSwitch)
	require.Equal(t, 0, result.Scheduled)
	require.Equal(t, float64(0), testutil.ToFloat64(observer.enabled))
	require.Empty(t, splitter.requests)
}

func TestSchedulerCatalogVersionGapConsumesBufferedEvidence(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	scheduler := newTestScheduler(source, &fakeSplitter{}, hotColumns(now, 1), nil)
	scheduler.wasLeader = true

	first, err := scheduler.Tick(context.Background(), now.Add(time.Minute))
	require.NoError(t, err)
	require.Empty(t, first.Detector.Decisions)
	require.Equal(t, 1, scheduler.state.RouteStatus(1).ConsecutiveOver)

	source.snapshot.Version = 8
	sampler := requireFakeSampler(t, scheduler)
	sampler.cols = append(sampler.cols, hotColumn(now.Add(2*time.Minute)))
	gap, err := scheduler.Tick(context.Background(), now.Add(2*time.Minute))
	require.NoError(t, err)
	require.Empty(t, gap.Detector.Decisions)
	require.Equal(t, 0, scheduler.state.RouteStatus(1).ConsecutiveOver)
	require.Equal(t, now.Add(2*time.Minute), scheduler.state.RouteStatus(1).LastProcessedAt)

	sampler.cols = append(sampler.cols, hotColumn(now.Add(3*time.Minute)))
	after, err := scheduler.Tick(context.Background(), now.Add(3*time.Minute))
	require.NoError(t, err)
	require.Empty(t, after.Detector.Decisions)
	require.Equal(t, 1, scheduler.state.RouteStatus(1).ConsecutiveOver)
}

func TestSchedulerHistoryGapClearsPriorConfidence(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	scheduler := newTestScheduler(source, &fakeSplitter{}, hotColumns(now, 1), nil)
	scheduler.wasLeader = true

	first, err := scheduler.Tick(context.Background(), now.Add(time.Minute))
	require.NoError(t, err)
	require.Empty(t, first.Detector.Decisions)
	require.Equal(t, 1, scheduler.state.RouteStatus(1).ConsecutiveOver)

	sampler := requireFakeSampler(t, scheduler)
	sampler.cols = []keyviz.MatrixColumn{
		hotMatrixColumn(now.Add(5*time.Minute), now.Add(4*time.Minute)),
	}
	afterGap, err := scheduler.Tick(context.Background(), now.Add(5*time.Minute))
	require.NoError(t, err)
	require.Empty(t, afterGap.Detector.Decisions)
	require.Equal(t, 0, scheduler.state.RouteStatus(1).ConsecutiveOver)
	require.Equal(t, now.Add(5*time.Minute), scheduler.state.RouteStatus(1).LastProcessedAt)
}

func TestSplitFailureReasonUsesBoundedLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		err           error
		targetGroupID uint64
		want          string
	}{
		{name: "catalog CAS", err: status.Error(codes.Aborted, "version"), want: "cas_conflict"},
		{name: "wrapped catalog CAS", err: errors.Wrap(status.Error(codes.Aborted, "version"), "split"), want: "cas_conflict"},
		{name: "target unavailable", err: status.Error(codes.FailedPrecondition, "target"), targetGroupID: 2, want: "target_unavailable"},
		{name: "same-group precondition", err: status.Error(codes.FailedPrecondition, "route"), want: "rpc_error"},
		{name: "other RPC", err: status.Error(codes.Internal, "failed"), targetGroupID: 2, want: "rpc_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, splitFailureReason(tt.err, tt.targetGroupID))
		})
	}
}

func TestSchedulerFailedSplitDoesNotSeedCooldown(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	splitter := &fakeSplitter{err: errors.New("cas conflict")}
	scheduler := newTestScheduler(source, splitter, hotColumns(now, 2), nil)
	scheduler.wasLeader = true

	result, err := scheduler.Tick(context.Background(), now.Add(2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, result.Failed)
	require.Len(t, splitter.requests, 1)
	require.True(t, scheduler.state.RouteStatus(1).CooldownUntil.IsZero())

	sampler := requireFakeSampler(t, scheduler)
	sampler.cols = append(sampler.cols, hotColumn(now.Add(3*time.Minute)))
	result, err = scheduler.Tick(context.Background(), now.Add(3*time.Minute))

	require.NoError(t, err)
	require.Equal(t, 1, result.Failed)
	require.Len(t, splitter.requests, 2)
	require.True(t, scheduler.state.RouteStatus(1).CooldownUntil.IsZero())
}

func TestSchedulerSeedsCooldownFromSplitAtHLC(t *testing.T) {
	t.Parallel()
	now := time.UnixMilli(1_700_000_000_000)
	route := testRoute(2, 1, "a", "z")
	route.SplitAtHLC = packTestHLC(now.Add(-time.Minute))
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 8,
		Routes:  []distribution.RouteDescriptor{route},
	}}
	splitter := &fakeSplitter{}
	scheduler := newTestScheduler(source, splitter, hotColumns(now.Add(-3*time.Minute), 3), nil)
	scheduler.wasLeader = true

	result, err := scheduler.Tick(context.Background(), now)

	require.NoError(t, err)
	require.Equal(t, 0, result.Scheduled)
	require.Empty(t, splitter.requests)
	require.WithinDuration(t, now.Add(9*time.Minute), scheduler.state.RouteStatus(2).CooldownUntil, time.Millisecond)
	requireEvent(t, result.Detector.Events, 2, SkipReasonCooldown)
}

func TestRemainingCooldownUsesHLCPhysicalMillis(t *testing.T) {
	t.Parallel()
	now := time.UnixMilli(1_700_000_000_000)
	splitAt := packTestHLC(now.Add(-time.Minute)) | 42

	remaining := RemainingCooldownFromSplitAtHLC(splitAt, 10*time.Minute, now)

	require.Equal(t, 9*time.Minute, remaining)
	rawElapsedMs := packTestHLC(now) - splitAt
	require.Greater(t, rawElapsedMs, uint64(600_000))

	future := RemainingCooldownFromSplitAtHLC(packTestHLC(now.Add(time.Minute)), 10*time.Minute, now)
	require.Equal(t, 10*time.Minute, future)
}

func TestSchedulerLeadershipResetIgnoresPreStartHistory(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	splitter := &fakeSplitter{}
	scheduler := newTestScheduler(source, splitter, hotColumns(start, 3), nil)

	result, err := scheduler.Tick(context.Background(), start.Add(3*time.Minute))
	require.NoError(t, err)
	require.Equal(t, 0, result.Scheduled)
	require.Empty(t, splitter.requests)

	sampler := requireFakeSampler(t, scheduler)
	sampler.cols = append(sampler.cols,
		hotColumn(start.Add(4*time.Minute)),
		hotColumn(start.Add(5*time.Minute)),
		hotColumn(start.Add(6*time.Minute)),
	)
	result, err = scheduler.Tick(context.Background(), start.Add(6*time.Minute))

	require.NoError(t, err)
	require.Equal(t, 1, result.Scheduled)
	require.Len(t, splitter.requests, 1)
	require.Equal(t, []byte("m"), splitter.requests[0].SplitKey)
}

func TestSchedulerReconcilesSamplerRoutes(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 1,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "m")},
	}}
	registrar := &fakeRegistrar{}
	scheduler := newTestScheduler(source, &fakeSplitter{}, nil, nil)
	scheduler.reconciler = NewRouteReconciler(registrar)
	scheduler.wasLeader = true

	_, err := scheduler.Tick(context.Background(), now)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, registrar.registered)

	source.snapshot = distribution.CatalogSnapshot{
		Version: 2,
		Routes:  []distribution.RouteDescriptor{testRoute(2, 1, "m", "z")},
	}
	_, err = scheduler.Tick(context.Background(), now.Add(time.Minute))

	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, registrar.registered)
	require.Equal(t, []uint64{1}, registrar.removed)
}

func TestSchedulerReregistersChangedSamplerRoute(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 1,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "m")},
	}}
	registrar := &fakeRegistrar{}
	scheduler := newTestScheduler(source, &fakeSplitter{}, nil, nil)
	scheduler.reconciler = NewRouteReconciler(registrar)
	scheduler.wasLeader = true

	_, err := scheduler.Tick(context.Background(), now)
	require.NoError(t, err)

	source.snapshot = distribution.CatalogSnapshot{
		Version: 2,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 2, "m", "z")},
	}
	_, err = scheduler.Tick(context.Background(), now.Add(time.Minute))

	require.NoError(t, err)
	require.Equal(t, []uint64{1, 1}, registrar.registered)
	require.Equal(t, []uint64{1}, registrar.removed)
	require.Len(t, registrar.registrations, 2)
	require.Equal(t, uint64(2), registrar.registrations[1].groupID)
	require.Equal(t, []byte("m"), registrar.registrations[1].start)
	require.Equal(t, []byte("z"), registrar.registrations[1].end)
}

func TestSchedulerExecutesCompoundIsolationBackToBack(t *testing.T) {
	t.Parallel()
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	splitter := &fakeSplitter{}
	splitter.onSuccess = func(req SplitRequest, result SplitResult) {
		applyFakeSplit(source, req, result)
	}
	scheduler := newTestScheduler(source, splitter, nil, nil)

	version, err := scheduler.executeDecision(context.Background(), 7, testCompoundDecision())

	require.NoError(t, err)
	require.Equal(t, uint64(9), version)
	require.Len(t, splitter.requests, 2)
	require.Equal(t, uint64(7), splitter.requests[0].ExpectedCatalogVersion)
	require.Equal(t, uint64(8), splitter.requests[1].ExpectedCatalogVersion)
	require.Equal(t, uint64(1), splitter.requests[0].RouteID)
	require.Equal(t, splitter.requests[0].SplitKey, splitter.requests[1].ParentStart)
	require.Equal(t, []byte("m"), splitter.requests[0].SplitKey)
	require.Equal(t, []byte{'m', 0}, splitter.requests[1].SplitKey)
	require.Zero(t, splitter.requests[0].TargetGroupID)
	require.Zero(t, splitter.requests[1].TargetGroupID)
	require.Empty(t, scheduler.pendingCompounds)
	require.Equal(t, uint64(9), source.snapshot.Version)
	require.Len(t, source.snapshot.Routes, 3)
}

func TestSchedulerCompoundUsesNormalizedCommittedBoundary(t *testing.T) {
	t.Parallel()
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	splitter := &fakeSplitter{
		normalizeSplitKey: func(key []byte) []byte {
			if bytes.Equal(key, []byte("m")) {
				return []byte("l")
			}
			return key
		},
	}
	splitter.onSuccess = func(req SplitRequest, result SplitResult) {
		applyFakeSplit(source, req, result)
	}
	scheduler := newTestScheduler(source, splitter, nil, nil)

	version, err := scheduler.executeDecision(context.Background(), 7, testCompoundDecision())

	require.NoError(t, err)
	require.Equal(t, uint64(9), version)
	require.Len(t, splitter.requests, 2)
	require.Equal(t, []byte("m"), splitter.requests[0].SplitKey)
	require.Equal(t, []byte("l"), splitter.requests[1].ParentStart)
	require.Equal(t, []byte{'m', 0}, splitter.requests[1].SplitKey)
	require.Empty(t, scheduler.pendingCompounds)
}

func TestSchedulerRetriesPendingCompoundWithoutNewSamplerColumn(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	killed := true
	splitter := &fakeSplitter{failAt: map[int]error{2: errors.New("temporary")}}
	splitter.onSuccess = func(req SplitRequest, result SplitResult) {
		applyFakeSplit(source, req, result)
	}
	scheduler := newTestScheduler(source, splitter, nil, func(context.Context) bool { return killed })
	scheduler.wasLeader = true

	version, err := scheduler.executeDecision(context.Background(), 7, testCompoundDecision())
	require.Error(t, err)
	require.Equal(t, uint64(8), version)
	require.Len(t, scheduler.pendingCompounds, 1)
	require.Len(t, splitter.requests, 2)
	require.Equal(t, uint64(8), source.snapshot.Version)
	require.Len(t, source.snapshot.Routes, 2)

	blocked, err := scheduler.Tick(context.Background(), now)
	require.NoError(t, err)
	require.True(t, blocked.KillSwitch)
	require.Len(t, splitter.requests, 2)
	require.Len(t, scheduler.pendingCompounds, 1)

	killed = false
	delete(splitter.failAt, 2)
	result, err := scheduler.Tick(context.Background(), now.Add(time.Second))

	require.NoError(t, err)
	require.Equal(t, 1, result.Scheduled)
	require.Zero(t, result.Failed)
	require.Len(t, splitter.requests, 3)
	require.Equal(t, uint64(8), splitter.requests[2].ExpectedCatalogVersion)
	require.Equal(t, []byte{'m', 0}, splitter.requests[2].SplitKey)
	require.Empty(t, scheduler.pendingCompounds)
	require.Equal(t, uint64(9), source.snapshot.Version)
	require.Len(t, source.snapshot.Routes, 3)
}

func TestSchedulerStopsTickAfterCompoundRefreshFailure(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	source := &fakeCatalogSnapshotSource{
		snapshot: distribution.CatalogSnapshot{
			Version: 8,
			Routes: []distribution.RouteDescriptor{
				testRoute(10, 1, "m", "z"),
				testRoute(11, 1, "a", "m"),
			},
		},
		failAt: map[int]error{2: errors.New("refresh failed")},
	}
	splitter := &fakeSplitter{}
	splitter.onSuccess = func(req SplitRequest, result SplitResult) {
		applyFakeSplit(source, req, result)
	}
	scheduler := newTestScheduler(source, splitter, []keyviz.MatrixColumn{
		{
			At:          now.Add(-2 * time.Minute),
			WindowStart: now.Add(-3 * time.Minute),
			Rows: []keyviz.MatrixRow{
				testRow(10, 1, 0, 2, "m", "n", 60, 0),
				testRow(10, 1, 1, 2, "n", "z", 60, 0),
			},
		},
		{
			At:          now.Add(-time.Minute),
			WindowStart: now.Add(-2 * time.Minute),
			Rows: []keyviz.MatrixRow{
				testRow(10, 1, 0, 2, "m", "n", 60, 0),
				testRow(10, 1, 1, 2, "n", "z", 60, 0),
			},
		},
	}, nil)
	scheduler.wasLeader = true
	scheduler.cfg.Detector.MaxSplitsPerCycle = 2
	registrar := &fakeRegistrar{}
	scheduler.reconciler = NewRouteReconciler(registrar)
	scheduler.pendingCompounds[1] = pendingCompound{
		parentRouteID: 1,
		intermediate:  testRoute(10, 1, "m", "z"),
		splitKey:      []byte{'m', 0},
	}

	result, err := scheduler.Tick(context.Background(), now)

	require.NoError(t, err)
	require.Equal(t, 1, result.Scheduled)
	require.Zero(t, result.Failed)
	require.Empty(t, result.Detector.Decisions)
	require.Equal(t, uint64(9), result.CatalogVersion)
	require.Equal(t, uint64(9), scheduler.catalogVersion)
	require.Len(t, splitter.requests, 1)
	require.Empty(t, registrar.registered, "must not reconcile the pre-refresh route set after catalog version advances")
}

func TestSchedulerCatalogTermChangeClearsLeaderLocalState(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	term := uint64(2)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7, Routes: []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	scheduler := newTestScheduler(source, &fakeSplitter{}, nil, nil)
	scheduler.cfg.Leadership = func() (bool, uint64) { return true, term }
	scheduler.wasLeader = true
	scheduler.leaderTerm = 1
	scheduler.state.routes[1] = RouteStatus{ConsecutiveOver: 2, LastProcessedAt: now.Add(-time.Minute)}
	scheduler.pendingCompounds[1] = pendingCompound{
		parentRouteID: 1,
		intermediate:  testRoute(10, 1, "m", "z"),
		splitKey:      []byte{'m', 0},
	}

	result, err := scheduler.Tick(context.Background(), now)

	require.NoError(t, err)
	require.True(t, result.Leader)
	require.Equal(t, term, scheduler.leaderTerm)
	require.Zero(t, scheduler.state.RouteStatus(1).ConsecutiveOver)
	require.Empty(t, scheduler.pendingCompounds)
}

func TestSchedulerShardTermChangeReearnsPostTransferWindows(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0)
	groupTerm := uint64(10)
	source := &fakeCatalogSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7, Routes: []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	splitter := &fakeSplitter{}
	scheduler := newTestScheduler(source, splitter, []keyviz.MatrixColumn{
		hotMatrixColumn(base, base.Add(-time.Minute)),
	}, func(context.Context) bool { return true })
	scheduler.wasLeader = true
	scheduler.cfg.GroupLeadership = func(groupID uint64) (bool, uint64) {
		return groupID == 1, groupTerm
	}

	first, err := scheduler.Tick(context.Background(), base)
	require.NoError(t, err)
	require.Empty(t, first.Detector.Decisions)

	sampler := requireFakeSampler(t, scheduler)
	sampler.cols = append(sampler.cols,
		hotMatrixColumn(base.Add(time.Minute), base),
		hotMatrixColumn(base.Add(2*time.Minute), base.Add(time.Minute)),
		hotMatrixColumn(base.Add(3*time.Minute), base.Add(2*time.Minute)),
	)
	promoted, err := scheduler.Tick(context.Background(), base.Add(3*time.Minute))
	require.NoError(t, err)
	require.Len(t, promoted.Detector.Decisions, 1)

	groupTerm++
	_, err = scheduler.Tick(context.Background(), base.Add(3*time.Minute+30*time.Second))
	require.NoError(t, err)
	sampler.cols = append(sampler.cols,
		hotMatrixColumn(base.Add(4*time.Minute), base.Add(3*time.Minute)),
		hotMatrixColumn(base.Add(5*time.Minute), base.Add(4*time.Minute)),
	)
	notYet, err := scheduler.Tick(context.Background(), base.Add(5*time.Minute))
	require.NoError(t, err)
	require.Empty(t, notYet.Detector.Decisions)
	require.Equal(t, 1, scheduler.state.RouteStatus(1).ConsecutiveOver)

	sampler.cols = append(sampler.cols,
		hotMatrixColumn(base.Add(6*time.Minute), base.Add(5*time.Minute)),
	)
	reearned, err := scheduler.Tick(context.Background(), base.Add(6*time.Minute))
	require.NoError(t, err)
	require.Equal(t, 2, scheduler.state.RouteStatus(1).ConsecutiveOver)
	require.Len(t, reearned.Detector.Decisions, 1)
}

func testCompoundDecision() Decision {
	return Decision{
		RouteID:        1,
		SplitKey:       []byte("m"),
		SecondSplitKey: []byte{'m', 0},
		SplitOrigin:    SplitOriginIsolationCompound,
		RouteDelta:     2,
		RouteStart:     []byte("a"),
		RouteEnd:       []byte("z"),
		RouteGroupID:   1,
	}
}

func applyFakeSplit(source *fakeCatalogSnapshotSource, req SplitRequest, result SplitResult) {
	routes := make([]distribution.RouteDescriptor, 0, len(source.snapshot.Routes)+1)
	for _, route := range source.snapshot.Routes {
		if route.RouteID == req.RouteID {
			continue
		}
		routes = append(routes, route)
	}
	routes = append(routes, result.Left, result.Right)
	source.snapshot = distribution.CatalogSnapshot{Version: result.CatalogVersion, Routes: routes}
}

func newTestScheduler(source *fakeCatalogSnapshotSource, splitter *fakeSplitter, cols []keyviz.MatrixColumn, killSwitch KillSwitch) *Scheduler {
	cfg := SchedulerConfig{
		Enabled: true,
		Detector: Config{
			WriteWeight:       1,
			ReadWeight:        0,
			ThresholdOpsMin:   50,
			CandidateWindows:  2,
			MaxRoutes:         8,
			MaxSplitsPerCycle: 1,
		},
		EvalInterval:  time.Minute,
		SplitCooldown: 10 * time.Minute,
		KillSwitch:    killSwitch,
	}
	return NewScheduler(cfg, source, splitter, &fakeSampler{step: time.Minute, history: 16, cols: cols}, nil)
}

func hotColumns(start time.Time, hotCount int) []keyviz.MatrixColumn {
	cols := make([]keyviz.MatrixColumn, 0, hotCount+1)
	cols = append(cols, keyviz.MatrixColumn{At: start})
	for i := 1; i <= hotCount; i++ {
		cols = append(cols, hotColumn(start.Add(time.Duration(i)*time.Minute)))
	}
	return cols
}

func dualRouteHotColumns(start time.Time, hotCount int) []keyviz.MatrixColumn {
	cols := make([]keyviz.MatrixColumn, 0, hotCount+1)
	cols = append(cols, keyviz.MatrixColumn{At: start})
	for i := 1; i <= hotCount; i++ {
		cols = append(cols, dualRouteHotColumn(start.Add(time.Duration(i)*time.Minute)))
	}
	return cols
}

func hotColumn(at time.Time) keyviz.MatrixColumn {
	return keyviz.MatrixColumn{
		At: at,
		Rows: []keyviz.MatrixRow{
			testRow(1, 1, 0, 2, "a", "m", 60, 0),
			testRow(1, 1, 1, 2, "m", "z", 60, 0),
			testRow(2, 1, 0, 2, "a", "m", 60, 0),
			testRow(2, 1, 1, 2, "m", "z", 60, 0),
		},
	}
}

func hotMatrixColumn(at, start time.Time) keyviz.MatrixColumn {
	column := hotColumn(at)
	column.WindowStart = start
	return column
}

func dualRouteHotColumn(at time.Time) keyviz.MatrixColumn {
	return keyviz.MatrixColumn{
		At: at,
		Rows: []keyviz.MatrixRow{
			testRow(1, 1, 0, 2, "a", "g", 60, 0),
			testRow(1, 1, 1, 2, "g", "m", 60, 0),
			testRow(2, 1, 0, 2, "m", "t", 60, 0),
			testRow(2, 1, 1, 2, "t", "z", 60, 0),
		},
	}
}

func packTestHLC(at time.Time) uint64 {
	ms := at.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms) << kv.HLCLogicalBits
}

func requireFakeSampler(t *testing.T, scheduler *Scheduler) *fakeSampler {
	t.Helper()
	sampler, ok := scheduler.sampler.(*fakeSampler)
	require.True(t, ok)
	return sampler
}

type fakeCatalogSnapshotSource struct {
	snapshot      distribution.CatalogSnapshot
	err           error
	snapshotCalls int
	failAt        map[int]error
}

func (f *fakeCatalogSnapshotSource) Snapshot(context.Context) (distribution.CatalogSnapshot, error) {
	f.snapshotCalls++
	if err := f.failAt[f.snapshotCalls]; err != nil {
		return distribution.CatalogSnapshot{}, err
	}
	if f.err != nil {
		return distribution.CatalogSnapshot{}, f.err
	}
	return f.snapshot, nil
}

type fakeSplitter struct {
	requests          []SplitRequest
	err               error
	failAt            map[int]error
	normalizeSplitKey func([]byte) []byte
	onSuccess         func(SplitRequest, SplitResult)
}

func (f *fakeSplitter) SplitRange(_ context.Context, req SplitRequest) (SplitResult, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return SplitResult{}, f.err
	}
	if err := f.failAt[len(f.requests)]; err != nil {
		return SplitResult{}, err
	}
	splitKey := req.SplitKey
	if f.normalizeSplitKey != nil {
		splitKey = f.normalizeSplitKey(splitKey)
	}
	baseID := req.RouteID*100 + uint64(len(f.requests))*2
	result := SplitResult{
		CatalogVersion: req.ExpectedCatalogVersion + 1,
		Left: distribution.RouteDescriptor{
			RouteID: baseID, Start: distribution.CloneBytes(req.ParentStart), End: distribution.CloneBytes(splitKey),
			GroupID: req.ParentGroupID, State: distribution.RouteStateActive,
		},
		Right: distribution.RouteDescriptor{
			RouteID: baseID + 1, Start: distribution.CloneBytes(splitKey), End: distribution.CloneBytes(req.ParentEnd),
			GroupID: req.ParentGroupID, State: distribution.RouteStateActive,
		},
	}
	if f.onSuccess != nil {
		f.onSuccess(req, result)
	}
	return result, nil
}

type fakeSampler struct {
	step    time.Duration
	history int
	cols    []keyviz.MatrixColumn
}

func (f *fakeSampler) Snapshot(from, to time.Time) []keyviz.MatrixColumn {
	out := make([]keyviz.MatrixColumn, 0, len(f.cols))
	for _, col := range f.cols {
		if col.At.Before(from) || !col.At.Before(to) {
			continue
		}
		out = append(out, col)
	}
	return out
}

func (f *fakeSampler) Step() time.Duration {
	return f.step
}

func (f *fakeSampler) HistoryColumns() int {
	return f.history
}

type fakeRegistrar struct {
	registered    []uint64
	registrations []fakeRegistration
	removed       []uint64
}

type fakeRegistration struct {
	routeID uint64
	start   []byte
	end     []byte
	groupID uint64
}

func (f *fakeRegistrar) RegisterRoute(routeID uint64, start, end []byte, groupID uint64) bool {
	f.registered = append(f.registered, routeID)
	f.registrations = append(f.registrations, fakeRegistration{
		routeID: routeID,
		start:   distribution.CloneBytes(start),
		end:     distribution.CloneBytes(end),
		groupID: groupID,
	})
	return true
}

func (f *fakeRegistrar) RemoveRoute(routeID uint64) {
	f.removed = append(f.removed, routeID)
}
