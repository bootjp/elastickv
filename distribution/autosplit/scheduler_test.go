package autosplit

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCommittedWindowsFromColumnsDerivesDurations(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0)
	cols := []keyviz.MatrixColumn{
		{At: base.Add(2 * time.Minute)},
		{At: base},
		{At: base.Add(time.Minute)},
	}

	windows := CommittedWindowsFromColumns(cols)

	require.Len(t, windows, 2)
	require.Equal(t, base.Add(time.Minute), windows[0].Column.At)
	require.Equal(t, time.Minute, windows[0].Duration)
	require.Equal(t, base.Add(2*time.Minute), windows[1].Column.At)
	require.Equal(t, time.Minute, windows[1].Duration)
}

func TestSchedulerTickSchedulesSplitRange(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeSnapshotSource{snapshot: distribution.CatalogSnapshot{
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
	source := &fakeSnapshotSource{snapshot: distribution.CatalogSnapshot{
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
}

func TestSchedulerKillSwitchSkipsSplitRange(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 7,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
	}}
	splitter := &fakeSplitter{}
	scheduler := newTestScheduler(source, splitter, hotColumns(now, 2), func(context.Context) bool {
		return true
	})
	scheduler.wasLeader = true

	result, err := scheduler.Tick(context.Background(), now.Add(2*time.Minute))

	require.NoError(t, err)
	require.True(t, result.KillSwitch)
	require.Equal(t, 0, result.Scheduled)
	require.Empty(t, splitter.requests)
}

func TestSchedulerFailedSplitDoesNotSeedCooldown(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	source := &fakeSnapshotSource{snapshot: distribution.CatalogSnapshot{
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
	source := &fakeSnapshotSource{snapshot: distribution.CatalogSnapshot{
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
	source := &fakeSnapshotSource{snapshot: distribution.CatalogSnapshot{
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
	source := &fakeSnapshotSource{snapshot: distribution.CatalogSnapshot{
		Version: 1,
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "m")},
	}}
	registrar := &fakeRegistrar{}
	scheduler := newTestScheduler(source, &fakeSplitter{}, nil, nil)
	scheduler.registrar = registrar
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

func newTestScheduler(source *fakeSnapshotSource, splitter *fakeSplitter, cols []keyviz.MatrixColumn, killSwitch KillSwitch) *Scheduler {
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

type fakeSnapshotSource struct {
	snapshot distribution.CatalogSnapshot
	err      error
}

func (f *fakeSnapshotSource) Snapshot(context.Context) (distribution.CatalogSnapshot, error) {
	if f.err != nil {
		return distribution.CatalogSnapshot{}, f.err
	}
	return f.snapshot, nil
}

type fakeSplitter struct {
	requests []SplitRequest
	err      error
}

func (f *fakeSplitter) SplitRange(_ context.Context, req SplitRequest) (SplitResult, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return SplitResult{}, f.err
	}
	return SplitResult{CatalogVersion: req.ExpectedCatalogVersion + 1}, nil
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
	registered []uint64
	removed    []uint64
}

func (f *fakeRegistrar) RegisterRoute(routeID uint64, _, _ []byte, _ uint64) bool {
	f.registered = append(f.registered, routeID)
	return true
}

func (f *fakeRegistrar) RemoveRoute(routeID uint64) {
	f.removed = append(f.removed, routeID)
}
