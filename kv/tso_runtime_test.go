package kv

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestParseTSOMode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		raw  string
		want TSOMode
	}{
		{raw: "legacy\n", want: TSOModeLegacy},
		{raw: "SHADOW", want: TSOModeShadow},
		{raw: "cutover", want: TSOModeCutover},
		{raw: "phase-d", want: TSOModePhaseD},
		{raw: "phase_d", want: TSOModePhaseD},
	} {
		mode, err := ParseTSOMode(tc.raw)
		require.NoError(t, err)
		require.Equal(t, tc.want, mode)
	}
	_, err := ParseTSOMode("disabled")
	require.ErrorIs(t, err, ErrInvalidTSOMode)
}

func TestDynamicTimestampAllocatorPreservesLegacyFallbackUntilActivated(t *testing.T) {
	t.Parallel()
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	dynamic := NewDynamicTimestampAllocator(nil)
	coord := NewShardedCoordinator(distribution.NewEngine(), nil, 1, clock, nil).
		WithTSOAllocator(dynamic)

	allocator, ok := TimestampAllocatorThrough(coord)
	require.False(t, ok)
	require.Nil(t, allocator)
	legacyTS, err := NextTimestampThrough(context.Background(), coord, "legacy dynamic fallback")
	require.NoError(t, err)

	dedicated := &phaseDTestAllocator{next: legacyTS + 100}
	dynamic.store(dedicated)
	allocator, ok = TimestampAllocatorThrough(coord)
	require.True(t, ok)
	require.Same(t, dedicated, allocator)
	dedicatedTS, err := NextTimestampThrough(context.Background(), coord, "active dynamic allocator")
	require.NoError(t, err)
	require.Equal(t, legacyTS+100, dedicatedTS)
}

func TestDynamicTimestampAllocatorRejectsValidationWithoutAllocator(t *testing.T) {
	t.Parallel()
	dynamic := NewDynamicTimestampAllocator(nil)
	require.ErrorIs(t,
		dynamic.ValidateDurableTimestamp(context.Background(), 1),
		ErrTSOAllocatorRequired,
	)
}

func TestTSORuntimeControllerEnforcesAdjacentOneWayTransitions(t *testing.T) {
	t.Parallel()
	controller, state := newTestTSORuntimeController(t)
	require.Equal(t, TSOModeLegacy, controller.CurrentMode())
	require.Nil(t, controller.Allocator().currentTimestampAllocator())

	err := controller.ApplyMode(TSOModeCutover)
	require.ErrorIs(t, err, ErrUnsafeTSOModeTransition)
	require.Equal(t, TSOModeLegacy, controller.CurrentMode())

	require.NoError(t, controller.ApplyMode(TSOModeShadow))
	require.Equal(t, TSOModeShadow, controller.CurrentMode())
	require.ErrorIs(t, controller.ApplyMode(TSOModeLegacy), ErrTSOModeRollback)

	require.NoError(t, controller.ApplyMode(TSOModeCutover))
	_, err = controller.Allocator().Next(context.Background())
	require.NoError(t, err)
	require.True(t, state.CutoverActive())

	require.NoError(t, controller.ApplyMode(TSOModePhaseD))
	controller.Allocator().Invalidate()
	_, err = controller.Allocator().Next(context.Background())
	require.NoError(t, err)
	require.True(t, state.PhaseDActive())
	require.NoError(t, controller.ApplyMode(TSOModeCutover))
	require.Equal(t, TSOModePhaseD, controller.CurrentMode())
}

func TestTSORuntimeControllerDurableStateOverridesStaleMode(t *testing.T) {
	t.Parallel()
	state := &runtimeTSOState{}
	state.cutover.Store(true)
	controller, _ := newTestTSORuntimeControllerWithState(t, state)
	require.Equal(t, TSOModeCutover, controller.CurrentMode())
	require.NotNil(t, controller.Allocator().currentTimestampAllocator())
	require.NoError(t, controller.ApplyMode(TSOModeShadow))
	require.Equal(t, TSOModeCutover, controller.CurrentMode())

	state.phaseD.Store(true)
	require.NoError(t, controller.ApplyMode(TSOModeLegacy))
	require.Equal(t, TSOModePhaseD, controller.CurrentMode())
}

func TestTSORuntimeControllerDurableStateMaySkipLocalPhases(t *testing.T) {
	t.Parallel()
	state := &runtimeTSOState{}
	controller, _ := newTestTSORuntimeControllerWithState(t, state)

	state.cutover.Store(true)
	require.NoError(t, controller.ApplyMode(TSOModeLegacy))
	require.Equal(t, TSOModeCutover, controller.CurrentMode())

	state.phaseD.Store(true)
	require.NoError(t, controller.ApplyMode(TSOModeLegacy))
	require.Equal(t, TSOModePhaseD, controller.CurrentMode())
}

func TestTSORuntimeControllerAllowsPhaseDAfterDurableCutoverOverride(t *testing.T) {
	t.Parallel()
	state := &runtimeTSOState{}
	controller, _ := newTestTSORuntimeControllerWithState(t, state)
	require.Equal(t, TSOModeLegacy, controller.CurrentMode())

	state.cutover.Store(true)
	require.NoError(t, controller.ApplyMode(TSOModePhaseD))
	require.Equal(t, TSOModePhaseD, controller.CurrentMode())
}

func TestTSORuntimeControllerDurableCutoverOverridesLegacyBeforeReload(t *testing.T) {
	t.Parallel()
	controller, state := newTestTSORuntimeController(t)
	require.Equal(t, TSOModeLegacy, controller.CurrentMode())

	state.cutover.Store(true)
	coord := NewShardedCoordinator(distribution.NewEngine(), nil, 1, controller.shadow.legacy, nil).
		WithTSOAllocator(controller.Allocator())
	allocator, ok := TimestampAllocatorThrough(coord)
	require.True(t, ok)
	require.Same(t, controller.batch, allocator)
	activateCutover, activatePhaseD := controller.routed.activationState()
	require.True(t, activateCutover)
	require.False(t, activatePhaseD)

	_, err := NextTimestampThrough(context.Background(), coord, "durable cutover before reload")
	require.NoError(t, err)
	require.Equal(t, TSOModeLegacy, controller.CurrentMode(), "mode poll has not run")
}

func TestDurableCutoverObservationDoesNotLowerPhaseDActivation(t *testing.T) {
	t.Parallel()
	controller, state := newTestTSORuntimeController(t)
	controller.routed.setActivation(true, true)
	state.cutover.Store(true)

	require.Same(t, controller.batch, controller.Allocator().currentTimestampAllocator())
	activateCutover, activatePhaseD := controller.routed.activationState()
	require.True(t, activateCutover)
	require.True(t, activatePhaseD)
}

func TestReloadTSOModeFileRefreshesDurableStateWithoutModeChange(t *testing.T) {
	t.Parallel()
	state := &runtimeTSOState{}
	observer := &recordingRuntimeTSOObserver{}
	controller, _ := newTestTSORuntimeControllerWithObserver(t, TSOModeCutover, state, observer)
	path := filepath.Join(t.TempDir(), "tso-mode")
	writeTSOModeFile(t, path, "cutover\n")
	observer.durableCalls = 0

	state.cutover.Store(true)
	require.NoError(t, reloadTSOModeFile(path, controller))
	require.Equal(t, 1, observer.durableCalls)
	require.True(t, observer.cutoverActive)
	require.False(t, observer.phaseDActive)
}

func TestReportTSOModeReloadErrorCountsEveryFailedPoll(t *testing.T) {
	t.Parallel()
	observer := &recordingRuntimeTSOObserver{}
	controller, _ := newTestTSORuntimeControllerWithObserver(
		t,
		TSOModeLegacy,
		&runtimeTSOState{},
		observer,
	)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := errors.Wrap(ErrInvalidTSOMode, "parse tso mode file")

	lastError := reportTSOModeReloadError(context.Background(), "mode", "", err, controller, logger)
	reportTSOModeReloadError(context.Background(), "mode", lastError, err, controller, logger)
	require.Equal(t, 2, observer.reloads["parse_error"])
}

func TestRunTSOModeFileReloadAppliesSafeSequence(t *testing.T) {
	t.Parallel()
	controller, _ := newTestTSORuntimeController(t)
	path := filepath.Join(t.TempDir(), "tso-mode")
	writeTSOModeFile(t, path, "legacy\n")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunTSOModeFileReload(ctx, path, time.Millisecond, controller, slog.Default())
	}()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})

	writeTSOModeFile(t, path, "shadow\n")
	require.Eventually(t, func() bool {
		return controller.CurrentMode() == TSOModeShadow
	}, time.Second, time.Millisecond)

	writeTSOModeFile(t, path, "phase-d\n")
	require.Never(t, func() bool {
		return controller.CurrentMode() == TSOModePhaseD
	}, 20*time.Millisecond, time.Millisecond)
	require.Equal(t, TSOModeShadow, controller.CurrentMode())

	writeTSOModeFile(t, path, "cutover\n")
	require.Eventually(t, func() bool {
		return controller.CurrentMode() == TSOModeCutover
	}, time.Second, time.Millisecond)
	writeTSOModeFile(t, path, "phase-d\n")
	require.Eventually(t, func() bool {
		return controller.CurrentMode() == TSOModePhaseD
	}, time.Second, time.Millisecond)

	writeTSOModeFile(t, path, "legacy\n")
	require.Never(t, func() bool {
		return controller.CurrentMode() != TSOModePhaseD
	}, 20*time.Millisecond, time.Millisecond)
}

func writeTSOModeFile(t *testing.T, path, contents string) {
	t.Helper()
	tmp := path + ".tmp"
	require.NoError(t, os.WriteFile(tmp, []byte(contents), 0o600))
	require.NoError(t, os.Rename(tmp, path))
}

func TestTSORuntimeControllerConcurrentAllocationsDuringReload(t *testing.T) {
	controller, _ := newTestTSORuntimeController(t)
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	coord := NewShardedCoordinator(distribution.NewEngine(), nil, 1, clock, nil).
		WithTSOAllocator(controller.Allocator())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if _, err := NextTimestampThrough(ctx, coord, "runtime reload race"); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	require.NoError(t, controller.ApplyMode(TSOModeShadow))
	require.NoError(t, controller.ApplyMode(TSOModeCutover))
	require.NoError(t, controller.ApplyMode(TSOModePhaseD))
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func newTestTSORuntimeController(t *testing.T) (*TSORuntimeController, *runtimeTSOState) {
	t.Helper()
	return newTestTSORuntimeControllerWithState(t, &runtimeTSOState{})
}

func newTestTSORuntimeControllerWithState(
	t *testing.T,
	state *runtimeTSOState,
) (*TSORuntimeController, *runtimeTSOState) {
	return newTestTSORuntimeControllerWithObserver(t, TSOModeLegacy, state, nil)
}

func newTestTSORuntimeControllerWithObserver(
	t *testing.T,
	initial TSOMode,
	state *runtimeTSOState,
	observer TSOObserver,
) (*TSORuntimeController, *runtimeTSOState) {
	t.Helper()
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	local := &runtimeTSOAllocator{state: state}
	routed, err := NewLeaderRoutedTSOAllocator(local, &recordingTSOEngine{}, WithTSORoutedClock(clock))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, routed.Close()) })
	controller, err := NewTSORuntimeController(TSORuntimeControllerConfig{
		Clock:       clock,
		Routed:      routed,
		State:       state,
		BatchSize:   8,
		InitialMode: initial,
		Observer:    observer,
	})
	require.NoError(t, err)
	return controller, state
}

type recordingRuntimeTSOObserver struct {
	reloads       map[string]int
	durableCalls  int
	cutoverActive bool
	phaseDActive  bool
}

func (o *recordingRuntimeTSOObserver) ObserveTSORequest(string, string, string, time.Duration) {}
func (o *recordingRuntimeTSOObserver) ObserveTSOShadowComparison(string, uint64)               {}
func (o *recordingRuntimeTSOObserver) ObserveTSOMode(string)                                   {}

func (o *recordingRuntimeTSOObserver) ObserveTSOModeReload(result string) {
	if o.reloads == nil {
		o.reloads = make(map[string]int)
	}
	o.reloads[result]++
}

func (o *recordingRuntimeTSOObserver) ObserveTSODurableState(cutoverActive, phaseDActive bool) {
	o.durableCalls++
	o.cutoverActive = cutoverActive
	o.phaseDActive = phaseDActive
}

type runtimeTSOState struct {
	cutover atomic.Bool
	phaseD  atomic.Bool
}

func (s *runtimeTSOState) CutoverActive() bool { return s.cutover.Load() }
func (s *runtimeTSOState) PhaseDActive() bool  { return s.phaseD.Load() }

type runtimeTSOAllocator struct {
	state *runtimeTSOState
	next  atomic.Uint64
	mu    sync.Mutex
}

func (a *runtimeTSOAllocator) Next(ctx context.Context) (uint64, error) {
	return a.NextBatch(ctx, 1)
}

func (a *runtimeTSOAllocator) NextBatch(ctx context.Context, n int) (uint64, error) {
	reservation, err := a.ReserveBatchAfter(ctx, n, 0, false, false)
	return reservation.Base, err
}

func (a *runtimeTSOAllocator) NextBatchAfter(ctx context.Context, n int, min uint64) (uint64, error) {
	reservation, err := a.ReserveBatchAfter(ctx, n, min, false, false)
	return reservation.Base, err
}

func (a *runtimeTSOAllocator) ReserveBatchAfter(
	_ context.Context,
	n int,
	min uint64,
	activateCutover bool,
	activatePhaseD bool,
) (TSOReservation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if activateCutover || activatePhaseD {
		a.state.cutover.Store(true)
	}
	if activatePhaseD {
		a.state.phaseD.Store(true)
	}
	previous := a.next.Load()
	base := max(previous+1, min+1)
	end := base + uint64(n) - 1 //nolint:gosec // test uses positive bounded n.
	a.next.Store(end)
	return TSOReservation{
		Base:                    base,
		Count:                   n,
		PreviousAllocationFloor: previous,
		CutoverActive:           a.state.CutoverActive(),
		PhaseDActive:            a.state.PhaseDActive(),
	}, nil
}

func (a *runtimeTSOAllocator) ValidateDurableTimestamp(context.Context, uint64) error { return nil }
func (a *runtimeTSOAllocator) PhaseDActive() bool                                     { return a.state.PhaseDActive() }
func (a *runtimeTSOAllocator) PhaseDRequired() bool                                   { return a.state.PhaseDActive() }
func (a *runtimeTSOAllocator) IsLeader() bool                                         { return true }
func (a *runtimeTSOAllocator) RunLeaseRenewal(ctx context.Context)                    { <-ctx.Done() }
