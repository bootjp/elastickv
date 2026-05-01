package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestBuildKeyVizSamplerHonorsEnabledFlag pins the on/off contract:
// --keyvizEnabled=false returns nil (so coordinator/admin server take
// the disabled paths), and --keyvizEnabled=true with explicit options
// returns a configured sampler whose options match every flag.
func TestBuildKeyVizSamplerHonorsEnabledFlag(t *testing.T) {
	t.Parallel()
	withFlags(t, false, time.Second, 5, 7, 16, func() {
		require.Nil(t, buildKeyVizSampler())
	})
	withFlags(t, true, 250*time.Millisecond, 5, 7, 16, func() {
		s := buildKeyVizSampler()
		require.NotNil(t, s)
		require.Equal(t, 250*time.Millisecond, s.Step())
		// Pin the --keyvizHistoryColumns forwarding so a future
		// refactor that drops the flag from buildKeyVizSampler is
		// caught here, not at runtime.
		require.Equal(t, 16, s.HistoryColumns())
	})
}

// TestSeedKeyVizRoutesCopiesEngineCatalogue pins that the startup
// seed registers each route the engine reports, so subsequent
// Observe(routeID, ...) calls find a slot. Uses a single route via
// UpdateRoute (which leaves RouteID=0) — the deeper invariant
// (one slot per distinct RouteID) is covered by the keyviz package's
// own unit tests.
func TestSeedKeyVizRoutesCopiesEngineCatalogue(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)

	s := keyviz.NewMemSampler(keyviz.MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	seedKeyVizRoutes(s, engine)

	for _, r := range engine.Stats() {
		s.Observe(r.RouteID, keyviz.OpRead, 1, 1)
	}
	s.Flush()
	cols := s.Snapshot(time.Time{}, time.Time{})
	require.Len(t, cols, 1)
	require.Len(t, cols[0].Rows, 1)
	require.Equal(t, []byte("a"), cols[0].Rows[0].Start)
}

// TestSeedKeyVizRoutesNoOpOnNilSampler pins that a disabled sampler
// is safe to seed — the function returns without panicking.
func TestSeedKeyVizRoutesNoOpOnNilSampler(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 1)
	require.NotPanics(t, func() {
		seedKeyVizRoutes(nil, engine)
	})
}

// TestStartKeyVizFlusherReturnsAfterCancel pins the goroutine
// lifecycle: when ctx fires the RunFlusher returns and the errgroup
// closure exits cleanly. Also verifies that a final Flush is called
// so the in-progress step is harvested.
func TestStartKeyVizFlusherReturnsAfterCancel(t *testing.T) {
	t.Parallel()
	s := keyviz.NewMemSampler(keyviz.MemSamplerOptions{Step: time.Millisecond, HistoryColumns: 4})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("b"), 0))
	s.Observe(1, keyviz.OpRead, 0, 0)

	ctx, cancel := context.WithCancel(context.Background())
	eg, _ := errgroup.WithContext(ctx)
	startKeyVizFlusher(ctx, eg, s)
	cancel()
	require.NoError(t, eg.Wait())
	// After cancel, the final Flush should have harvested the
	// pre-cancel Observe into the ring buffer.
	cols := s.Snapshot(time.Time{}, time.Time{})
	saw := false
	for _, c := range cols {
		for _, r := range c.Rows {
			if r.RouteID == 1 && r.Reads > 0 {
				saw = true
			}
		}
	}
	require.True(t, saw, "post-cancel Flush did not harvest pending Observe")
}

// TestPublishLeaderTermsFromSnapshotsStampsRows pins the bridge
// between the main.go ticker and the keyviz sampler: a published
// snapshot for a group surfaces on every subsequent flushed row for
// routes registered with that groupID. The snapshot publish is the
// only mechanism that takes RaftGroupID/LeaderTerm from a non-zero
// "term not tracked" baseline to a useful per-term dedupe key in the
// fan-out aggregator (PR-3c).
func TestPublishLeaderTermsFromSnapshotsStampsRows(t *testing.T) {
	t.Parallel()
	s := keyviz.NewMemSampler(keyviz.MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("b"), 7))
	publishLeaderTermsFromSnapshots(s, []groupTermSnapshot{
		{groupID: 7, term: 42},
	})
	s.Observe(1, keyviz.OpWrite, 16, 64)
	s.Flush()
	cols := s.Snapshot(time.Time{}, time.Time{})
	require.Len(t, cols, 1)
	require.Len(t, cols[0].Rows, 1)
	require.Equal(t, uint64(7), cols[0].Rows[0].RaftGroupID)
	require.Equal(t, uint64(42), cols[0].Rows[0].LeaderTerm)
}

// TestPublishLeaderTermsFromSnapshotsNilSafe pins the
// nil-receiver-safe contract: main.go can call the publish step
// before the sampler is constructed (or when keyviz is disabled)
// without panicking.
func TestPublishLeaderTermsFromSnapshotsNilSafe(t *testing.T) {
	t.Parallel()
	require.NotPanics(t, func() {
		publishLeaderTermsFromSnapshots(nil, []groupTermSnapshot{{groupID: 1, term: 2}})
	})
}

// TestStartKeyVizLeaderTermPublisherSkipsWhenSamplerNil pins that
// the goroutine is not launched when the sampler is disabled — the
// errgroup must close cleanly without a hanging goroutine.
func TestStartKeyVizLeaderTermPublisherSkipsWhenSamplerNil(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, _ := errgroup.WithContext(ctx)
	startKeyVizLeaderTermPublisher(ctx, eg, nil, []*raftGroupRuntime{{spec: groupSpec{id: 1}}})
	require.NoError(t, eg.Wait(),
		"errgroup must close immediately because no goroutine should have launched")
}

// TestStartKeyVizLeaderTermPublisherSkipsWhenNoRuntimes pins the
// other no-op branch: a configured sampler but zero runtimes should
// not launch a goroutine that would just spin doing nothing.
func TestStartKeyVizLeaderTermPublisherSkipsWhenNoRuntimes(t *testing.T) {
	t.Parallel()
	s := keyviz.NewMemSampler(keyviz.MemSamplerOptions{Step: time.Millisecond, HistoryColumns: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, _ := errgroup.WithContext(ctx)
	startKeyVizLeaderTermPublisher(ctx, eg, s, nil)
	require.NoError(t, eg.Wait())
}

func withFlags(
	t *testing.T,
	enabled bool,
	step time.Duration,
	maxTracked, maxMembers, historyColumns int,
	fn func(),
) {
	t.Helper()
	prevEnabled := *keyvizEnabled
	prevStep := *keyvizStep
	prevMaxTracked := *keyvizMaxTrackedRoutes
	prevMaxMembers := *keyvizMaxMemberRoutesPerSlot
	prevHistoryColumns := *keyvizHistoryColumns
	*keyvizEnabled = enabled
	*keyvizStep = step
	*keyvizMaxTrackedRoutes = maxTracked
	*keyvizMaxMemberRoutesPerSlot = maxMembers
	*keyvizHistoryColumns = historyColumns
	defer func() {
		*keyvizEnabled = prevEnabled
		*keyvizStep = prevStep
		*keyvizMaxTrackedRoutes = prevMaxTracked
		*keyvizMaxMemberRoutesPerSlot = prevMaxMembers
		*keyvizHistoryColumns = prevHistoryColumns
	}()
	fn()
}
