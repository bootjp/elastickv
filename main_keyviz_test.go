package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

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
	s.Observe(1, make([]byte, 16), keyviz.OpWrite, 64, keyviz.LabelLegacy)
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
