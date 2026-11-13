package keyviz

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRouteRegistrationCoversCommittedWindow(t *testing.T) {
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 4})
	start, end := []byte("a"), []byte("z")
	require.True(t, s.RegisterRoute(1, start, end, 7))
	start[0] = 'x' // registration must own its descriptor
	clk.Advance(time.Second)
	s.Flush()
	first := lastSnapshotColumn(t, s)
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), end, first.WindowStart, first.At))

	clk.Advance(time.Second)
	s.Flush() // no Observe; provenance must not fabricate a row
	second := lastSnapshotColumn(t, s)
	require.Empty(t, second.Rows)
	require.True(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), end, second.WindowStart, second.At))
	s.routesMu.Lock()
	reg := s.routeRegistrations[1]
	reg.registeredAt = second.WindowStart
	s.routeRegistrations[1] = reg
	s.routesMu.Unlock()
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), end, second.WindowStart, second.At))
	s.routesMu.Lock()
	reg.registeredAt = first.WindowStart
	s.routeRegistrations[1] = reg
	s.routesMu.Unlock()
	require.False(t, s.RouteRegistrationCoversWindow(1, 8, []byte("a"), end, second.WindowStart, second.At))
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, []byte("b"), end, second.WindowStart, second.At))
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), []byte("y"), second.WindowStart, second.At))
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), end, first.WindowStart, second.At))
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), end, second.At, second.WindowStart))

	// An idempotent registration does not reset the original generation.
	require.True(t, s.RegisterRoute(1, []byte("different"), end, 8))
	require.True(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), end, second.WindowStart, second.At))
	s.Flush() // duplicate bounds from a repeated timestamp are ambiguous
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), end, second.WindowStart, second.At))
}

func TestRouteRegistrationInsideWindowAndReregistration(t *testing.T) {
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 5})
	clk.Advance(time.Second)
	s.Flush()
	start, end := []byte("a"), []byte("z")
	require.True(t, s.RegisterRoute(1, start, end, 7))
	clk.Advance(time.Second)
	s.Flush()
	inside := lastSnapshotColumn(t, s)
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, start, end, inside.WindowStart, inside.At))

	clk.Advance(time.Second)
	s.Flush()
	before := lastSnapshotColumn(t, s)
	require.True(t, s.RouteRegistrationCoversWindow(1, 7, start, end, before.WindowStart, before.At))
	s.RemoveRoute(1)
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, start, end, before.WindowStart, before.At))
	require.True(t, s.RegisterRoute(1, start, end, 7))
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, start, end, before.WindowStart, before.At))
	clk.Advance(time.Second)
	s.Flush()
	rejoined := lastSnapshotColumn(t, s)
	require.False(t, s.RouteRegistrationCoversWindow(1, 7, start, end, rejoined.WindowStart, rejoined.At))
	clk.Advance(time.Second)
	s.Flush()
	stable := lastSnapshotColumn(t, s)
	require.True(t, s.RouteRegistrationCoversWindow(1, 7, start, end, stable.WindowStart, stable.At))
}

func TestCoarsenedRouteHasNoRegistrationProof(t *testing.T) {
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 3, MaxTrackedRoutes: 1})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("m"), 7))
	require.False(t, s.RegisterRoute(2, []byte("m"), []byte("z"), 7))
	clk.Advance(time.Second)
	s.Flush()
	clk.Advance(time.Second)
	s.Flush()
	col := lastSnapshotColumn(t, s)
	require.True(t, s.RouteRegistrationCoversWindow(1, 7, []byte("a"), []byte("m"), col.WindowStart, col.At))
	require.False(t, s.RouteRegistrationCoversWindow(2, 7, []byte("m"), []byte("z"), col.WindowStart, col.At))
}

func TestCommittedWindowProofAfterHistoryWrap(t *testing.T) {
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 2})
	require.True(t, s.RegisterRoute(1, []byte("a"), []byte("m"), 7))
	require.True(t, s.RegisterRoute(2, []byte("m"), []byte("z"), 7))
	s.EnrollReadCoverage(ReadCoverageEnrollment{
		LocalKeyedReadsObserved: true, ForwardedReadsObserved: true, UnkeyedReadsMarked: true,
	})
	for range 3 {
		clk.Advance(time.Second)
		s.Flush()
	}
	col := lastSnapshotColumn(t, s)
	proof, ok := s.ProofForCommittedWindow(col.WindowStart, col.At)
	require.True(t, ok)
	require.True(t, proof.RouteRegistrationCovers(1, 7, []byte("a"), []byte("m")))
	require.True(t, proof.RouteRegistrationCovers(2, 7, []byte("m"), []byte("z")))
	require.True(t, proof.ReadCoverageCovers(1))
	s.RemoveRoute(2)
	require.False(t, proof.RouteRegistrationCovers(2, 7, []byte("m"), []byte("z")))
	s.Flush() // duplicate bounds are ambiguous
	_, ok = s.ProofForCommittedWindow(col.WindowStart, col.At)
	require.False(t, ok)
}
