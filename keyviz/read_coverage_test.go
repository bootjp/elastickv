package keyviz

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadCoverageRequiresEnrollmentAndExactCommittedWindow(t *testing.T) {
	s, clk := newTestSampler(t, MemSamplerOptions{Step: time.Second, HistoryColumns: 5})
	var marker UnattributableClientReadMarker = s
	full := ReadCoverageEnrollment{true, true, true}

	s.Observe(2, []byte("seed"), OpWrite, 1, LabelLegacy)
	clk.Advance(time.Second)
	s.Flush()
	defaultWindow := lastSnapshotColumn(t, s)
	require.True(t, s.NoUnattributableClientReads(2, defaultWindow.WindowStart, defaultWindow.At))
	require.False(t, s.ReadCoverageCoversWindow(2, defaultWindow.WindowStart, defaultWindow.At))

	s.EnrollReadCoverage(ReadCoverageEnrollment{LocalKeyedReadsObserved: true, UnkeyedReadsMarked: true})
	clk.Advance(time.Second)
	s.Flush()
	partial := lastSnapshotColumn(t, s)
	require.False(t, s.ReadCoverageCoversWindow(2, partial.WindowStart, partial.At))

	s.EnrollReadCoverage(full)
	clk.Advance(time.Second)
	s.Flush()
	transition := lastSnapshotColumn(t, s)
	require.False(t, s.ReadCoverageCoversWindow(2, transition.WindowStart, transition.At))

	s.Observe(2, []byte("seed"), OpRead, 0, LabelLegacy)
	clk.Advance(time.Second)
	s.Flush()
	covered := lastSnapshotColumn(t, s)
	require.True(t, s.ReadCoverageCoversWindow(2, covered.WindowStart, covered.At))
	require.False(t, s.ReadCoverageCoversWindow(2, transition.WindowStart, covered.At))
	require.False(t, s.ReadCoverageCoversWindow(2, covered.WindowStart.Add(time.Nanosecond), covered.At))

	marker.MarkUnattributableClientRead(2) // HGET-like read not observed by KeyViz
	clk.Advance(time.Second)
	s.Flush()
	routeMarked := lastSnapshotColumn(t, s)
	require.False(t, s.NoUnattributableClientReads(2, routeMarked.WindowStart, routeMarked.At))
	require.False(t, s.ReadCoverageCoversWindow(2, routeMarked.WindowStart, routeMarked.At))
	require.True(t, s.ReadCoverageCoversWindow(3, routeMarked.WindowStart, routeMarked.At))

	marker.MarkUnattributableClientRead(0) // KEYS/SCAN/DBSIZE
	clk.Advance(time.Second)
	s.Flush()
	globalMarked := lastSnapshotColumn(t, s)
	require.False(t, s.ReadCoverageCoversWindow(3, globalMarked.WindowStart, globalMarked.At))
	require.True(t, s.ReadCoverageCoversWindow(2, covered.WindowStart, covered.At))

	clone := cloneColumn(globalMarked)
	clone.unattributableReads[0] = false
	require.False(t, s.ReadCoverageCoversWindow(2, globalMarked.WindowStart, globalMarked.At))
}
