package autosplit

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

func TestTargetAdmissionRequiresObservedReadsInCommittedWindow(t *testing.T) {
	now := time.Unix(1_700_100_000, 0)
	sampler := keyviz.NewMemSampler(keyviz.MemSamplerOptions{
		Step: time.Minute, Now: func() time.Time { return now },
	})
	routes := []distribution.RouteDescriptor{
		testRoute(1, 1, "a", "m"),
		testRoute(2, 2, "m", "z"),
	}
	for _, route := range routes {
		require.True(t, sampler.RegisterRoute(route.RouteID, route.Start, route.End, route.GroupID))
		sampler.SetLeaderTerm(route.GroupID, 9)
	}
	advance := func() keyviz.MatrixColumn {
		now = now.Add(time.Minute)
		sampler.Flush()
		cols := sampler.Snapshot(time.Time{}, time.Time{})
		return cols[len(cols)-1]
	}
	advance() // establish a committed lower boundary
	sampler.EnrollReadCoverage(keyviz.ReadCoverageEnrollment{
		LocalKeyedReadsObserved: true,
		ForwardedReadsObserved:  true,
		UnkeyedReadsMarked:      true,
	})
	advance() // enrollment during this window cannot prove it

	selectTarget := func(col keyviz.MatrixColumn) uint64 {
		evidence := BuildLocalTargetEvidence(col, routes, sampler,
			func(uint64, uint64, time.Time, time.Time) bool { return true },
			Config{ReadWeight: 1, WriteWeight: 1})
		return SelectTargetGroup(TargetSelectionInput{
			Enabled: true, Decision: Decision{
				RouteGroupID: 1, SplitOrigin: SplitOriginP50Mid,
				SplitKey: []byte("h"), RouteEnd: []byte("m"),
				LeftLoad: 40, RightLoad: 60,
			},
			P50LoadKnown: true, CandidateGroupIDs: []uint64{1, 2},
			Routes: routes, Evidence: evidence, WindowStart: col.WindowStart,
			WindowEnd: col.At, Now: col.At, MaxEvidenceAge: time.Minute,
		})
	}
	seedWrites := func() {
		for range 100 {
			sampler.Observe(1, []byte("b"), keyviz.OpWrite, 1, keyviz.LabelLegacy)
		}
		sampler.Observe(2, []byte("n"), keyviz.OpWrite, 1, keyviz.LabelLegacy)
	}
	seedWrites()
	sampler.MarkUnattributableClientRead(2) // HGET-like read bypassing Observe
	marked := advance()
	require.Zero(t, selectTarget(marked))

	seedWrites()
	sampler.Observe(2, []byte("n"), keyviz.OpRead, 0, keyviz.LabelLegacy)
	covered := advance()
	require.Equal(t, uint64(2), selectTarget(covered))
	require.Zero(t, selectTarget(keyviz.MatrixColumn{WindowStart: marked.WindowStart, At: covered.At, Rows: covered.Rows}))
}
