package autosplit

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

type registrationProofFunc func(uint64, uint64, []byte, []byte, time.Time, time.Time) bool

type completeProof struct {
	registrationProofFunc
	covered bool
}

func (p completeProof) ReadCoverageCoversWindow(uint64, time.Time, time.Time) bool { return p.covered }

func (f registrationProofFunc) RouteRegistrationCoversWindow(
	routeID, groupID uint64, start, end []byte, windowStart, windowEnd time.Time,
) bool {
	return f(routeID, groupID, start, end, windowStart, windowEnd)
}

func TestBuildLocalTargetEvidence(t *testing.T) {
	t.Parallel()
	start := time.Unix(1000, 0)
	end := start.Add(time.Minute)
	route := distribution.RouteDescriptor{
		RouteID: 2, GroupID: 7, State: distribution.RouteStateActive,
		Start: []byte("a"), End: []byte("z"),
	}
	col := keyviz.MatrixColumn{
		WindowStart: start, At: end,
		Rows: []keyviz.MatrixRow{
			{RouteID: 2, RaftGroupID: 7, LeaderTerm: 9, Writes: 20},
			{RouteID: 2, RaftGroupID: 7, LeaderTerm: 9, Reads: 10},
		},
	}
	registered := registrationProofFunc(func(id, group uint64, lo, hi []byte, windowStart, windowEnd time.Time) bool {
		require.Equal(t, uint64(2), id)
		require.Equal(t, uint64(7), group)
		require.Equal(t, []byte("a"), lo)
		require.Equal(t, []byte("z"), hi)
		require.Equal(t, start, windowStart)
		require.Equal(t, end, windowEnd)
		return true
	})
	serving := func(group, term uint64, windowStart, windowEnd time.Time) bool {
		require.Equal(t, uint64(7), group)
		require.Equal(t, uint64(9), term)
		require.Equal(t, start, windowStart)
		require.Equal(t, end, windowEnd)
		return true
	}
	cfg := Config{WriteWeight: 1, ReadWeight: 2}
	got := BuildLocalTargetEvidence(col, []distribution.RouteDescriptor{route}, completeProof{registered, true}, serving, cfg)
	require.Len(t, got, 1)
	require.Equal(t, float64(40), got[0].LoadOpsMin)
	require.True(t, got[0].Committed)
	require.True(t, got[0].ServedWholeWindow)
	require.True(t, got[0].RegisteredBeforeWindow)
	require.True(t, got[0].ReadCoverageComplete)
	require.Empty(t, BuildLocalTargetEvidence(col, []distribution.RouteDescriptor{route}, registered, serving, cfg),
		"registration and leadership without protocol read proof must fail closed")
	require.Empty(t, BuildLocalTargetEvidence(col, []distribution.RouteDescriptor{route}, completeProof{registered, false}, serving, cfg),
		"an explicit negative read proof must fail closed")

	tests := []struct {
		name string
		edit func(*keyviz.MatrixColumn, *distribution.RouteDescriptor)
		reg  bool
		lead bool
	}{
		{"missing row", func(c *keyviz.MatrixColumn, _ *distribution.RouteDescriptor) { c.Rows = nil }, true, true},
		{"unknown term", func(c *keyviz.MatrixColumn, _ *distribution.RouteDescriptor) { c.Rows[0].LeaderTerm = 0 }, true, true},
		{"mixed terms", func(c *keyviz.MatrixColumn, _ *distribution.RouteDescriptor) { c.Rows[1].LeaderTerm = 10 }, true, true},
		{"aggregate", func(c *keyviz.MatrixColumn, _ *distribution.RouteDescriptor) {
			c.Rows[0].Aggregate = true
			c.Rows[1].Aggregate = true
		}, true, true},
		{"route moved", func(_ *keyviz.MatrixColumn, r *distribution.RouteDescriptor) { r.GroupID = 8 }, true, true},
		{"route transitional", func(_ *keyviz.MatrixColumn, r *distribution.RouteDescriptor) {
			r.State = distribution.RouteStateMigratingSource
		}, true, true},
		{"late registration", func(*keyviz.MatrixColumn, *distribution.RouteDescriptor) {}, false, true},
		{"not locally serving", func(*keyviz.MatrixColumn, *distribution.RouteDescriptor) {}, true, false},
		{"unproven window", func(c *keyviz.MatrixColumn, _ *distribution.RouteDescriptor) { c.WindowStart = c.At }, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			copyCol := col
			copyCol.Rows = append([]keyviz.MatrixRow(nil), col.Rows...)
			copyRoute := route
			tt.edit(&copyCol, &copyRoute)
			got := BuildLocalTargetEvidence(copyCol, []distribution.RouteDescriptor{copyRoute},
				completeProof{registrationProofFunc(func(uint64, uint64, []byte, []byte, time.Time, time.Time) bool { return tt.reg }), true},
				func(uint64, uint64, time.Time, time.Time) bool { return tt.lead }, cfg)
			require.Empty(t, got)
		})
	}
}
