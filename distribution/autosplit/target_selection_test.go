package autosplit

import (
	"math"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/stretchr/testify/require"
)

func TestSelectTargetGroup(t *testing.T) {
	t.Parallel()
	end := time.Unix(300, 0)
	start := end.Add(-time.Minute)
	route := func(id, group uint64, state distribution.RouteState) distribution.RouteDescriptor {
		return distribution.RouteDescriptor{RouteID: id, GroupID: group, State: state}
	}
	row := func(id, group uint64, load float64) TargetRouteEvidence {
		return TargetRouteEvidence{
			RouteID: id, GroupID: group, WindowStart: start, WindowEnd: end,
			LoadOpsMin: load, Committed: true, ServedWholeWindow: true,
			RegisteredBeforeWindow: true,
			ReadCoverageComplete:   true,
		}
	}
	base := func() TargetSelectionInput {
		return TargetSelectionInput{
			Enabled: true, Decision: Decision{
				RouteGroupID: 1, SplitOrigin: SplitOriginP50Mid,
				SplitKey: []byte("n"), RouteEnd: []byte("z"),
				LeftLoad: 40, RightLoad: 60,
			},
			P50LoadKnown: true, CandidateGroupIDs: []uint64{1, 2, 3},
			Routes: []distribution.RouteDescriptor{
				route(10, 1, distribution.RouteStateActive),
				route(20, 2, distribution.RouteStateActive),
				route(30, 3, distribution.RouteStateActive),
			},
			Evidence:    []TargetRouteEvidence{row(20, 2, 5), row(30, 3, 8), row(10, 1, 100)},
			WindowStart: start, WindowEnd: end, Now: end.Add(time.Second),
			MaxEvidenceAge: time.Minute,
		}
	}

	tests := []struct {
		name string
		edit func(*TargetSelectionInput)
		want uint64
	}{
		{"least loaded authoritative group", func(*TargetSelectionInput) {}, 2},
		{"opt in required", func(in *TargetSelectionInput) { in.Enabled = false }, 0},
		{"source excluded", func(in *TargetSelectionInput) { in.CandidateGroupIDs = []uint64{1} }, 0},
		{"missing idle route row is unknown", func(in *TargetSelectionInput) { in.Evidence = in.Evidence[1:] }, 3},
		{"one of two active routes missing", func(in *TargetSelectionInput) {
			in.Routes = append(in.Routes, route(21, 2, distribution.RouteStateActive))
		}, 3},
		{"ingress or follower row", func(in *TargetSelectionInput) { in.Evidence[0].ServedWholeWindow = false }, 3},
		{"membership registered late", func(in *TargetSelectionInput) { in.Evidence[0].RegisteredBeforeWindow = false }, 3},
		{"uncommitted row", func(in *TargetSelectionInput) { in.Evidence[0].Committed = false }, 3},
		{"unobserved HGET with seeded write", func(in *TargetSelectionInput) {
			in.CandidateGroupIDs = []uint64{2}
			in.Evidence[0].ReadCoverageComplete = false
		}, 0},
		{"no coverage proof", func(in *TargetSelectionInput) {
			for i := range in.Evidence {
				in.Evidence[i].ReadCoverageComplete = false
			}
		}, 0},
		{"source load unknown", func(in *TargetSelectionInput) { in.Evidence = in.Evidence[:2] }, 0},
		{"source group route missing", func(in *TargetSelectionInput) {
			in.Routes = append(in.Routes, route(11, 1, distribution.RouteStateActive))
		}, 0},
		{"target not lower than source", func(in *TargetSelectionInput) {
			in.Evidence[0].LoadOpsMin = 100
			in.Evidence[1].LoadOpsMin = 101
		}, 0},
		{"stale window", func(in *TargetSelectionInput) { in.Now = end.Add(2 * time.Minute) }, 0},
		{"different committed window", func(in *TargetSelectionInput) { in.Evidence[0].WindowStart = start.Add(time.Second) }, 3},
		{"duplicate route reports unknown", func(in *TargetSelectionInput) { in.Evidence = append(in.Evidence, row(20, 2, 0)) }, 3},
		{"non-finite load unknown", func(in *TargetSelectionInput) { in.Evidence[0].LoadOpsMin = math.NaN() }, 3},
		{"zero live routes explicitly listed", func(in *TargetSelectionInput) {
			in.CandidateGroupIDs = append(in.CandidateGroupIDs, 4)
		}, 4},
		{"empty group needs fresh window", func(in *TargetSelectionInput) {
			in.CandidateGroupIDs = []uint64{4}
			in.Now = end.Add(2 * time.Minute)
		}, 0},
		{"transitional route excludes group", func(in *TargetSelectionInput) {
			in.Routes = append(in.Routes, route(21, 2, distribution.RouteStateMigratingTarget))
		}, 3},
		{"transitional group is not idle", func(in *TargetSelectionInput) {
			in.CandidateGroupIDs = []uint64{4}
			in.Routes = append(in.Routes, route(40, 4, distribution.RouteStateMigratingTarget))
		}, 0},
		{"unlisted empty group excluded", func(in *TargetSelectionInput) { in.Evidence = nil }, 0},
		{"tie breaks by group id", func(in *TargetSelectionInput) {
			in.Evidence[1].LoadOpsMin = 5
			in.CandidateGroupIDs = []uint64{3, 2}
		}, 2},
		{"compound isolation", func(in *TargetSelectionInput) { in.Decision.SplitOrigin = SplitOriginIsolationCompound }, 0},
		{"lower edge singleton", func(in *TargetSelectionInput) { in.Decision.SplitOrigin = SplitOriginIsolationSingleLowerEdge }, 0},
		{"first bucket p50", func(in *TargetSelectionInput) { in.Decision.SplitOrigin = SplitOriginP50FirstBucketHi }, 0},
		{"left hot p50", func(in *TargetSelectionInput) { in.Decision.LeftLoad = 61 }, 0},
		{"unknown p50", func(in *TargetSelectionInput) { in.P50LoadKnown = false }, 0},
		{"invalid p50 estimate", func(in *TargetSelectionInput) { in.Decision.RightLoad = math.Inf(1) }, 0},
		{"reserved migration namespace", func(in *TargetSelectionInput) {
			in.Decision.SplitKey = []byte("!a")
			in.Decision.RouteEnd = []byte("!z")
		}, 0},
		{"resolver owned SQS route", func(in *TargetSelectionInput) {
			in.Decision.SplitKey = []byte("!sqs|route|g")
			in.Decision.RouteEnd = []byte("!sqs|route|h")
		}, 0},
		{"last bucket right hot", func(in *TargetSelectionInput) { in.Decision.SplitOrigin = SplitOriginP50LastBucketLo }, 2},
		{"upper edge singleton complete", func(in *TargetSelectionInput) { in.Decision.SplitOrigin = SplitOriginIsolationSingleUpperEdge }, 2},
		{"upper edge singleton incomplete", func(in *TargetSelectionInput) {
			in.Decision.SplitOrigin = SplitOriginIsolationSingleUpperEdge
			in.Evidence = nil
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			in := base()
			tt.edit(&in)
			require.Equal(t, tt.want, SelectTargetGroup(in))
		})
	}
}
