package autosplit

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type crossGroupCatalogSource struct {
	*fakeCatalogSnapshotSource
	jobs []distribution.SplitJob
	ts   uint64
}

func (s *crossGroupCatalogSource) ListSplitJobsAt(_ context.Context, ts uint64) ([]distribution.SplitJob, error) {
	s.ts = ts
	return s.jobs, nil
}

type crossGroupSampler struct {
	*fakeSampler
	registered bool
}

func (s *crossGroupSampler) RouteRegistrationCoversWindow(uint64, uint64, []byte, []byte, time.Time, time.Time) bool {
	return s.registered
}

func TestSchedulerCrossGroupTargetRequiresCompleteEvidence(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0)
	routes := []distribution.RouteDescriptor{
		testRoute(1, 1, "a", "z"),
		testRoute(2, 2, "z", "zz"),
	}
	cols := []keyviz.MatrixColumn{{At: base}}
	for i := 1; i <= 2; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		cols = append(cols, keyviz.MatrixColumn{
			WindowStart: at.Add(-time.Minute), At: at,
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, RaftGroupID: 1, LeaderTerm: 9, Start: []byte("a"), End: []byte("m"), SubBucketCount: 2, Writes: 40},
				{RouteID: 1, RaftGroupID: 1, LeaderTerm: 9, Start: []byte("m"), End: []byte("z"), SubBucket: 1, SubBucketCount: 2, Writes: 80},
				{RouteID: 2, RaftGroupID: 2, LeaderTerm: 9, Start: []byte("z"), End: []byte("zz"), Writes: 5},
			},
		})
	}
	tests := []struct {
		name        string
		registered  bool
		term        uint64
		jobs        []distribution.SplitJob
		wantTarget  uint64
		wantCalls   int
		wantUnknown int
	}{
		{"complete evidence", true, 9, nil, 2, 1, 0},
		{"registration unknown", false, 9, nil, 0, 1, 1},
		{"leadership changed", true, 10, nil, 0, 0, 0},
		{"migration already active", true, 9, []distribution.SplitJob{{SourceRouteID: 1, Phase: distribution.SplitJobPhaseBackfill}}, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			source := &crossGroupCatalogSource{fakeCatalogSnapshotSource: &fakeCatalogSnapshotSource{
				snapshot: distribution.CatalogSnapshot{Version: 7, ReadTS: 123, Routes: routes},
			}, jobs: tt.jobs}
			splitter := &fakeSplitter{}
			observer, ok := NewPrometheusObserver(prometheus.NewRegistry()).(*prometheusObserver)
			require.True(t, ok)
			sampler := &crossGroupSampler{fakeSampler: &fakeSampler{step: time.Minute, history: 16, cols: cols}, registered: tt.registered}
			cfg := SchedulerConfig{
				Enabled: true, CrossGroupEnabled: true, CandidateGroupIDs: []uint64{1, 2},
				Detector:     Config{WriteWeight: 1, ThresholdOpsMin: 50, CandidateWindows: 2, MaxRoutes: 8, MaxSplitsPerCycle: 1},
				EvalInterval: time.Minute, SplitCooldown: 10 * time.Minute,
				GroupLeadership: func(uint64) (bool, uint64) { return true, tt.term },
				Observer:        observer,
			}
			scheduler := NewScheduler(cfg, source, splitter, sampler, nil)
			scheduler.wasLeader = true
			scheduler.leaderStartedAt = base.Add(-time.Minute)
			scheduler.groupLeadership[1] = groupLeadershipState{leader: true, term: 9, startedAt: base.Add(-time.Minute)}
			scheduler.groupLeadership[2] = groupLeadershipState{leader: true, term: 9, startedAt: base.Add(-time.Minute)}
			result, err := scheduler.Tick(context.Background(), base.Add(2*time.Minute))
			require.NoError(t, err)
			require.Equal(t, uint64(123), source.ts)
			require.Len(t, splitter.requests, tt.wantCalls)
			if tt.wantCalls > 0 {
				require.Equal(t, tt.wantTarget, splitter.requests[0].TargetGroupID)
			}
			require.Equal(t, tt.wantCalls, result.Scheduled)
			require.Equal(t, float64(tt.wantUnknown), testutil.ToFloat64(observer.targetSkipped.WithLabelValues("load_unknown")))
		})
	}
}
