package autosplit

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/stretchr/testify/require"
)

type splitJobGateSource struct {
	jobs []distribution.SplitJob
	err  error
	ts   uint64
}

func (s *splitJobGateSource) Snapshot(context.Context) (distribution.CatalogSnapshot, error) {
	return distribution.CatalogSnapshot{}, nil
}

func (s *splitJobGateSource) ListSplitJobsAt(_ context.Context, ts uint64) ([]distribution.SplitJob, error) {
	s.ts = ts
	return s.jobs, s.err
}

type snapshotOnlyGateSource struct{}

func (snapshotOnlyGateSource) Snapshot(context.Context) (distribution.CatalogSnapshot, error) {
	return distribution.CatalogSnapshot{}, nil
}

func TestActiveSplitJobRouteIDsPhases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		phase   distribution.SplitJobPhase
		exclude bool
	}{
		{"planned", distribution.SplitJobPhasePlanned, true},
		{"backfill", distribution.SplitJobPhaseBackfill, true},
		{"fence", distribution.SplitJobPhaseFence, true},
		{"delta copy", distribution.SplitJobPhaseDeltaCopy, true},
		{"cutover", distribution.SplitJobPhaseCutover, true},
		{"cleanup", distribution.SplitJobPhaseCleanup, true},
		{"failed", distribution.SplitJobPhaseFailed, true},
		{"abandoning", distribution.SplitJobPhaseAbandoning, true},
		{"done", distribution.SplitJobPhaseDone, false},
		{"abandoned", distribution.SplitJobPhaseAbandoned, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := &splitJobGateSource{jobs: []distribution.SplitJob{{SourceRouteID: 10, Phase: tc.phase}}}
			got, err := activeSplitJobRouteIDs(context.Background(), source,
				distribution.CatalogSnapshot{ReadTS: 123})
			require.NoError(t, err)
			require.Equal(t, uint64(123), source.ts)
			if tc.exclude {
				require.Contains(t, got, uint64(10))
			} else {
				require.Empty(t, got)
			}
		})
	}
}

func TestActiveSplitJobRouteIDsFailsClosed(t *testing.T) {
	t.Parallel()
	snapshot := distribution.CatalogSnapshot{ReadTS: 456}

	got, err := activeSplitJobRouteIDs(context.Background(), snapshotOnlyGateSource{}, snapshot)
	require.ErrorContains(t, err, "does not support ListSplitJobsAt")
	require.Nil(t, got)

	readErr := errors.New("catalog unavailable")
	source := &splitJobGateSource{err: readErr}
	got, err = activeSplitJobRouteIDs(context.Background(), source, snapshot)
	require.ErrorIs(t, err, readErr)
	require.Nil(t, got)
	require.Equal(t, snapshot.ReadTS, source.ts)
}

func TestActiveSplitJobRouteIDsWorkingRoute(t *testing.T) {
	t.Parallel()
	source := &splitJobGateSource{jobs: []distribution.SplitJob{{
		SourceRouteID: 10, WorkingSourceRouteID: 11, Phase: distribution.SplitJobPhaseBackfill,
	}}}
	got, err := activeSplitJobRouteIDs(context.Background(), source,
		distribution.CatalogSnapshot{ReadTS: 123})
	require.NoError(t, err)
	require.Contains(t, got, uint64(10))
	require.Contains(t, got, uint64(11))
	require.NotContains(t, got, uint64(12))
}
