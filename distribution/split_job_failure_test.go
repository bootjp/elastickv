package distribution

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitJobFailurePreservesExactRetryPhase(t *testing.T) {
	t.Parallel()
	for _, phase := range []SplitJobPhase{SplitJobPhasePlanned, SplitJobPhaseBackfill, SplitJobPhaseFence, SplitJobPhaseDeltaCopy, SplitJobPhaseCutover, SplitJobPhaseCleanup, SplitJobPhaseAbandoning} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			job := SplitJob{JobID: 7, SourceRouteID: 1, SplitKey: []byte("m"), TargetGroupID: 9, Phase: phase, FenceTS: 50, SourceRetentionPinTS: 1}
			if phase == SplitJobPhaseAbandoning {
				job.AbandonFromPhase = SplitJobPhaseFence
			}
			failed, err := FailSplitJobState(job, "broken phase", 10)
			require.NoError(t, err)
			encoded, err := EncodeSplitJob(failed)
			require.NoError(t, err)
			failed, err = DecodeSplitJob(encoded)
			require.NoError(t, err)
			require.Equal(t, phase, failed.RetryPhase)
			retried, err := RetrySplitJobState(failed, 20)
			require.NoError(t, err)
			require.Equal(t, phase, retried.Phase)
			require.Equal(t, job.AbandonFromPhase, retried.AbandonFromPhase)
			require.Equal(t, job.SourceRetentionPinTS, retried.SourceRetentionPinTS)
			require.Equal(t, job.FenceTS, retried.FenceTS)
			if phase == SplitJobPhaseAbandoning {
				require.Equal(t, "broken phase", retried.LastError, "compensation retries retain the original failure evidence")
			} else {
				require.Empty(t, retried.LastError)
			}
			_, err = EncodeSplitJob(retried)
			require.NoError(t, err)
		})
	}
}
