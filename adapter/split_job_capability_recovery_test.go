package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSplitPreflightRecoversMonotoneGuardsAfterCapabilityLoss(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	interrupted := false
	completed := r.migrate(t, 1, 2, "m", func(job distribution.SplitJob) {
		if interrupted || job.Phase != distribution.SplitJobPhaseFence || job.TransferPreflightDone {
			return
		}
		partial := false
		for _, p := range job.TransferPreflightProgress {
			partial = partial || p.Done
		}
		if !partial {
			return
		}
		interrupted = true
		r.server.migrationCapabilityGate = func(context.Context) error { return errors.New("voter temporarily unavailable") }
		require.NoError(t, r.server.RunSplitJobRunnerOnce(t.Context()))
		paused := loadTransferJob(t, t.Context(), r.server.catalog, job.JobID)
		require.True(t, paused.CapabilityRegressed)
		require.False(t, paused.TransferPreflightDone)
		r.server.migrationCapabilityGate = r.capability
		members := r.server.splitApplyMemberFactory
		r.server.splitApplyMemberFactory = func(ctx context.Context, id uint64) (SplitMigrationApplyMemberSnapshot, error) {
			snapshot, err := members(ctx, id)
			snapshot.ConfigurationIndex++
			return snapshot, err
		}
		require.Error(t, r.server.RunSplitJobRunnerOnce(t.Context()))
		mismatched := loadTransferJob(t, t.Context(), r.server.catalog, job.JobID)
		require.True(t, mismatched.CapabilityRegressed, "a healthy capability gate alone must not authorize recovery with stale member evidence")
		require.Equal(t, job.TransferPreflightProgress, mismatched.TransferPreflightProgress)
		r.server.splitApplyMemberFactory = members
		require.NoError(t, r.server.RunSplitJobRunnerOnce(t.Context()))
		recovered := loadTransferJob(t, t.Context(), r.server.catalog, job.JobID)
		require.False(t, recovered.CapabilityRegressed, "healthy members must acknowledge retained stronger guards instead of waiting for forbidden fence removal")
		require.Equal(t, job.TransferPreflightTS, recovered.TransferPreflightTS)
		reader, ok := r.groups[1].Store.(store.MigrationTargetReadinessReader)
		require.True(t, ok)
		states, err := reader.MigrationTargetReadinessStates(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, states)
		found := false
		for _, state := range states {
			if state.JobID == job.JobID {
				found = true
				require.True(t, state.SourceReadFence, "recovery must retain the fail-closed read fence")
				require.True(t, state.SourceWriteFence)
				require.True(t, state.TrackWrites)
			}
		}
		require.True(t, found, "the recovered source control must remain durable")
	})
	require.True(t, interrupted)
	require.True(t, completed.TransferPreflightDone)
	r.read(t, "n", "original-n")
	r.read(t, "y", "original-y")
	require.NoError(t, r.put(t, 2, "n", "after-recovery", completed.FenceTS+100))
	r.read(t, "n", "after-recovery")
}
