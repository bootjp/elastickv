package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTargetReadinessAtomicallyAdvancesClockFloor(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		writer, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		state := TargetStagedReadinessState{
			JobID: 7, MigrationJobID: 7, RouteStart: []byte("m"), RouteEnd: []byte("z"),
			Armed: true, MinWriteTSExclusive: 100,
		}
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(ctx, state, 77))
		require.Equal(t, uint64(100), st.LastCommitTS())
		floor, err := st.MigrationHLCFloor(ctx, 7)
		require.NoError(t, err)
		require.Equal(t, uint64(100), floor)
		state.MinWriteTSExclusive = 50
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(ctx, state, 78))
		require.Equal(t, uint64(100), st.LastCommitTS())
		floor, err = st.MigrationHLCFloor(ctx, 7)
		require.NoError(t, err)
		require.Equal(t, uint64(100), floor)
		if reader, ok := st.(interface{ LastAppliedIndex() (uint64, bool, error) }); ok {
			index, found, readErr := reader.LastAppliedIndex()
			require.NoError(t, readErr)
			require.True(t, found)
			require.Equal(t, uint64(78), index)
		}
	})
}
