package kv

import (
	"context"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestApplyTargetStagedReadinessAdvancesClockBeforeCutover(t *testing.T) {
	t.Parallel()
	for _, imported := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty range", true: "higher imported floor"}[imported], func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dir := t.TempDir()
			st, err := store.NewPebbleStore(dir)
			require.NoError(t, err)
			clock := NewHLC()
			fsm := NewKvFSMWithHLC(st, clock)
			fence := nonNegativeUint64(time.Now().Add(time.Hour).UnixMilli()) << HLCLogicalBits
			want := fence
			if imported {
				want += 100
				cmd, marshalErr := MarshalMigrationImportCommand(&pb.ImportRangeVersionsRequest{
					JobId: 7, BracketId: 1, BatchSeq: 1,
					Versions: []*pb.MVCCVersion{{Key: []byte("n"), CommitTs: want, Value: []byte("v")}},
				})
				require.NoError(t, marshalErr)
				require.IsType(t, store.ImportVersionsResult{}, fsm.Apply(cmd))
			}
			cmd, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{
				JobId: 7, MigrationJobId: 7, RouteStart: []byte("m"), RouteEnd: []byte("z"),
				ExpectedCutoverVersion: 2, MinWriteTsExclusive: fence, Armed: true,
			})
			require.NoError(t, err)
			require.Nil(t, fsm.Apply(cmd))
			require.Equal(t, want, st.LastCommitTS())
			require.GreaterOrEqual(t, clock.Current(), want)
			require.Greater(t, clock.Next(), fence)
			require.NoError(t, st.Close())
			reopened, err := store.NewPebbleStore(dir)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			require.Equal(t, want, reopened.LastCommitTS())
			floor, err := reopened.MigrationHLCFloor(ctx, 7)
			require.NoError(t, err)
			require.Equal(t, want, floor)
			reader, ok := reopened.(store.MigrationTargetReadinessReader)
			require.True(t, ok)
			states, err := reader.MigrationTargetReadinessStates(ctx)
			require.NoError(t, err)
			require.Len(t, states, 1)
			require.Equal(t, fence, states[0].MinWriteTSExclusive)
			restartedClock := NewHLC()
			_ = NewKvFSMWithHLC(reopened, restartedClock)
			require.Greater(t, restartedClock.Next(), fence)
		})
	}
}
