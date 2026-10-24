package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrationCleanupConfChangeInterleavingAndRestart(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "1")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("n"), []byte("customer"), 100, 0))
	fsm := kv.NewKvFSMWithHLC(st, nil)
	engine, err := Open(ctx, OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7001",
		DataDir: dir, Bootstrap: true, StateMachine: fsm, TickInterval: 10 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.Eventually(t, func() bool { return engine.State() == raftengine.StateLeader }, 3*time.Second, 10*time.Millisecond)
	aware, ok := fsm.(raftengine.AppliedConfigurationAware)
	require.True(t, ok)
	require.NotZero(t, aware.AppliedConfigurationIndex(), "bootstrap ConfChange must install the durable epoch")
	control, err := kv.MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{JobId: 7, MigrationJobId: 7,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), Armed: true, SourceWriteFence: true, SourceReadFence: true,
		MinWriteTsExclusive: 100, RetentionPinTs: 1})
	require.NoError(t, err)
	_, err = engine.Propose(ctx, control)
	require.NoError(t, err)
	oldIndex := aware.AppliedConfigurationIndex()
	cleanup, err := kv.MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 7,
		Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA, RequireConfigurationIndex: true, ExpectedConfigurationIndex: oldIndex})
	require.NoError(t, err)
	index, err := engine.AddLearner(ctx, "n2", "127.0.0.1:7002", 0)
	require.NoError(t, err)
	require.Greater(t, index, oldIndex)
	require.Equal(t, index, aware.AppliedConfigurationIndex())
	result, err := engine.Propose(ctx, cleanup)
	require.NoError(t, err)
	responseErr, ok := result.Response.(error)
	require.True(t, ok)
	require.ErrorIs(t, responseErr, kv.ErrMigrationConfigurationChanged)
	reader, ok := st.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := reader.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Len(t, states, 1, "a committed membership change invalidates the old cleanup before guard removal")
	require.Equal(t, raftengine.StateLeader, engine.State(), "configuration mismatch must not halt raft")
	require.Eventually(t, func() bool { return engine.Status().LastSnapshotIndex > index }, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, engine.Close())
	restored := store.NewMVCCStore()
	restoredFSM := kv.NewKvFSMWithHLC(restored, nil)
	reopened, err := Open(ctx, OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7001",
		DataDir: dir, StateMachine: restoredFSM, TickInterval: 10 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1})
	require.NoError(t, err)
	defer reopened.Close()
	restoredAware, ok := restoredFSM.(raftengine.AppliedConfigurationAware)
	require.True(t, ok)
	require.Equal(t, index, restoredAware.AppliedConfigurationIndex(),
		"snapshot data index must not replace the actual ConfChange index")
}
