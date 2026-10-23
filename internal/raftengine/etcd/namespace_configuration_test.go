package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestNamespaceApplyMembersIncludesJointAndLearnerMembers(t *testing.T) {
	state := raftpb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{2, 3}, Learners: []uint64{4}, LearnersNext: []uint64{5}}
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, namespaceApplyMembers(state))
}

func TestNamespaceActivationRealRaftConfChangeSnapshotRestart(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "1")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	st := store.NewMVCCStore()
	fsm := kv.NewKvFSMWithHLC(st, nil)
	cfg := OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7001", DataDir: dir, Bootstrap: true,
		StateMachine: fsm, TickInterval: 10 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1}
	engine, err := Open(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.Eventually(t, func() bool { return engine.State() == raftengine.StateLeader }, 3*time.Second, 10*time.Millisecond)
	index, err := kv.MigrationConfigurationIndex(ctx, st)
	require.NoError(t, err)
	capability := kv.NamespaceMemberCapability{MemberID: 1, Version: kv.MigrationNamespaceVersion, ConfigurationIndex: index}
	command := kv.NamespaceAdmissionCommand{Version: kv.MigrationNamespaceVersion, ConfigurationIndex: index,
		ApplyMembers: []uint64{1}, Members: []kv.NamespaceMemberCapability{capability}}
	require.NoError(t, kv.ActivateMigrationNamespace(ctx, st, engine, command))
	joiningID := DeriveNodeID("n2")
	require.ErrorIs(t, kv.CheckMigrationNamespacePrejoin(ctx, st, index, joiningID, kv.MigrationNamespaceVersion), kv.ErrMigrationNamespaceProtocol)
	command.Joining = []kv.NamespaceMemberCapability{{MemberID: joiningID, Version: kv.MigrationNamespaceVersion, ConfigurationIndex: index}}
	require.NoError(t, kv.ActivateMigrationNamespace(ctx, st, engine, command))
	require.NoError(t, kv.CheckMigrationNamespacePrejoin(ctx, st, index, joiningID, kv.MigrationNamespaceVersion))
	require.ErrorIs(t, kv.CheckMigrationNamespacePrejoin(ctx, st, index, joiningID, 0), kv.ErrMigrationNamespaceProtocol)
	joinedAt, err := engine.AddLearner(ctx, "n2", "127.0.0.1:7002", 0)
	require.NoError(t, err)
	require.Greater(t, joinedAt, index)
	require.NoError(t, kv.RequireMigrationNamespaceAdmission(ctx, st))
	require.Eventually(t, func() bool { return engine.Status().LastSnapshotIndex >= joinedAt }, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, engine.Close())
	restored := store.NewMVCCStore()
	restoredFSM := kv.NewKvFSMWithHLC(restored, nil)
	cfg.StateMachine, cfg.Bootstrap = restoredFSM, false
	reopened, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer reopened.Close()
	require.NoError(t, kv.RequireMigrationNamespaceAdmission(ctx, restored))
	actual, err := kv.MigrationConfigurationIndex(ctx, restored)
	require.NoError(t, err)
	require.Equal(t, joinedAt, actual)
}
