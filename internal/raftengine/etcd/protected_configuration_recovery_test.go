package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestProtectedReceivedSnapshotRetainsConfigurationProof(t *testing.T) {
	for _, tc := range []struct {
		name    string
		index   uint64
		members []uint64
		valid   bool
	}{
		{"data snapshot", 4, []uint64{1, 2, 3}, true},
		{"missing epoch", 0, []uint64{1, 2, 3}, false},
		{"future epoch", 11, []uint64{1, 2, 3}, false},
		{"wrong membership", 4, []uint64{1, 2}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &recoveryProofFSM{}
			require.NoError(t, source.RecordAppliedNamespaceConfiguration(tc.index, tc.members))
			source.Apply([]byte("received data"))
			s, err := source.Snapshot()
			require.NoError(t, err)
			defer s.Close()
			e := identityElectionEngine(t, 1)
			e.dataDir = t.TempDir()
			e.fsmSnapDir = t.TempDir()
			crc, err := writeFSMSnapshotFile(s, e.fsmSnapDir, 10)
			require.NoError(t, err)
			restored := &recoveryProofFSM{}
			e.fsm = restored
			ready, calls := false, 0
			e.admissionChanged = func() { ready = false }
			e.recoveredAdmission = func(state RecoveredState) error {
				calls++
				require.Equal(t, uint64(4), state.ConfigurationIndex())
				require.Equal(t, uint64(10), state.AppliedIndex())
				ready = true
				return nil
			}
			snapshot := raftTestSnapshot(10, 3, []uint64{1, 2, 3}, encodeSnapshotToken(10, crc))
			require.NoError(t, e.rawNode.Step(&raftpb.Message{Type: raftpb.MsgSnap.Enum(),
				From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(3), Snapshot: &snapshot}))
			err = e.drainReady()
			if !tc.valid {
				require.Error(t, err)
				require.False(t, ready)
				require.Zero(t, calls)
				require.Equal(t, uint64(1), e.currentConfigIndex())
				return
			}
			require.NoError(t, err)
			require.True(t, ready)
			require.Positive(t, calls)
			require.Equal(t, uint64(4), restored.AppliedConfigurationIndex())
			require.Equal(t, uint64(4), e.currentConfigIndex())
			require.Equal(t, [][]byte{[]byte("received data")}, restored.Applied())
		})
	}
}

func TestProtectedDataSnapshotRetainsConfigurationIndex(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "1")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	fsm := kv.NewKvFSMWithHLC(store.NewMVCCStore(), nil)
	cfg := OpenConfig{NodeID: 7, LocalID: "n7", LocalAddress: "127.0.0.1:7007",
		GroupID: identityFixture().Identity.Group, NativeAdmission: fixtureNativeAdmission{},
		DataDir: t.TempDir(), Bootstrap: true, NativeIdentity: identityFixture(), StateMachine: fsm,
		TickInterval: 10 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1}
	e, err := Open(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	aware, ok := fsm.(raftengine.AppliedConfigurationAware)
	require.True(t, ok)
	n := aware.AppliedConfigurationIndex()
	require.NotZero(t, n)
	require.Eventually(t, func() bool { return e.Status().LastSnapshotIndex > n }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, e.Close())
	cfg.Bootstrap = false
	cfg.StateMachine = kv.NewKvFSMWithHLC(store.NewMVCCStore(), nil)
	cfg.AdmissionChanged = func() {}
	cfg.RecoveredAdmission = func(s RecoveredState) error {
		require.Equal(t, n, s.ConfigurationIndex())
		return nil
	}
	reopened, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
	peers, present, err := loadPersistedPeersState(cfg.DataDir)
	require.NoError(t, err)
	require.True(t, present)
	require.NoError(t, savePersistedPeersWithBootstrapSeed(cfg.DataDir, peers.Index+100, peers.Peers, nil))
	_, err = Open(ctx, cfg)
	require.ErrorContains(t, err, "peers metadata is ahead")
}

func TestProtectedSnapshotRejectsUnprovenConfigurationIndex(t *testing.T) {
	for _, index := range []uint64{0, 11} {
		fsm := kv.NewKvFSMWithHLC(store.NewMVCCStore(), nil)
		aware, ok := fsm.(raftengine.AppliedConfigurationAware)
		require.True(t, ok)
		require.NoError(t, aware.RecordAppliedConfiguration(index))
		e := &Engine{fsm: fsm}
		prepared := preparedOpenState{cfg: OpenConfig{RecoveredAdmission: func(RecoveredState) error { return nil }},
			disk: &diskState{LocalSnap: raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(10)}}}}
		require.ErrorContains(t, e.recoverProtectedConfigurationIndex(prepared), "configuration index is inconsistent")
	}
}
