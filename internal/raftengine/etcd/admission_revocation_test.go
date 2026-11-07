package etcd

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestAdmissionRevokedBeforeStatusPublication(t *testing.T) {
	t.Parallel()
	storage := etcdraft.NewMemoryStorage()
	e := &Engine{nodeID: 1, rawNode: mustRawNode(t, storage, 1), storage: storage}
	e.refreshStatus()
	old := e.status
	calls := 0
	e.admissionChanged = func() {
		calls++
		// Acquiring this lock also checks the callback is outside e.mu.
		e.mu.RLock()
		defer e.mu.RUnlock()
		require.Equal(t, old.Term, e.status.Term)
		require.Equal(t, old.State, e.status.State)
	}
	require.NoError(t, e.rawNode.Step(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(10)}))
	e.refreshStatus()
	require.Equal(t, 1, calls)
	require.Equal(t, uint64(10), e.status.Term)
	e.refreshStatus()
	require.Equal(t, 1, calls, "unchanged status must not trigger revocation")
}

func TestProtectedConfigurationRejectedBeforeRawNodeMutation(t *testing.T) {
	t.Parallel()
	for _, v2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "v1", true: "v2"}[v2], func(t *testing.T) {
			storage := etcdraft.NewMemoryStorage()
			e := &Engine{rawNode: mustRawNode(t, storage, 1), storage: storage}
			before := e.rawNode.Status().Config
			calls := 0
			e.admissionChanged = func() {
				calls++
				e.mu.Lock()
				require.Zero(t, e.applied)
				e.mu.Unlock()
				require.Equal(t, before, e.rawNode.Status().Config)
			}
			cc := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode.Enum(), NodeId: proto.Uint64(2)}
			var data []byte
			var err error
			if v2 {
				data, err = proto.Marshal(cc.AsV2())
			} else {
				data, err = proto.Marshal(cc)
			}
			require.NoError(t, err)
			entry := raftpb.Entry{Data: data, Index: proto.Uint64(2)}
			if v2 {
				err = e.applyConfChangeV2Committed(entry)
			} else {
				err = e.applyConfChangeCommitted(entry)
			}
			require.ErrorContains(t, err, "exact policy context")
			require.Equal(t, 1, calls)
			require.Equal(t, before, e.rawNode.Status().Config)
			require.Zero(t, e.applied)
		})
	}
}

func TestAdmissionRevokedBeforeRoleAndConfigurationPublication(t *testing.T) {
	t.Parallel()
	for _, configuration := range []bool{false, true} {
		t.Run(map[bool]string{false: "role", true: "configuration"}[configuration], func(t *testing.T) {
			storage := etcdraft.NewMemoryStorage()
			e := &Engine{nodeID: 1, rawNode: mustRawNode(t, storage, 1), storage: storage}
			e.refreshStatus()
			if configuration {
				e.configIndex.Store(7)
			} else {
				e.status.State = raftengine.StateCandidate
			}
			old := e.status
			calls := 0
			e.admissionChanged = func() {
				calls++
				e.mu.RLock()
				defer e.mu.RUnlock()
				require.Equal(t, old.State, e.status.State)
				require.Equal(t, old.ConfigurationIndex, e.status.ConfigurationIndex)
			}
			e.refreshStatus()
			require.Equal(t, 1, calls)
		})
	}
}

func TestSnapshotInvalidatesBeforeRestore(t *testing.T) {
	t.Parallel()
	e := &Engine{status: raftengine.Status{Term: 9}}
	calls := 0
	e.admissionChanged = func() {
		calls++
		e.mu.Lock()
		defer e.mu.Unlock()
		require.Equal(t, uint64(9), e.status.Term)
	}
	err := e.applyReadySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(10)}})
	require.ErrorIs(t, err, errSnapshotRequired)
	require.Equal(t, 1, calls)
}

func TestTerminalTransitionsRevokeBeforePublication(t *testing.T) {
	t.Parallel()
	for _, fatal := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown", true: "fatal"}[fatal], func(t *testing.T) {
			e := &Engine{transport: NewGRPCTransport(nil), closeCh: make(chan struct{})}
			calls := 0
			e.admissionChanged = func() {
				calls++
				e.mu.RLock()
				defer e.mu.RUnlock()
				require.False(t, e.closed)
			}
			if fatal {
				e.fail(nil)
			} else {
				e.shutdown()
			}
			require.Equal(t, 1, calls)
			require.True(t, e.closed)
			require.Equal(t, raftengine.StateShutdown, e.status.State)
		})
	}
}

func TestWithBootstrapStartupCopiesFactorySettings(t *testing.T) {
	t.Parallel()
	cipher := &encryption.Cipher{}
	original := NewFactory(FactoryConfig{TickInterval: time.Second, RaftCipher: cipher, RaftCutoverIndex: func() uint64 { return 42 }})
	derived := original.WithBootstrapStartup(func(raftengine.FactoryConfig, *GRPCTransport) (*BootstrapStartup, error) {
		return &BootstrapStartup{}, nil
	})
	require.NotSame(t, original, derived)
	require.Nil(t, original.cfg.BootstrapStartup)
	require.NotNil(t, derived.cfg.BootstrapStartup)
	require.Same(t, cipher, derived.cfg.RaftCipher)
	require.Equal(t, uint64(42), derived.cfg.RaftCutoverIndex())
	require.Equal(t, time.Second, derived.cfg.TickInterval)
	derived.cfg.TickInterval = 2 * time.Second
	require.Equal(t, time.Second, original.cfg.TickInterval)
}
