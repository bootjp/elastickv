package etcd

import (
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type nativeSnapshotKVFSM struct {
	pipelineNativeConfigurationStateMachine
	gate  *nativeSnapshotGate
	index atomic.Uint64
}

func (f *nativeSnapshotKVFSM) SetApplyIndex(index uint64) {
	f.index.Store(index)
	f.pipelineNativeConfigurationStateMachine.SetApplyIndex(index)
}

func (f *nativeSnapshotKVFSM) Snapshot() (Snapshot, error) {
	index := f.index.Load()
	if index >= 3 {
		f.gate.minimum.Store(index)
	}
	if err := f.gate.visit("capture", index); err != nil {
		return nil, err
	}
	return f.pipelineNativeConfigurationStateMachine.Snapshot()
}

func (f *nativeSnapshotKVFSM) Restore(reader io.Reader) error {
	if err := f.gate.visit("restore", f.gate.minimum.Load()); err != nil {
		return err
	}
	return f.pipelineNativeConfigurationStateMachine.Restore(reader)
}

func TestEnginePipelineThreeVoterRealKVSnapshotRecovery(t *testing.T) {
	for _, format := range []string{"native", "sst"} {
		t.Run(format, func(t *testing.T) { testPipelineRealKVSnapshotRecovery(t, format == "sst") })
	}
}

func testPipelineRealKVSnapshotRecovery(t *testing.T, sst bool) {
	t.Helper()
	stores := make([]store.MVCCStore, 3)
	machines := make([]pipelineNativeConfigurationStateMachine, 3)
	for i := range stores {
		st, err := store.NewPebbleStore(t.TempDir(), store.WithSSTIngestSnapshots(sst))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		stores[i] = st
		var ok bool
		machines[i], ok = kv.NewKvFSMWithHLC(st, nil).(pipelineNativeConfigurationStateMachine)
		require.True(t, ok)
	}
	c := newNativeSnapshotClusterWithMachines(t, "", "restore", machines)
	c.dropThird = true
	payload, err := proto.Marshal(&pb.RaftCommand{Requests: []*pb.Request{{Ts: 100,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("snapshot-customer"), Value: []byte("durable-value")}},
	}}})
	require.NoError(t, err)
	index := c.propose(t, payload)
	c.pumpUntil(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > index }, "real KV checkpoint")
	c.gates[2].minimum.Store(index)
	c.dropThird = false
	c.pumpUntil(t, func() bool { return c.gates[2].used.Load() }, "real KV restore front")
	c.requireReceiverHeartbeats(t, func() {
		require.Less(t, c.nodes[2].engine.AppliedIndex(), index)
		require.Less(t, c.ackThird.Load(), index)
	})
	c.gates[2].unblock()
	c.pumpUntil(t, func() bool { return c.nodes[2].engine.AppliedIndex() >= index }, "real KV restore publication")
	value, err := stores[2].GetAt(t.Context(), []byte("snapshot-customer"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("durable-value"), value)
	configurationIndex, err := kv.MigrationConfigurationIndex(t.Context(), stores[2])
	require.NoError(t, err)
	// No membership change followed bootstrap: the configuration epoch is 1,
	// while the data/durable snapshot prefix is the later mutation index.
	require.Equal(t, uint64(1), configurationIndex)
	durableIndex, ok, err := machines[2].LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, index, durableIndex)
	require.Positive(t, c.nodes[0].engine.SnapshotStreamSendCount())
	for _, node := range c.nodes {
		require.NoError(t, node.engine.Close())
	}
	fresh, err := store.NewPebbleStore(t.TempDir(), store.WithSSTIngestSnapshots(sst))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fresh.Close()) })
	fsm := kv.NewKvFSMWithHLC(fresh, nil)
	reopened, err := Open(t.Context(), OpenConfig{NodeID: 3, LocalID: c.nodes[2].peer.ID,
		LocalAddress: c.nodes[2].peer.Address, DataDir: c.nodes[2].dir, StateMachine: fsm})
	require.NoError(t, err)
	require.GreaterOrEqual(t, reopened.AppliedIndex(), index)
	require.NoError(t, reopened.Close())
	value, err = fresh.GetAt(t.Context(), []byte("snapshot-customer"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("durable-value"), value)
	configurationIndex, err = kv.MigrationConfigurationIndex(t.Context(), fresh)
	require.NoError(t, err)
	require.Equal(t, uint64(1), configurationIndex)
	selected, err := loadPersistedSnapshot(zap.NewNop(), filepath.Join(c.nodes[2].dir, walDirName), snap.New(zap.NewNop(), filepath.Join(c.nodes[2].dir, snapDirName)))
	require.NoError(t, err)
	require.Equal(t, index, selected.GetMetadata().GetIndex())
	require.Equal(t, []uint64{1, 2, 3}, selected.GetMetadata().GetConfState().GetVoters())
}
