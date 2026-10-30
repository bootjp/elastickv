package etcd

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type pipelineNativeConfigurationStateMachine interface {
	kv.FSM
	raftengine.ApplyIndexAware
	raftengine.NamespaceConfigurationAware
	raftengine.AppliedConfigurationAware
	raftengine.AppliedIndexWriter
	raftengine.AppliedIndexReader
}

type pipelineNativeConfigurationFSM struct {
	pipelineNativeConfigurationStateMachine
	gate *pipelineConfigurationGate
}

func (f *pipelineNativeConfigurationFSM) RecordAppliedNamespaceConfiguration(index uint64, members []uint64) error {
	if err := f.gate.visit("record", index); err != nil {
		return err
	}
	return f.pipelineNativeConfigurationStateMachine.RecordAppliedNamespaceConfiguration(index, members)
}

func TestEnginePipelineConfigurationRealKVRecoveryBeforePublication(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) { testPipelineRealKVConfiguration(t, v2) })
	}
}

func testPipelineRealKVConfiguration(t *testing.T, v2 bool) {
	t.Helper()
	failure := errors.New("stop after real configuration SaveSnap before publication")
	gate := &pipelineConfigurationGate{stage: "release", failure: failure, entered: make(chan uint64, 1), release: make(chan struct{})}
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	actual, ok := kv.NewKvFSMWithHLC(st, nil).(pipelineNativeConfigurationStateMachine)
	require.True(t, ok)
	fsm := &pipelineNativeConfigurationFSM{pipelineNativeConfigurationStateMachine: actual, gate: gate}
	walGate := &asyncStorageSaveGate{expectedCloseError: failure}
	disk := &pipelineConfigurationStorage{gate: gate, tailSaved: make(chan uint64, 1)}
	h := newNativeStorageRuntime(t, &asyncStorageFSM{}, walGate, newStorageRawNode, func(e *Engine) {
		disk.Storage = walGate
		pipeline, err := newEngineStoragePipeline(disk)
		require.NoError(t, err)
		e.storagePipeline, e.fsm, e.persist = pipeline, fsm, disk
		dispatch := e.dispatchFn
		e.dispatchFn = func(ctx context.Context, request dispatchRequest) error {
			if request.msg.GetTo() != 2 {
				return nil
			}
			return dispatch(ctx, request)
		}
	})
	t.Cleanup(gate.unblock)
	asyncStorageAwait(t, h, h.engine.startedCh, "real KV startup")
	require.NoError(t, h.engine.handleTransportMessage(h.ctx, raftpb.Message{
		Type: messageTypePtr(raftpb.MsgTimeoutNow), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(1),
	}))
	asyncStorageAwait(t, h, h.engine.leaderReady, "real KV leader")
	payload, err := proto.Marshal(&pb.RaftCommand{Requests: []*pb.Request{{Ts: 100,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("customer"), Value: []byte("durable-value")}},
	}}})
	require.NoError(t, err)
	result := asyncStorageAwait(t, h, h.propose(payload), "real KV mutation")
	require.NoError(t, result.err)
	require.NotNil(t, result.result)
	before := h.engine.AppliedIndex()
	proposeNativePipelineConfiguration(t, h, v2)
	index := asyncStorageAwait(t, h, gate.entered, "real KV namespace/snapshot durable before Release")
	require.Greater(t, index, before)
	require.Equal(t, index, actual.AppliedConfigurationIndex())
	require.Equal(t, before, h.engine.AppliedIndex())
	h.verifyBlockedHeartbeats(t, func() {
		require.Equal(t, before, h.engine.AppliedIndex())
		require.True(t, h.engine.LastQuorumAck().IsZero())
	})
	gate.unblock()
	select {
	case <-h.engine.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("real KV configuration failure did not join")
	}
	require.ErrorIs(t, h.engine.Err(), failure)
	fresh, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fresh.Close()) })
	restoredFSM := kv.NewKvFSMWithHLC(fresh, nil)
	selected, err := loadWalState(zap.NewNop(), filepath.Join(h.engine.dataDir, walDirName),
		filepath.Join(h.engine.dataDir, snapDirName), filepath.Join(h.engine.dataDir, fsmSnapDirName), restoredFSM, nil)
	require.NoError(t, err)
	require.Equal(t, index, selected.LocalSnap.GetMetadata().GetIndex())
	require.Equal(t, []uint64{3}, selected.LocalSnap.GetMetadata().GetConfState().GetLearners())
	value, err := fresh.GetAt(t.Context(), []byte("customer"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("durable-value"), value)
	configuration, err := kv.MigrationConfigurationIndex(t.Context(), fresh)
	require.NoError(t, err)
	require.Equal(t, index, configuration)
	require.NoError(t, closePersist(selected.Persist))
	reopened, err := Open(t.Context(), OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7011", DataDir: h.engine.dataDir, StateMachine: restoredFSM})
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}
