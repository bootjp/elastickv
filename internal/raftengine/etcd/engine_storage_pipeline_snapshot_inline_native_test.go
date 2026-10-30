package etcd

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Re-encode the genuine native sender's file/token snapshot for public inline
// ingress (not the stream adapter, which normalizes before that boundary).
// No index, term, membership or response is made up. This is not a TLS witness.
func inlineNativeSnapshotDispatch(base func(context.Context, dispatchRequest) error,
	directory string, sends *atomic.Uint64, receiver *atomic.Pointer[Engine],
) func(context.Context, dispatchRequest) error {
	return func(ctx context.Context, request dispatchRequest) error {
		if request.msg.GetType() != raftpb.MsgSnap || request.msg.GetTo() != 3 {
			return base(ctx, request)
		}
		token, err := decodeSnapshotToken(request.msg.GetSnapshot().GetData())
		if err != nil {
			return err
		}
		files := &pipelineSnapshotFiles{}
		// The dispatch request's real file pin stays held by its caller until
		// this return. The helper below only needs that immutable source file.
		files.mu = new(sync.Mutex)
		reader, err := files.openReceived(pipelineConfigurationEnvironment{fsmSnapDir: directory}, token.Index, token.CRC32C)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(reader)
		err = errors.CombineErrors(err, reader.Close())
		if err != nil {
			return err
		}
		request.msg.Snapshot = proto.CloneOf(request.msg.Snapshot)
		request.msg.Snapshot.Data = data
		sends.Add(1)
		target := receiver.Load()
		if target == nil {
			return errors.Wrap(errInvalidStorageWork, "inline snapshot receiver not installed")
		}
		return target.handleTransportMessage(ctx, request.msg)
	}
}

func TestEnginePipelineThreeVoterInlineSnapshotNormalizesBeforeRestore(t *testing.T) {
	var sends atomic.Uint64
	var receiver atomic.Pointer[Engine]
	c := newNativeSnapshotClusterWithHook(t, "", "restore", nil, func(e *Engine, i int) {
		if i == 0 {
			e.dispatchFn = inlineNativeSnapshotDispatch(e.dispatchFn, e.fsmSnapDir, &sends, &receiver)
		}
		if i == 2 {
			receiver.Store(e)
		}
	})
	c.dropThird = true
	index := c.propose(t, []byte("genuine-native-inline-snapshot-prefix"))
	c.pumpUntil(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > index }, "source checkpoint compaction")
	c.gates[2].minimum.Store(index)
	c.dropThird = false
	c.pumpUntil(t, func() bool { return c.gates[2].used.Load() }, "inline snapshot reached ordered Restore")
	c.requireReceiverHeartbeats(t, func() {
		require.Less(t, c.ackThird.Load(), index, "the real opaque response must wait for restore and publication")
		require.Less(t, c.nodes[2].engine.AppliedIndex(), index)
	})
	c.gates[2].unblock()
	c.pumpUntil(t, func() bool { return c.nodes[2].engine.AppliedIndex() >= index }, "normalized snapshot publication")
	require.Positive(t, sends.Load())
	require.Positive(t, c.fsms[2].restores.Load())
	require.Equal(t, c.fsms[0].Applied(), c.fsms[2].Applied())
	snapshot, err := c.nodes[2].engine.storage.Snapshot()
	require.NoError(t, err)
	require.True(t, isSnapshotToken(snapshot.Data))
	require.Equal(t, []uint64{1, 2, 3}, snapshot.Metadata.ConfState.Voters)
	historyBytes, historyItems := c.nodes[2].engine.storagePipeline.configurationHistoryBudget.usage()
	require.Positive(t, historyBytes, "received publication retains an independently cloned history base after its plan retires")
	require.Positive(t, historyItems)
	require.NoError(t, c.nodes[2].engine.Close())
	require.Empty(t, c.nodes[2].engine.storagePipeline.normalizations)
	require.Empty(t, c.nodes[2].engine.storagePipeline.snapshotFiles.pins)
	fsm := &testStateMachine{}
	disk, err := openDiskState(OpenConfig{DataDir: c.nodes[2].dir, StateMachine: fsm}, nil)
	require.NoError(t, err)
	require.True(t, isSnapshotToken(disk.LocalSnap.Data))
	require.Equal(t, index, disk.LocalSnap.GetMetadata().GetIndex())
	require.Equal(t, c.fsms[0].Applied(), fsm.Applied())
	require.NoError(t, closePersist(disk.Persist))
}
