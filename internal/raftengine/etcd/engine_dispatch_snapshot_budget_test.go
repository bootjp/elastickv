package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func dispatchSnapshotPinCount(files *pipelineSnapshotFiles, index uint64) int {
	files.pinsMu.Lock()
	defer files.pinsMu.Unlock()
	return files.pins[index]
}

func TestPipelineOutboundSnapshotPinAndBytesRetireTogether(t *testing.T) {
	e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2), Snapshot: &raftpb.Snapshot{
		Data: []byte("snapshot"), Metadata: testSnapshotMetadata(17, 3, []uint64{1, 2}),
	}}
	require.NoError(t, e.enqueueDispatchMessage(message))
	files := e.storagePipeline.snapshotFiles
	require.Equal(t, 1, dispatchSnapshotPinCount(files, 17))
	used, items := e.storagePipeline.outboundBulk.usage()
	require.Equal(t, outboundDispatchBytes(&message), used)
	require.Equal(t, 1, items)
	request := <-queues.normal
	t.Cleanup(func() { require.NoError(t, request.Close()) })
	require.NoError(t, request.Close())
	require.NoError(t, request.Close())
	require.Zero(t, dispatchSnapshotPinCount(files, 17))
	used, items = e.storagePipeline.outboundBulk.usage()
	require.Zero(t, used)
	require.Zero(t, items)
	e.storagePipeline.outboundBulk = newStorageWorkBudget(1, 1)
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.Empty(t, queues.normal)
	require.Zero(t, dispatchSnapshotPinCount(files, 17), "a rejected clone must not acquire a snapshot file pin")
}

func TestPipelineOutboundRemovedPeerDrainsSnapshotPinsAndBytes(t *testing.T) {
	e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	queues.cancel = cancel
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2), Snapshot: &raftpb.Snapshot{
		Data: []byte("snapshot"), Metadata: testSnapshotMetadata(17, 3, []uint64{1, 2}),
	}}
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.NoError(t, e.enqueueDispatchMessage(message))
	files := e.storagePipeline.snapshotFiles
	require.Equal(t, 2, dispatchSnapshotPinCount(files, 17))
	e.removePeer(2)
	for _, ch := range []chan dispatchRequest{queues.normal, queues.heartbeat, queues.heartbeatResp, queues.readIndexResp} {
		e.dispatchWG.Add(1)
		go e.runDispatchWorker(ctx, ch)
	}
	e.stopDispatchWorkers()
	require.Zero(t, dispatchSnapshotPinCount(files, 17))
	used, items := e.storagePipeline.outboundBulk.usage()
	require.Zero(t, used)
	require.Zero(t, items)
}
