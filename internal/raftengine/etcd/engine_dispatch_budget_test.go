package etcd

import (
	"bytes"
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func outboundBudgetMessage() raftpb.Message {
	return raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2),
		Term: uint64Ptr(1), Index: uint64Ptr(1),
		Entries: []*raftpb.Entry{{Index: uint64Ptr(2), Term: uint64Ptr(1), Data: bytes.Repeat([]byte("v"), 1024)}},
	}
}

func newOutboundBudgetEngine(t *testing.T, bulkBytes uint64, bulkItems int) (*Engine, *peerQueues) {
	t.Helper()
	queues := &peerQueues{
		normal: make(chan dispatchRequest, 8), heartbeat: make(chan dispatchRequest, 8),
		heartbeatResp: make(chan dispatchRequest, 1), readIndexResp: make(chan dispatchRequest, 8),
	}
	e := &Engine{nodeID: 1, peerDispatchers: map[uint64]*peerQueues{2: queues},
		storagePipeline: &engineStoragePipeline{
			outboundBulk:    newStorageWorkBudget(bulkBytes, bulkItems),
			outboundControl: newStorageWorkBudget(pipelineControlBytes, pipelineControlItems),
		},
	}
	t.Cleanup(func() {
		for _, ch := range []chan dispatchRequest{queues.normal, queues.heartbeat, queues.heartbeatResp, queues.readIndexResp} {
			for len(ch) > 0 {
				require.NoError(t, (<-ch).Close())
			}
		}
		used, items := e.storagePipeline.outboundBulk.usage()
		require.Zero(t, used)
		require.Zero(t, items)
		used, items = e.storagePipeline.outboundControl.usage()
		require.Zero(t, used)
		require.Zero(t, items)
	})
	return e, queues
}

func TestPipelineOutboundCloneIsReservedUntilClose(t *testing.T) {
	message := outboundBudgetMessage()
	size := outboundDispatchBytes(&message)
	e, queues := newOutboundBudgetEngine(t, size, 8)
	require.NoError(t, e.enqueueDispatchMessage(message))
	used, items := e.storagePipeline.outboundBulk.usage()
	require.Equal(t, size, used, "a queued deep clone must retain a byte reservation")
	require.Equal(t, 1, items)
	request := <-queues.normal
	t.Cleanup(func() { require.NoError(t, request.Close()) })
	message.Entries[0].Data[0] = 'x'
	require.Equal(t, byte('v'), request.msg.Entries[0].Data[0])
	require.NoError(t, request.Close())
	require.NoError(t, request.Close(), "repeated cleanup must not release another request's permit")
	used, items = e.storagePipeline.outboundBulk.usage()
	require.Zero(t, used)
	require.Zero(t, items)
}

func TestPipelineOutboundByteLimitDropsBeforeChannelIsFull(t *testing.T) {
	message := outboundBudgetMessage()
	size := outboundDispatchBytes(&message)
	e, queues := newOutboundBudgetEngine(t, size, 8)
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.Len(t, queues.normal, 1, "byte exhaustion must not create a second retained clone")
	require.Equal(t, uint64(1), e.DispatchDropCount())
	used, items := e.storagePipeline.outboundBulk.usage()
	require.Equal(t, size, used)
	require.Equal(t, 1, items)
	request := <-queues.normal
	require.NoError(t, request.Close())
	require.NoError(t, e.enqueueDispatchMessage(message), "real retirement reopens bulk capacity")
	require.Len(t, queues.normal, 1)
}

func TestPipelineOutboundControlIsIndependentOfBulk(t *testing.T) {
	message := outboundBudgetMessage()
	e, queues := newOutboundBudgetEngine(t, outboundDispatchBytes(&message), 1)
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.NoError(t, e.enqueueDispatchMessage(message))
	heartbeat := raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), From: uint64Ptr(1), To: uint64Ptr(2), Term: uint64Ptr(1)}
	require.NoError(t, e.enqueueDispatchMessage(heartbeat))
	require.Len(t, queues.normal, 1)
	require.Len(t, queues.heartbeat, 1)
	used, items := e.storagePipeline.outboundControl.usage()
	require.Equal(t, outboundDispatchBytes(&heartbeat), used)
	require.Equal(t, 1, items)
}

func TestPipelineOutboundNear64MiBEntryFits(t *testing.T) {
	message := outboundBudgetMessage()
	message.Entries[0].Data = bytes.Repeat([]byte("v"), (64<<20)-(1<<19))
	e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.Len(t, queues.normal, 1)
	request := <-queues.normal
	t.Cleanup(func() { require.NoError(t, request.Close()) })
	require.Equal(t, message.Entries[0].Data, request.msg.Entries[0].Data)
	message.Entries[0].Data[len(message.Entries[0].Data)-1] = 'x'
	require.Equal(t, byte('v'), request.msg.Entries[0].Data[len(request.msg.Entries[0].Data)-1])
	used, items := e.storagePipeline.outboundBulk.usage()
	require.Equal(t, outboundDispatchBytes(&request.msg), used)
	require.Equal(t, 1, items)
}

func TestPipelineOutboundCoalescingRetiresOnlyReplacedRequest(t *testing.T) {
	e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeatResp), From: uint64Ptr(1), To: uint64Ptr(2), Index: uint64Ptr(1)}
	require.NoError(t, e.enqueueDispatchMessage(message))
	old := <-queues.heartbeatResp
	queues.heartbeatResp <- old
	for index := uint64(2); index <= 20; index++ {
		message.Index = uint64Ptr(index)
		require.NoError(t, e.enqueueDispatchMessage(message))
		require.Len(t, queues.heartbeatResp, 1)
		used, items := e.storagePipeline.outboundControl.usage()
		require.Equal(t, outboundDispatchBytes(&message), used)
		require.Equal(t, 1, items, "replacement must not leak the old reservation")
	}
	require.NoError(t, old.Close(), "a stale request copy cannot retire the current replacement")
	request := <-queues.heartbeatResp
	t.Cleanup(func() { require.NoError(t, request.Close()) })
	require.Equal(t, uint64(20), request.msg.GetIndex())
	require.Zero(t, e.DispatchDropCount())
}

func TestPipelineOutboundCoalescingBudgetFailureKeepsOldRequest(t *testing.T) {
	e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeatResp), To: uint64Ptr(2), Index: uint64Ptr(1)}
	size := outboundDispatchBytes(&message)
	e.storagePipeline.outboundControl = newStorageWorkBudget(size, 1)
	require.NoError(t, e.enqueueDispatchMessage(message))
	message.Index = uint64Ptr(2)
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.Len(t, queues.heartbeatResp, 1)
	request := <-queues.heartbeatResp
	t.Cleanup(func() { require.NoError(t, request.Close()) })
	require.Equal(t, uint64(1), request.msg.GetIndex())
	require.Equal(t, uint64(1), e.DispatchDropCount())
	used, items := e.storagePipeline.outboundControl.usage()
	require.Equal(t, size, used)
	require.Equal(t, 1, items)
}

func TestPipelineOutboundCanceledWorkerJoinsAndRetiresQueue(t *testing.T) {
	message := outboundBudgetMessage()
	size := outboundDispatchBytes(&message)
	e, queues := newOutboundBudgetEngine(t, 2*size, 2)
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.NoError(t, e.enqueueDispatchMessage(message))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	e.dispatchCancel = cancel
	e.dispatchStopCh = make(chan struct{})
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		e.stopDispatchWorkers()
	})
	e.dispatchFn = func(ctx context.Context, _ dispatchRequest) error {
		close(entered)
		<-release // Deliberately require the real non-cancellable RPC to return.
		return ctx.Err()
	}
	e.dispatchWG.Add(1)
	go e.runDispatchWorker(ctx, queues.normal)
	testutil.Closed(t, entered, time.Second, "dispatch did not start")
	go func() { e.stopDispatchWorkers(); close(stopped) }()
	testutil.NoRecv(t, stopped, 20*time.Millisecond, "stop returned before the held dispatch returned")
	used, items := e.storagePipeline.outboundBulk.usage()
	require.Equal(t, 2*size, used, "canceling a live RPC must not release its or the queued request's ownership")
	require.Equal(t, 2, items)
	unblock()
	testutil.Closed(t, stopped, time.Second, "worker did not join after real completion")
	require.Empty(t, queues.normal)
	used, items = e.storagePipeline.outboundBulk.usage()
	require.Zero(t, used)
	require.Zero(t, items)
}

func TestPipelineOutboundSnapshotReportRetainsOwnership(t *testing.T) {
	e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2), Snapshot: &raftpb.Snapshot{
		Data: []byte("snapshot"), Metadata: testSnapshotMetadata(7, 3, []uint64{1, 2}),
	}}
	require.NoError(t, e.enqueueDispatchMessage(message))
	request := <-queues.normal
	e.dispatchReportCh = make(chan dispatchReport, 1)
	e.dispatchReportCh <- dispatchReport{to: 9}
	e.closeCh = make(chan struct{})
	returned, done := make(chan struct{}), make(chan struct{})
	e.dispatchFn = func(context.Context, dispatchRequest) error { close(returned); return nil }
	t.Cleanup(func() {
		close(e.closeCh)
		testutil.Closed(t, done, time.Second, "completion report did not stop on shutdown")
	})
	go func() { e.handleDispatchRequest(t.Context(), request); close(done) }()
	testutil.Closed(t, returned, time.Second, "transport did not return")
	testutil.NoRecv(t, done, 20*time.Millisecond, "snapshot finish did not retain its reliable report")
	used, items := e.storagePipeline.outboundBulk.usage()
	require.Equal(t, outboundDispatchBytes(&message), used, "RPC return alone is not the last request use")
	require.Equal(t, 1, items)
	<-e.dispatchReportCh
	testutil.Closed(t, done, time.Second, "report did not finish after capacity opened")
	report := <-e.dispatchReportCh
	require.True(t, report.snapshotFinish)
	used, items = e.storagePipeline.outboundBulk.usage()
	require.Zero(t, used)
	require.Zero(t, items)
}

func TestPipelineOutboundPeerRemovalRetiresCanceledLanes(t *testing.T) {
	message := outboundBudgetMessage()
	e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
	ctx, cancel := context.WithCancel(t.Context())
	queues.cancel = cancel
	t.Cleanup(cancel)
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.NoError(t, e.enqueueDispatchMessage(message))
	require.NoError(t, e.enqueueDispatchMessage(raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), To: uint64Ptr(2)}))
	e.removePeer(2)
	require.NotContains(t, e.peerDispatchers, uint64(2))
	for _, ch := range []chan dispatchRequest{queues.normal, queues.heartbeat, queues.heartbeatResp, queues.readIndexResp} {
		e.dispatchWG.Add(1)
		go e.runDispatchWorker(ctx, ch)
	}
	e.stopDispatchWorkers()
	used, items := e.storagePipeline.outboundBulk.usage()
	require.Zero(t, used)
	require.Zero(t, items)
	used, items = e.storagePipeline.outboundControl.usage()
	require.Zero(t, used)
	require.Zero(t, items)
}

func TestPipelineOutboundItemLimitAndGlobalPeerBudget(t *testing.T) {
	sample := outboundBudgetMessage()
	for _, test := range []struct {
		name     string
		maxBytes uint64
		maxItems int
	}{
		{name: "items", maxBytes: 1 << 20, maxItems: 1},
		{name: "shared-peer-bytes", maxBytes: outboundDispatchBytes(&sample), maxItems: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := outboundBudgetMessage()
			e, queues := newOutboundBudgetEngine(t, test.maxBytes, test.maxItems)
			other := &peerQueues{normal: make(chan dispatchRequest, 8)}
			e.peerDispatchers[3] = other
			t.Cleanup(func() {
				for len(other.normal) > 0 {
					require.NoError(t, (<-other.normal).Close())
				}
			})
			require.NoError(t, e.enqueueDispatchMessage(message))
			message.To = uint64Ptr(3)
			require.NoError(t, e.enqueueDispatchMessage(message))
			require.Len(t, queues.normal, 1)
			require.Empty(t, other.normal, "each peer must share the same byte/item allowance")
			require.Equal(t, uint64(1), e.DispatchDropCount())
		})
	}
}

func TestTryReceiveDispatchRequestClosedOrEmpty(t *testing.T) {
	closed := make(chan dispatchRequest)
	close(closed)
	for _, ch := range []chan dispatchRequest{closed, make(chan dispatchRequest, 1), nil} {
		_, received := tryReceiveDispatchRequest(ch)
		require.False(t, received, "a closed/empty lane is not a zero-value dispatch request")
	}
}

func TestPipelineOutboundRetainedSliceCapacityIsCharged(t *testing.T) {
	t.Run("snapshot-confstate", func(t *testing.T) {
		message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2), Snapshot: &raftpb.Snapshot{
			Data: []byte("snapshot"), Metadata: testSnapshotMetadata(7, 3, nil),
		}}
		voters := make([]uint64, 1000)
		for i := range voters {
			voters[i] = uint64(i + 1)
		}
		message.Snapshot.Metadata.ConfState.Voters = voters
		e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
		require.NoError(t, e.enqueueDispatchMessage(message))
		request := <-queues.normal
		t.Cleanup(func() { require.NoError(t, request.Close()) })
		used, _ := e.storagePipeline.outboundBulk.usage()
		retained := uint64(cap(request.msg.Snapshot.Metadata.ConfState.Voters))*8 + uint64(cap(request.msg.Snapshot.Data))
		require.GreaterOrEqual(t, used, retained, "packed varints do not measure the retained uint64 array")
	})
	t.Run("entry-backing-capacity", func(t *testing.T) {
		message := outboundBudgetMessage()
		message.Entries[0].Data = bytes.Repeat([]byte("v"), (1<<16)+1)
		e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
		require.NoError(t, e.enqueueDispatchMessage(message))
		request := <-queues.normal
		t.Cleanup(func() { require.NoError(t, request.Close()) })
		used, _ := e.storagePipeline.outboundBulk.usage()
		require.GreaterOrEqual(t, used, uint64(cap(request.msg.Entries[0].Data)), "retained clone capacity can exceed its protobuf payload length")
	})
}

func TestOutboundArrayBillingSaturatesOnOverflow(t *testing.T) {
	require.Equal(t, uint64(16), outboundArrayBytes(0, 1))
	require.Equal(t, uint64(math.MaxUint64), outboundArrayBytes(math.MaxUint64-15, 1))
	require.Equal(t, uint64(math.MaxUint64), outboundArrayBytes(0, math.MaxUint64))
	require.Equal(t, uint64(math.MaxUint64), outboundArrayBytes(math.MaxUint64, 0))
	require.Equal(t, uint64(math.MaxUint64-15), outboundArrayBytes(math.MaxUint64-31, 1))
}

func TestPipelineOutboundNestedSnapshotArraysAreCharged(t *testing.T) {
	message := outboundBudgetMessage()
	state := testSnapshotMetadata(7, 3, nil)
	ids := make([]uint64, 1000)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	state.ConfState.Voters, state.ConfState.VotersOutgoing = ids, ids
	state.ConfState.Learners, state.ConfState.LearnersNext = ids, ids
	message.Responses = []*raftpb.Message{{Snapshot: &raftpb.Snapshot{Metadata: state}}}
	e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
	require.NoError(t, e.enqueueDispatchMessage(message))
	request := <-queues.normal
	t.Cleanup(func() { require.NoError(t, request.Close()) })
	cloned := request.msg.Responses[0].Snapshot.Metadata.ConfState
	retained := 8 * uint64(cap(cloned.Voters)+cap(cloned.VotersOutgoing)+cap(cloned.Learners)+cap(cloned.LearnersNext))
	used, _ := e.storagePipeline.outboundBulk.usage()
	require.GreaterOrEqual(t, used, retained)
}

func TestPipelineOutboundUnknownFieldsAreOwnedAndCharged(t *testing.T) {
	for _, test := range []struct {
		name  string
		field func(*raftpb.Message) proto.Message
	}{
		{name: "message", field: func(m *raftpb.Message) proto.Message { return m }},
		{name: "response", field: func(m *raftpb.Message) proto.Message { return m.Responses[0] }},
		{name: "snapshot", field: func(m *raftpb.Message) proto.Message { return m.Snapshot }},
		{name: "metadata", field: func(m *raftpb.Message) proto.Message { return m.Snapshot.Metadata }},
		{name: "confstate", field: func(m *raftpb.Message) proto.Message { return m.Snapshot.Metadata.ConfState }},
		{name: "entry-control", field: func(m *raftpb.Message) proto.Message { return m.Entries[0] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := outboundBudgetMessage()
			message.Responses = []*raftpb.Message{{Type: messageTypePtr(raftpb.MsgAppResp)}}
			message.Snapshot = &raftpb.Snapshot{Metadata: testSnapshotMetadata(7, 3, []uint64{1, 2})}
			unknown := make([]byte, 2, 1<<20)
			unknown[0], unknown[1] = 0x78, 1
			test.field(&message).ProtoReflect().SetUnknown(unknown)
			e, _ := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
			request, admitted := e.tryPrepareOwnedDispatchRequest(message)
			require.True(t, admitted)
			t.Cleanup(func() { require.NoError(t, request.Close()) })
			cloned := test.field(&request.msg).ProtoReflect().GetUnknown()
			used, _ := e.storagePipeline.outboundBulk.usage()
			require.GreaterOrEqual(t, used, uint64(cap(cloned)), "unknown backing storage must not escape the byte budget")
			unknown[1] = 2
			require.Equal(t, byte(1), cloned[1], "an owned clone cannot retain mutable caller backing storage")
		})
	}
}
