package etcd

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestEnginePipelineDispatchActualSnapshotOwnershipSurvivesNewerCheckpoint(t *testing.T) {
	held := make(chan dispatchRequest, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	c := newNativeSnapshotClusterWithHook(t, "", "", nil, func(e *Engine, i int) {
		if i == 0 {
			holdPipelineDispatchSnapshot(e, func(request dispatchRequest) { held <- request }, release)
		}
	})
	t.Cleanup(func() { unblock(); closePipelineDispatchCluster(t, c) })
	e := c.nodes[0].engine
	c.dropThird = true
	old := c.propose(t, []byte("held-transport-prefix"))
	c.pumpUntil(t, func() bool { first, _ := e.storage.FirstIndex(); return first > old }, "old actual checkpoint")
	c.dropThird = false
	var request dispatchRequest
	c.pumpUntil(t, func() bool {
		select {
		case request = <-held:
			return true
		default:
			return false
		}
	}, "real gRPC snapshot result held before reliable report")
	files := e.storagePipeline.snapshotFiles
	assertHeld := func() {
		used, items := e.storagePipeline.outboundBulk.usage()
		require.GreaterOrEqual(t, used, request.permit.bytes)
		require.Positive(t, items)
		require.Positive(t, dispatchSnapshotPinCount(files, old))
	}
	c.requireHeartbeats(t, assertHeld)
	newIndex := c.propose(t, []byte("newer-local-prefix"))
	c.pumpUntil(t, func() bool {
		current, _ := e.storage.Snapshot()
		return current.GetMetadata().GetIndex() >= newIndex
	}, "newer checkpoint while old outbound request remains owned")
	admitPipelineDispatchConfiguration(t, c)
	c.pumpUntil(t, func() bool { return e.currentConfigIndex() > newIndex }, "later configuration publication")
	assertHeld()
	require.Equal(t, old, request.msg.GetSnapshot().GetMetadata().GetIndex())
	require.Equal(t, []uint64{1, 2, 3}, request.msg.GetSnapshot().GetMetadata().GetConfState().GetVoters())
	require.Empty(t, request.msg.GetSnapshot().GetMetadata().GetConfState().GetLearners())
	token, err := decodeSnapshotToken(request.msg.GetSnapshot().GetData())
	require.NoError(t, err)
	oldPath := fsmSnapPath(e.fsmSnapDir, token.Index)
	require.FileExists(t, oldPath, "newer prewrite/purge cannot unlink the held old request's payload")
	restored := &testStateMachine{}
	require.NoError(t, openAndRestoreFSMSnapshot(restored, oldPath, token.CRC32C))
	require.Equal(t, [][]byte{[]byte("held-transport-prefix")}, restored.Applied())
	unblock()
	c.pumpUntil(t, func() bool { return dispatchSnapshotPinCount(files, old) == 0 }, "actual dispatch/report retirement")
	finalIndex := c.propose(t, []byte("checkpoint-after-report"))
	c.pumpUntil(t, func() bool {
		current, _ := e.storage.Snapshot()
		return current.GetMetadata().GetIndex() >= finalIndex
	}, "checkpoint after old request retirement")
	_, err = os.Stat(oldPath)
	require.True(t, os.IsNotExist(err), "the retired old pair can now be pruned")
}

func admitPipelineDispatchConfiguration(t *testing.T, c *nativeSnapshotCluster) {
	t.Helper()
	message := nativeSnapshotConfigurationProposal(t, true)
	c.pumpUntil(t, func() bool {
		err := c.nodes[0].engine.handleTransportMessage(t.Context(), message)
		if errors.Is(err, errStepQueueFull) {
			return false
		}
		require.NoError(t, err)
		return true
	}, "later native membership admission")
}

func TestEnginePipelineDispatchReliableReportRetainsBytesAndPin(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			e, queues := newOutboundBudgetEngine(t, pipelineBulkBytes, pipelineBulkItems)
			message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2),
				Snapshot: &raftpb.Snapshot{Data: []byte("owned"), Metadata: testSnapshotMetadata(17, 3, []uint64{1, 2})}}
			require.NoError(t, e.enqueueDispatchMessage(message))
			request := testutil.Recv(t, queues.normal, time.Second, "owned snapshot request missing")
			e.closeCh = make(chan struct{})
			e.dispatchReportCh = make(chan dispatchReport, 1)
			e.dispatchReportCh <- dispatchReport{to: 99}
			returned, done := make(chan struct{}), make(chan struct{})
			e.dispatchFn = func(context.Context, dispatchRequest) error {
				close(returned)
				if failed {
					return errors.New("actual dispatch failure")
				}
				return nil
			}
			t.Cleanup(func() {
				close(e.closeCh)
				testutil.Closed(t, done, time.Second, "reliable report cleanup did not join")
			})
			go func() { e.handleDispatchRequest(t.Context(), request); close(done) }()
			testutil.Closed(t, returned, time.Second, "dispatch did not actually return")
			select {
			case <-done:
				t.Fatal("request retired before reliable report capacity")
			case <-time.After(20 * time.Millisecond):
			}
			used, items := e.storagePipeline.outboundBulk.usage()
			require.Equal(t, request.permit.bytes, used)
			require.Equal(t, 1, items)
			require.Equal(t, 1, dispatchSnapshotPinCount(e.storagePipeline.snapshotFiles, 17))
			testutil.Recv(t, e.dispatchReportCh, time.Second, "report-capacity filler missing")
			testutil.Closed(t, done, time.Second, "report retirement did not finish")
			report := testutil.Recv(t, e.dispatchReportCh, time.Second, "reliable snapshot report missing")
			require.Equal(t, raftpb.MsgSnap, report.msgType)
			require.Equal(t, !failed, report.snapshotFinish)
			used, items = e.storagePipeline.outboundBulk.usage()
			require.Zero(t, used)
			require.Zero(t, items)
			require.Zero(t, dispatchSnapshotPinCount(e.storagePipeline.snapshotFiles, 17))
		})
	}
}
