package etcd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestEnginePipelineDispatchRejectedSnapshotMustRetryAfterCapacityReturns(t *testing.T) {
	testPipelineDispatchRejectedSnapshot(t, false)
}

func TestEnginePipelineDispatchSnapshotPressurePreservesProbeProgress(t *testing.T) {
	testPipelineDispatchRejectedSnapshot(t, true)
}

func testPipelineDispatchRejectedSnapshot(t *testing.T, probeWhilePressured bool) {
	t.Helper()
	c := newNativeSnapshotCluster(t, "", "")
	t.Cleanup(func() { closePipelineDispatchCluster(t, c) })
	e := c.nodes[0].engine
	c.dropThird = true
	index := c.propose(t, []byte("snapshot-before-budget-pressure"))
	c.pumpUntil(t, func() bool {
		first, _ := e.storage.FirstIndex()
		return first > index && c.nodes[1].engine.AppliedIndex() >= index && e.Status().PerPeer[2].Match >= index
	}, "actual compacted snapshot and healthy other voter")
	budget := e.storagePipeline.outboundBulk
	used, _ := budget.usage()
	pressure, err := budget.tryAcquire(budget.maxBytes - used)
	require.NoError(t, err)
	t.Cleanup(pressure.Release)
	before := e.DispatchDropCount()
	c.dropThird = false
	c.pumpUntil(t, func() bool { return e.DispatchDropCount() > before }, "native snapshot dispatch reservation rejection")
	if probeWhilePressured {
		assertPipelineDispatchProbeProgress(t, c)
		require.Greater(t, e.DispatchDropCount(), before+1, "real probes must retry while snapshot capacity remains unavailable")
	}
	pressure.Release()
	stalled := verifyPipelineDispatchCatchup(t, c, index)
	closePipelineDispatchCluster(t, c)
	assertPipelineDispatchDropRetired(t, e, stalled)
}

func assertPipelineDispatchProbeProgress(t *testing.T, c *nativeSnapshotCluster) {
	t.Helper()
	for i := 0; i < 3; i++ {
		c.requireReceiverHeartbeats(t, func() { require.NoError(t, c.nodes[0].engine.Err()) })
	}
}

func verifyPipelineDispatchCatchup(t *testing.T, c *nativeSnapshotCluster, index uint64) bool {
	t.Helper()
	e := c.nodes[0].engine
	// Use real receiver heartbeat/ReadIndex progress after capacity returns.
	assertPipelineDispatchProbeProgress(t, c)
	stalled := c.nodes[2].engine.AppliedIndex() < index
	if stalled {
		t.Logf("after genuine receiver heartbeat progress: target=%d receiver=%d sends=%d drops=%d",
			index, c.nodes[2].engine.AppliedIndex(), e.SnapshotStreamSendCount(), e.DispatchDropCount())
		// Rescue only after observing failure, with the omitted LOCAL outcome,
		// never a fabricated quorum/durability ACK.
		select {
		case e.dispatchReportCh <- dispatchReport{to: 3, msgType: raftpb.MsgSnap}:
		case <-time.After(time.Second):
			t.Fatal("missing local failure report rescue could not be delivered")
		}
		c.pumpUntil(t, func() bool { return c.nodes[2].engine.AppliedIndex() >= index }, "actual catch-up after missing local drop feedback")
	}
	return stalled
}

func assertPipelineDispatchDropRetired(t *testing.T, e *Engine, stalled bool) {
	t.Helper()
	var retained int
	for _, report := range e.deferredReadyDispatchReports {
		if report.to == 3 && report.msgType == raftpb.MsgSnap {
			retained++
		}
	}
	if stalled {
		t.Errorf("async owner never flushed its native rejected-snapshot feedback: retained=%d; manual failure report restored actual snapshot catch-up", retained)
	}
	require.Zero(t, retained, "no snapshot drop feedback may remain orphaned after owner processing")
}
