package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const testDispatchReportTimeout = 2 * time.Second

// TestPostDispatchReport_DeliversWhenChannelHasSpace verifies that a failure
// report from the dispatch worker is delivered to the event loop through the
// dedicated channel, which is what allows the engine to call
// rawNode.ReportUnreachable / ReportSnapshot from the correct goroutine.
func TestPostDispatchReport_DeliversWhenChannelHasSpace(t *testing.T) {
	t.Parallel()
	e := &Engine{
		dispatchReportCh: make(chan dispatchReport, 4),
		closeCh:          make(chan struct{}),
	}
	report := dispatchReport{to: 42, msgType: raftpb.MsgSnap}

	e.postDispatchReport(report)

	select {
	case got := <-e.dispatchReportCh:
		require.Equal(t, report, got)
	default:
		t.Fatal("expected dispatchReport to be delivered to channel")
	}
}

func TestReportSuccessfulDispatchReportsSnapshotFinish(t *testing.T) {
	t.Parallel()
	e := &Engine{
		dispatchReportCh: make(chan dispatchReport, 1),
		closeCh:          make(chan struct{}),
	}

	e.reportSuccessfulDispatch(raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2)})

	select {
	case got := <-e.dispatchReportCh:
		require.Equal(t, dispatchReport{to: 2, msgType: raftpb.MsgSnap, snapshotFinish: true}, got)
	default:
		t.Fatal("expected successful MsgSnap dispatch to report SnapshotFinish input")
	}
}

func TestReportSuccessfulDispatchWaitsForSnapshotFinishSlot(t *testing.T) {
	t.Parallel()
	e := &Engine{
		dispatchReportCh: make(chan dispatchReport, 1),
		closeCh:          make(chan struct{}),
	}
	blockingReport := dispatchReport{to: 1, msgType: raftpb.MsgApp}
	e.dispatchReportCh <- blockingReport

	done := make(chan struct{})
	go func() {
		e.reportSuccessfulDispatch(raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2)})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("snapshot finish report returned while dispatchReportCh was full")
	case <-time.After(50 * time.Millisecond):
	}

	require.Equal(t, blockingReport, <-e.dispatchReportCh)
	select {
	case <-done:
	case <-time.After(testDispatchReportTimeout):
		t.Fatal("snapshot finish report did not complete after dispatchReportCh had space")
	}
	select {
	case got := <-e.dispatchReportCh:
		require.Equal(t, dispatchReport{to: 2, msgType: raftpb.MsgSnap, snapshotFinish: true}, got)
	default:
		t.Fatal("expected reliable successful MsgSnap dispatch report")
	}
}

func TestReportSuccessfulDispatchAbortsOnCloseWhenReportFull(t *testing.T) {
	t.Parallel()
	e := &Engine{
		dispatchReportCh: make(chan dispatchReport, 1),
		closeCh:          make(chan struct{}),
	}
	e.dispatchReportCh <- dispatchReport{to: 1, msgType: raftpb.MsgApp}
	close(e.closeCh)

	done := make(chan struct{})
	go func() {
		e.reportSuccessfulDispatch(raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2)})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testDispatchReportTimeout):
		t.Fatal("snapshot finish report did not abort when closeCh was signalled")
	}
}

func TestReportSuccessfulDispatchIgnoresRegularMessage(t *testing.T) {
	t.Parallel()
	e := &Engine{
		dispatchReportCh: make(chan dispatchReport, 1),
		closeCh:          make(chan struct{}),
	}

	e.reportSuccessfulDispatch(raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), To: uint64Ptr(2)})

	select {
	case got := <-e.dispatchReportCh:
		t.Fatalf("unexpected dispatch report for regular message: %+v", got)
	default:
	}
}

// TestPostDispatchReport_DropsWhenChannelFull asserts the non-blocking
// contract: dispatch workers must not stall because the event loop is busy.
// The worst case is an eventually-consistent gap that raft will fix on the
// next heartbeat-driven retry, so dropping under pressure is acceptable.
func TestPostDispatchReport_DropsWhenChannelFull(t *testing.T) {
	t.Parallel()
	e := &Engine{
		dispatchReportCh: make(chan dispatchReport, 1),
		closeCh:          make(chan struct{}),
	}
	e.dispatchReportCh <- dispatchReport{to: 1, msgType: raftpb.MsgApp}

	done := make(chan struct{})
	go func() {
		e.postDispatchReport(dispatchReport{to: 2, msgType: raftpb.MsgApp})
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), testDispatchReportTimeout)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("postDispatchReport blocked while channel was full")
	}
}

// TestPostDispatchReport_AbortsOnClose ensures the worker does not get stuck
// posting reports during shutdown when the channel is full.
func TestPostDispatchReport_AbortsOnClose(t *testing.T) {
	t.Parallel()
	e := &Engine{
		dispatchReportCh: make(chan dispatchReport, 1),
		closeCh:          make(chan struct{}),
	}
	e.dispatchReportCh <- dispatchReport{to: 1, msgType: raftpb.MsgApp}
	close(e.closeCh)

	done := make(chan struct{})
	go func() {
		e.postDispatchReport(dispatchReport{to: 2, msgType: raftpb.MsgApp})
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), testDispatchReportTimeout)
	defer cancel()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("postDispatchReport did not abort when closeCh was signalled")
	}
}

func TestEnqueueDispatchDefersDroppedSnapshotReportWhenLaneFull(t *testing.T) {
	t.Parallel()
	snapshotCh := make(chan dispatchRequest, 1)
	snapshotCh <- dispatchRequest{msg: raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2)}}
	e := &Engine{
		transport:              &GRPCTransport{},
		dispatcherLanesEnabled: true,
		dispatchReportCh:       make(chan dispatchReport, 1),
		closeCh:                make(chan struct{}),
		peerDispatchers: map[uint64]*peerQueues{
			2: {snapshot: snapshotCh},
		},
	}

	require.NoError(t, e.enqueueDispatchMessage(raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), To: uint64Ptr(2)}))

	require.Equal(t, uint64(1), e.DispatchDropCount())
	select {
	case got := <-e.dispatchReportCh:
		t.Fatalf("dropped MsgSnap should defer without queueing: %+v", got)
	default:
	}
	require.Equal(t, []dispatchReport{{to: 2, msgType: raftpb.MsgSnap}}, e.deferredReadyDispatchReports)
}

func TestEnqueueDispatchReportsDroppedRegularMessageWhenLaneFull(t *testing.T) {
	t.Parallel()
	replicationCh := make(chan dispatchRequest, 1)
	replicationCh <- dispatchRequest{msg: raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), To: uint64Ptr(2)}}
	e := &Engine{
		transport:              &GRPCTransport{},
		dispatcherLanesEnabled: true,
		dispatchReportCh:       make(chan dispatchReport, 1),
		closeCh:                make(chan struct{}),
		peerDispatchers: map[uint64]*peerQueues{
			2: {replication: replicationCh},
		},
	}

	require.NoError(t, e.enqueueDispatchMessage(raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), To: uint64Ptr(2)}))

	require.Equal(t, uint64(1), e.DispatchDropCount())
	select {
	case got := <-e.dispatchReportCh:
		require.Equal(t, dispatchReport{to: 2, msgType: raftpb.MsgApp}, got)
	default:
		t.Fatal("expected dropped MsgApp to report unreachable input")
	}
}
