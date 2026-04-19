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
