package etcd

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type pipelineDispatchFaultStorage struct {
	etcdstorage.Storage
	armed        atomic.Bool
	failure      error
	closeStarted atomic.Bool
}

func (d *pipelineDispatchFaultStorage) Save(state *raftpb.HardState, entries []*raftpb.Entry) error {
	if d.armed.Load() {
		return d.failure
	}
	return d.Storage.Save(state, entries)
}

func (d *pipelineDispatchFaultStorage) Close() error {
	d.closeStarted.Store(true)
	return d.Storage.Close()
}

func holdPipelineDispatchSnapshot(e *Engine, capture func(dispatchRequest), release <-chan struct{}) {
	original := e.dispatchFn
	var intercepted atomic.Bool
	e.dispatchFn = func(ctx context.Context, request dispatchRequest) error {
		err := original(ctx, request)
		if err == nil && request.msg.GetType() == raftpb.MsgSnap && request.msg.GetTo() == 3 && request.msg.GetSnapshot().GetMetadata().GetIndex() >= 3 && intercepted.CompareAndSwap(false, true) {
			capture(request)
			<-release // The real gRPC transfer has already succeeded.
		}
		return err
	}
}

func closePipelineDispatchCluster(t *testing.T, c *nativeSnapshotCluster) {
	t.Helper()
	for _, node := range c.nodes {
		closed := make(chan error, 1)
		go func() { closed <- node.engine.Close() }()
		err := testutil.Recv(t, closed, time.Second, "native dispatch engine %d did not join", node.peer.NodeID)
		if failure := c.failures[node.peer.NodeID]; failure != nil {
			require.ErrorIs(t, err, failure)
		} else {
			require.NoError(t, err)
		}
	}
}

type pipelineDispatchFaultRuntime struct {
	cluster          *nativeSnapshotCluster
	engine           *Engine
	disk             *pipelineDispatchFaultStorage
	rpcReturned      <-chan uint64
	unblock          func()
	index            uint64
	proposalReturned <-chan struct{}
}

func newPipelineDispatchFaultRuntime(t *testing.T) *pipelineDispatchFaultRuntime {
	t.Helper()
	disk := &pipelineDispatchFaultStorage{failure: errors.New("native WAL fault after actual snapshot RPC success")}
	rpcReturned := make(chan uint64, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	c := newNativeSnapshotClusterWithHook(t, "", "", nil, func(e *Engine, i int) {
		if i != 0 {
			return
		}
		disk.Storage = e.persist
		e.persist, e.storagePipeline.persist = disk, disk
		holdPipelineDispatchSnapshot(e, func(request dispatchRequest) {
			rpcReturned <- request.msg.GetSnapshot().GetMetadata().GetIndex()
		}, release)
	})
	c.failures[1] = disk.failure
	r := &pipelineDispatchFaultRuntime{cluster: c, engine: c.nodes[0].engine, disk: disk, rpcReturned: rpcReturned, unblock: unblock}
	t.Cleanup(func() {
		unblock()
		r.engine.requestShutdown()
		closePipelineDispatchCluster(t, c)
		if r.proposalReturned != nil {
			testutil.Closed(t, r.proposalReturned, time.Second, "failed proposal caller did not return")
		}
	})
	return r
}

func (r *pipelineDispatchFaultRuntime) trigger(t *testing.T) <-chan struct{} {
	t.Helper()
	c, e := r.cluster, r.engine
	c.dropThird = true
	r.index = c.propose(t, []byte("native-snapshot-before-fatal"))
	c.pumpUntil(t, func() bool { first, _ := e.storage.FirstIndex(); return first > r.index }, "native snapshot compaction")
	c.dropThird = false
	var heldIndex uint64
	c.pumpUntil(t, func() bool {
		select {
		case heldIndex = <-r.rpcReturned:
			return true
		default:
			return false
		}
	}, "actual gRPC snapshot transfer success")
	require.Equal(t, r.index, heldIndex)
	require.Positive(t, dispatchSnapshotPinCount(e.storagePipeline.snapshotFiles, r.index))
	r.disk.armed.Store(true)
	proposalReturned := make(chan struct{})
	r.proposalReturned = proposalReturned
	go func() {
		_, _ = e.Propose(t.Context(), []byte("native-WAL-failure"))
		close(proposalReturned)
	}()
	c.pumpUntil(t, func() bool { return errors.Is(e.Err(), r.disk.failure) }, "real WAL actor failure")
	for _, joined := range []<-chan struct{}{e.storagePipeline.disk.done, e.storagePipeline.apply.done} {
		testutil.Closed(t, joined, time.Second, "storage actor did not actually join")
	}
	return proposalReturned
}

func TestEnginePipelineDispatchFatalSnapshotReportMustJoinWithoutExternalClose(t *testing.T) {
	testPipelineDispatchFatalReport(t, true)
}

func TestEnginePipelineDispatchFatalUncongestedReportJoins(t *testing.T) {
	testPipelineDispatchFatalReport(t, false)
}

func testPipelineDispatchFatalReport(t *testing.T, congested bool) {
	t.Helper()
	r := newPipelineDispatchFaultRuntime(t)
	proposalReturned := r.trigger(t)
	e := r.engine
	if congested {
		// Fill capacity, not Raft ACKs. The quiescing owner does not consume it.
		for i := 0; i < cap(e.dispatchReportCh); i++ {
			select {
			case e.dispatchReportCh <- dispatchReport{to: 2}:
			default:
			}
		}
	}
	r.unblock()
	select {
	case <-e.Done():
		testutil.Closed(t, proposalReturned, time.Second, "failed proposal caller did not return")
		require.ErrorIs(t, e.Err(), r.disk.failure)
		require.True(t, r.disk.closeStarted.Load())
		require.Zero(t, dispatchSnapshotPinCount(e.storagePipeline.snapshotFiles, r.index))
		used, items := e.storagePipeline.outboundBulk.usage()
		require.Zero(t, used)
		require.Zero(t, items)
		return
	case <-time.After(200 * time.Millisecond):
	}
	used, items := e.storagePipeline.outboundBulk.usage()
	t.Logf("joined storage actors, fatal=%v, request bytes=%d items=%d pins=%d persistCloseStarted=%v reports=%d/%d",
		e.Err(), used, items, dispatchSnapshotPinCount(e.storagePipeline.snapshotFiles, r.index), r.disk.closeStarted.Load(),
		len(e.dispatchReportCh), cap(e.dispatchReportCh))
	// Rescue only after recording the missing automatic fatal join.
	e.requestShutdown()
	testutil.Closed(t, e.Done(), time.Second, "external Close rescue did not join")
	testutil.Closed(t, proposalReturned, time.Second, "rescued proposal caller did not return")
	t.Errorf("fatal pipeline shutdown requires external Close to escape reliable snapshot report (congested=%v)", congested)
}
