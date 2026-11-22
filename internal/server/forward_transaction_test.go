package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type sharedQueueProposal struct {
	index    uint64
	requests []*pb.Request
	release  chan struct{}
}
type sharedQueueEngine struct {
	mainTSOEngine
	started chan sharedQueueProposal
	stop    <-chan struct{}
	next    atomic.Uint64
}

func (e *sharedQueueEngine) Propose(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	var requests []*pb.Request
	const singleRequestEncoding = 0
	if data[0] == singleRequestEncoding {
		r := new(pb.Request)
		if err := proto.Unmarshal(data[1:], r); err != nil {
			return nil, errors.WithStack(err)
		}
		requests = []*pb.Request{r}
	} else {
		c := new(pb.RaftCommand)
		if err := proto.Unmarshal(data[1:], c); err != nil {
			return nil, errors.WithStack(err)
		}
		requests = c.Requests
	}
	item := sharedQueueProposal{e.next.Add(1), requests, make(chan struct{})}
	e.started <- item
	select {
	case <-item.release:
		return &raftengine.ProposalResult{CommitIndex: item.index}, nil
	case <-e.stop:
		return nil, errors.WithStack(context.Canceled)
	}
}
func (e *sharedQueueEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return e.Propose(ctx, data)
}

type sharedQueueResult struct {
	response *kv.TransactionResponse
	err      error
}

type sharedQueueWaitContext struct {
	context.Context
	queued chan struct{}
	once   sync.Once
}

func (c *sharedQueueWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.queued) })
	return c.Context.Done()
}

func sharedQueueCommit(t *testing.T, tx kv.Transactional, id byte) (<-chan sharedQueueResult, <-chan struct{}) {
	t.Helper()
	result := make(chan sharedQueueResult, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	waiting := &sharedQueueWaitContext{Context: ctx, queued: make(chan struct{})}
	go func() {
		defer cancel()
		response, err := tx.Commit(waiting, []*pb.Request{{Ts: uint64(id), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte{id}, Value: []byte("value")}}}})
		result <- sharedQueueResult{response, err}
	}()
	return result, waiting.queued
}
func TestForwardTransactionSharesGroupRawQueue(t *testing.T) {
	stop := make(chan struct{})
	e := &sharedQueueEngine{mainTSOEngine: mainTSOEngine{state: raftengine.StateLeader}, started: make(chan sharedQueueProposal, 8), stop: stop}
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	group := &kv.ShardGroup{Engine: e, Store: st}
	proxy := kv.NewLeaderProxyForShardGroup(group, kv.WithMigrationRequestAdmission(st, 1))
	group.Txn = proxy
	rt := &clusterbootstrap.GroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 1}, Engine: e, Store: st}}
	forwarded := forwardTransactionForGroup(rt, map[uint64]*kv.ShardGroup{1: group}, nil)
	t.Cleanup(func() { close(stop) })
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	firstResult, _ := sharedQueueCommit(t, group.Txn, 1)
	first := testutil.Recv(t, e.started, time.Second, "first local proposal")
	localResult, localQueued := sharedQueueCommit(t, group.Txn, 2)
	forwardResult, forwardQueued := sharedQueueCommit(t, forwarded, 3)
	testutil.Closed(t, localQueued, time.Second, "local request did not reach its wait")
	testutil.Closed(t, forwardQueued, time.Second, "forward request did not reach its wait")
	// Both calls have reached the same held group. Give the legacy idle queue
	// ample time to start its own proposal; the fixed queue must remain held.
	var premature *sharedQueueProposal
	select {
	case next := <-e.started:
		premature = &next
	case <-time.After(30 * time.Millisecond):
	}
	close(first.release)
	require.NoError(t, testutil.Recv(t, firstResult, time.Second, "first ACK").err)
	if premature != nil {
		close(premature.release)
	}
	next := testutil.Recv(t, e.started, time.Second, "remaining group proposal")
	require.Empty(t, localResult, "local ACK before durable completion")
	if premature == nil {
		require.Empty(t, forwardResult, "forward ACK before durable completion")
	}
	close(next.release)
	local := testutil.Recv(t, localResult, time.Second, "local ACK")
	forward := testutil.Recv(t, forwardResult, time.Second, "forward ACK")
	require.NoError(t, local.err)
	require.NoError(t, forward.err)
	require.Nil(t, premature, "forward traffic used a second independently flushing queue")
	require.Len(t, next.requests, 2)
	require.Equal(t, local.response.CommitIndex, forward.response.CommitIndex)
}

func TestForwardTransactionForGroupRetainsFallback(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	e := &mainTSOEngine{state: raftengine.StateLeader}
	rt := &clusterbootstrap.GroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 7}, Engine: e, Store: st}}
	for _, groups := range []map[uint64]*kv.ShardGroup{nil, {7: nil}, {7: {Engine: e, Store: st}}} {
		tx := forwardTransactionForGroup(rt, groups, nil)
		require.IsType(t, (*kv.TransactionManager)(nil), tx)
		require.True(t, kv.MigrationPartitionRequestAdmissionReady(tx))
		manager, ok := tx.(*kv.TransactionManager)
		require.True(t, ok)
		manager.Close()
	}
}
