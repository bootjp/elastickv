package kv

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type stubProposalObserver struct {
	mu       sync.Mutex
	failures int
}

func (o *stubProposalObserver) ObserveProposalFailure() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failures++
}

func (o *stubProposalObserver) FailureCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.failures
}

func TestFSMApplyBatchKeepsPerRequestResults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	cmd := &pb.RaftCommand{
		Requests: []*pb.Request{
			{
				IsTxn: false,
				Ts:    10,
				Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte("good"), Value: []byte("v1")},
				},
			},
			{
				IsTxn: false,
				Ts:    11,
				Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: nil, Value: []byte("bad")},
				},
			},
			{
				IsTxn: false,
				Ts:    12,
				Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte("after"), Value: []byte("v2")},
				},
			},
		},
	}
	data, err := proto.Marshal(cmd)
	require.NoError(t, err)

	resp := fsm.Apply(data)
	applyResp, ok := resp.(*fsmApplyResponse)
	require.True(t, ok)
	require.Len(t, applyResp.results, 3)
	require.NoError(t, applyResp.results[0])
	require.ErrorIs(t, applyResp.results[1], ErrInvalidRequest)
	require.NoError(t, applyResp.results[2])

	got, err := st.GetAt(ctx, []byte("good"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), got)

	got, err = st.GetAt(ctx, []byte("after"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), got)
}

func TestTransactionManagerBatchesConcurrentRawCommits(t *testing.T) {
	oldWindow := rawBatchWindow
	rawBatchWindow = 10 * time.Millisecond
	t.Cleanup(func() {
		rawBatchWindow = oldWindow
	})

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "raw-batch", NewKvFSMWithHLC(st, NewHLC()))
	defer stop()

	tm := NewTransactionWithProposer(r)

	req1 := []*pb.Request{{
		IsTxn: false,
		Ts:    100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k1"), Value: []byte("v1")},
		},
	}}
	req2 := []*pb.Request{{
		IsTxn: false,
		Ts:    101,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k2"), Value: []byte("v2")},
		},
	}}

	type result struct {
		resp *TransactionResponse
		err  error
	}

	results := make(chan result, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		resp, err := tm.Commit(context.Background(), req1)
		results <- result{resp: resp, err: err}
	}()
	go func() {
		defer wg.Done()
		<-start
		resp, err := tm.Commit(context.Background(), req2)
		results <- result{resp: resp, err: err}
	}()
	close(start)
	wg.Wait()
	close(results)

	out := make([]result, 0, 2)
	for res := range results {
		out = append(out, res)
	}
	require.Len(t, out, 2)
	require.NoError(t, out[0].err)
	require.NoError(t, out[1].err)
	require.NotNil(t, out[0].resp)
	require.NotNil(t, out[1].resp)
	require.Equal(t, out[0].resp.CommitIndex, out[1].resp.CommitIndex)

	got, err := st.GetAt(context.Background(), []byte("k1"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), got)
	got, err = st.GetAt(context.Background(), []byte("k2"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), got)
}

type rawBacklogProposer struct {
	fakeProposer
	first   chan struct{}
	release chan struct{}
}

func (p *rawBacklogProposer) Propose(_ context.Context, _ []byte) (*raftengine.ProposalResult, error) {
	index := p.calls.Add(1)
	if index == 1 {
		close(p.first)
		<-p.release
	}
	return &raftengine.ProposalResult{CommitIndex: uint64(index)}, nil
}

func TestRawCommitDrainsSmallWriteBacklogAfterProposalStall(t *testing.T) {
	t.Parallel()
	proposer := &rawBacklogProposer{first: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(proposer.release) })
	defer release()
	manager := NewTransactionWithProposer(proposer)
	defer manager.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	const queued = 256
	type outcome struct {
		response *TransactionResponse
		err      error
	}
	results := make(chan outcome, queued)
	submit := func(i int, result chan<- outcome) {
		request := &pb.Request{Ts: uint64(i + 1), Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("buffered-" + strconv.Itoa(i)), Value: make([]byte, 1024)},
		}}
		response, err := manager.Commit(ctx, []*pb.Request{request})
		result <- outcome{response: response, err: err}
	}
	first := make(chan outcome, 1)
	go submit(0, first)
	select {
	case <-proposer.first:
	case <-ctx.Done():
		t.Fatal("first proposal never reached the proposer")
	}
	for i := 1; i <= queued; i++ {
		go submit(i, results)
	}
	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return len(manager.rawPending) == queued
	}, 3*time.Second, time.Millisecond)
	require.Empty(t, results)
	release()

	head := <-first
	require.NoError(t, head.err)
	require.Equal(t, uint64(1), head.response.CommitIndex)
	for range queued {
		result := <-results
		require.NoError(t, result.err)
		require.Equal(t, uint64(2), result.response.CommitIndex, "all queued writes should share the next proposal")
	}
	require.Equal(t, int32(2), proposer.calls.Load())
}

func TestRawBatchCoalescingBoundsBytesAndKeepsRequestGroupsIntact(t *testing.T) {
	t.Parallel()
	item := func(valueSizes ...int) *rawCommitItem {
		reqs := make([]*pb.Request, 0, len(valueSizes))
		for i, size := range valueSizes {
			reqs = append(reqs, &pb.Request{Ts: uint64(i + 1), Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte("key-" + strconv.Itoa(i)), Value: make([]byte, size)},
			}})
		}
		return &rawCommitItem{reqs: reqs, encodedBytes: proto.Size(&pb.RaftCommand{Requests: reqs})}
	}
	first, second, third := item(200*1024), item(200*1024), item(200*1024)
	large := item(300*1024, 300*1024)
	last := item(1)
	manager := NewTransactionWithProposer(nil)
	manager.rawPending = []*rawCommitItem{first, second, third, large, last}

	require.Equal(t, []*rawCommitItem{first, second}, manager.takeRawBatch())
	require.Equal(t, []*rawCommitItem{third}, manager.takeRawBatch())
	require.Equal(t, []*rawCommitItem{large}, manager.takeRawBatch())
	require.Len(t, large.reqs, 2)
	require.Equal(t, []*rawCommitItem{last}, manager.takeRawBatch())
	require.Empty(t, manager.takeRawBatch())
}

func TestApplyRequestsCountsProposalFailureOnRaftApplyError(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "proposal-fail", NewKvFSMWithHLC(st, NewHLC()))
	stop()

	observer := &stubProposalObserver{}
	reqs := []*pb.Request{{
		IsTxn: false,
		Ts:    100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}}

	_, _, err := applyRequests(context.Background(), r, reqs, observer)
	require.Error(t, err)
	require.Equal(t, 1, observer.FailureCount())
}

func TestApplyRequestsDoesNotCountBusinessErrorAsProposalFailure(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "proposal-business-error", NewKvFSMWithHLC(st, NewHLC()))
	defer stop()

	observer := &stubProposalObserver{}
	reqs := []*pb.Request{{
		IsTxn: false,
		Ts:    100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: nil, Value: []byte("bad")},
		},
	}}

	_, results, err := applyRequests(context.Background(), r, reqs, observer)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0], ErrInvalidRequest)
	require.Zero(t, observer.FailureCount())
}

func TestApplyRequestsWithEtcdEngineKeepsKVCommandSemantics(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	engine, err := etcdraftengine.Open(context.Background(), etcdraftengine.OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      t.TempDir(),
		Bootstrap:    true,
		StateMachine: NewKvFSMWithHLC(st, NewHLC()),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})

	observer := &stubProposalObserver{}
	goodReqs := []*pb.Request{{
		IsTxn: false,
		Ts:    100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}}

	commitIndex, results, err := applyRequests(context.Background(), engine, goodReqs, observer)
	require.NoError(t, err)
	require.NotZero(t, commitIndex)
	require.Len(t, results, 1)
	require.NoError(t, results[0])

	got, err := st.GetAt(context.Background(), []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)

	badReqs := []*pb.Request{{
		IsTxn: false,
		Ts:    101,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: nil, Value: []byte("bad")},
		},
	}}

	_, results, err = applyRequests(context.Background(), engine, badReqs, observer)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0], ErrInvalidRequest)
	require.Zero(t, observer.FailureCount())
}
