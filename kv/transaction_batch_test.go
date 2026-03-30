package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
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
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSM(st).(*kvFSM)
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

	resp := fsm.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
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
	r, stop := newSingleRaft(t, "raw-batch", NewKvFSM(st))
	defer stop()

	tm := NewTransaction(r)

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
		resp, err := tm.Commit(req1)
		results <- result{resp: resp, err: err}
	}()
	go func() {
		defer wg.Done()
		<-start
		resp, err := tm.Commit(req2)
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

func TestApplyRequestsCountsProposalFailureOnRaftApplyError(t *testing.T) {
	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "proposal-fail", NewKvFSM(st))
	stop()

	observer := &stubProposalObserver{}
	reqs := []*pb.Request{{
		IsTxn: false,
		Ts:    100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}}

	_, _, err := applyRequests(r, reqs, observer)
	require.Error(t, err)
	require.Equal(t, 1, observer.FailureCount())
}

func TestApplyRequestsDoesNotCountBusinessErrorAsProposalFailure(t *testing.T) {
	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "proposal-business-error", NewKvFSM(st))
	defer stop()

	observer := &stubProposalObserver{}
	reqs := []*pb.Request{{
		IsTxn: false,
		Ts:    100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: nil, Value: []byte("bad")},
		},
	}}

	_, results, err := applyRequests(r, reqs, observer)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0], ErrInvalidRequest)
	require.Zero(t, observer.FailureCount())
}
