package kv

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
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
	r, stop := newSingleRaft(t, "raw-batch", NewKvFSMWithHLC(st, NewHLC()))
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

	_, _, err := applyRequests(hashicorpraftengine.New(r), reqs, observer)
	require.Error(t, err)
	require.Equal(t, 1, observer.FailureCount())
}

func TestApplyRequestsDoesNotCountBusinessErrorAsProposalFailure(t *testing.T) {
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

	_, results, err := applyRequests(hashicorpraftengine.New(r), reqs, observer)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0], ErrInvalidRequest)
	require.Zero(t, observer.FailureCount())
}

type etcdFSMAdapter struct {
	fsm raft.FSM
}

func (a etcdFSMAdapter) Apply(data []byte) any {
	return a.fsm.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
}

func (a etcdFSMAdapter) Snapshot() (etcdraftengine.Snapshot, error) {
	snapshot, err := a.fsm.Snapshot()
	if err != nil {
		return nil, err
	}
	return hashicorpSnapshotAdapter{snapshot: snapshot}, nil
}

func (a etcdFSMAdapter) Restore(r io.Reader) error {
	return a.fsm.Restore(io.NopCloser(r))
}

type hashicorpSnapshotAdapter struct {
	snapshot raft.FSMSnapshot
}

func (a hashicorpSnapshotAdapter) WriteTo(w io.Writer) (int64, error) {
	sink := &snapshotSinkAdapter{writer: w}
	if err := a.snapshot.Persist(sink); err != nil {
		return sink.written, err
	}
	return sink.written, nil
}

func (a hashicorpSnapshotAdapter) Close() error {
	a.snapshot.Release()
	return nil
}

type snapshotSinkAdapter struct {
	writer  io.Writer
	written int64
}

func (s *snapshotSinkAdapter) ID() string {
	return "etcd-fsm-adapter"
}

func (s *snapshotSinkAdapter) Cancel() error {
	return nil
}

func (s *snapshotSinkAdapter) Close() error {
	return nil
}

func (s *snapshotSinkAdapter) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	s.written += int64(n)
	return n, err
}

func TestApplyRequestsWithEtcdEngineKeepsKVCommandSemantics(t *testing.T) {
	st := store.NewMVCCStore()
	engine, err := etcdraftengine.Open(context.Background(), etcdraftengine.OpenConfig{
		NodeID:       1,
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:7001",
		DataDir:      t.TempDir(),
		Bootstrap:    true,
		StateMachine: etcdFSMAdapter{fsm: NewKvFSMWithHLC(st, NewHLC())},
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

	commitIndex, results, err := applyRequests(engine, goodReqs, observer)
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

	_, results, err = applyRequests(engine, badReqs, observer)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0], ErrInvalidRequest)
	require.Zero(t, observer.FailureCount())
}

func TestNeedsTxnCleanupSkipsAbortRequests(t *testing.T) {
	t.Parallel()

	reqs := []*pb.Request{
		{IsTxn: true, Phase: pb.Phase_ABORT},
	}
	require.False(t, needsTxnCleanup(reqs))
	require.True(t, needsTxnCleanup([]*pb.Request{{IsTxn: true, Phase: pb.Phase_PREPARE}}))
	require.True(t, needsTxnCleanup([]*pb.Request{{IsTxn: true, Phase: pb.Phase_COMMIT}}))
	require.False(t, needsTxnCleanup([]*pb.Request{{IsTxn: true, Phase: pb.Phase_NONE}}))
	require.True(t, needsTxnCleanup([]*pb.Request{{IsTxn: true, Phase: pb.Phase(99)}}))
}
