package kv

import (
	"context"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
)

// newTestRaft creates a single-node raft engine for tests. Historically this
// built a 3-node in-memory hashicorp cluster; with the hashicorp backend
// removed we simply delegate to the newSingleRaft helper used elsewhere.
func newTestRaft(t *testing.T, id string, fsm FSM) (raftengine.Engine, func()) {
	t.Helper()
	return newSingleRaft(t, id, fsm)
}

func TestShardRouterCommit(t *testing.T) {
	ctx := context.Background()

	e := distribution.NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), nil, 2)

	router := NewShardRouter(e)

	// group 1
	s1 := store.NewMVCCStore()
	r1, stop1 := newTestRaft(t, "1", NewKvFSMWithHLC(s1, NewHLC()))
	defer stop1()
	router.Register(1, NewTransactionWithProposer(r1), s1)

	// group 2
	s2 := store.NewMVCCStore()
	r2, stop2 := newTestRaft(t, "2", NewKvFSMWithHLC(s2, NewHLC()))
	defer stop2()
	router.Register(2, NewTransactionWithProposer(r2), s2)

	reqs := []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v1")}}},
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("x"), Value: []byte("v2")}}},
	}

	if _, err := router.Commit(reqs); err != nil {
		t.Fatalf("commit: %v", err)
	}

	v, err := router.Get(ctx, []byte("b"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("group1 get: %v %v", v, err)
	}
	v, err = router.Get(ctx, []byte("x"))
	if err != nil || string(v) != "v2" {
		t.Fatalf("group2 get: %v %v", v, err)
	}
}

func TestShardRouterSplitAndMerge(t *testing.T) {
	ctx := context.Background()

	e := distribution.NewEngine()
	// start with single shard handled by group 1
	e.UpdateRoute([]byte("a"), nil, 1)

	router := NewShardRouter(e)

	// group 1
	s1 := store.NewMVCCStore()
	r1, stop1 := newTestRaft(t, "1", NewKvFSMWithHLC(s1, NewHLC()))
	defer stop1()
	router.Register(1, NewTransactionWithProposer(r1), s1)

	// group 2 (will be used after split)
	s2 := store.NewMVCCStore()
	r2, stop2 := newTestRaft(t, "2", NewKvFSMWithHLC(s2, NewHLC()))
	defer stop2()
	router.Register(2, NewTransactionWithProposer(r2), s2)

	// initial write routed to group 1
	req := []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v1")}}},
	}
	if _, err := router.Commit(req); err != nil {
		t.Fatalf("commit group1: %v", err)
	}
	v, err := router.Get(ctx, []byte("b"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("group1 value before split: %v %v", v, err)
	}

	// split shard: group1 handles [a,m), group2 handles [m,∞)
	e2 := distribution.NewEngine()
	e2.UpdateRoute([]byte("a"), []byte("m"), 1)
	e2.UpdateRoute([]byte("m"), nil, 2)
	router.engine = e2

	// write routed to group 2 after split
	req = []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("x"), Value: []byte("v2")}}},
	}
	if _, err := router.Commit(req); err != nil {
		t.Fatalf("commit group2: %v", err)
	}
	v, err = router.Get(ctx, []byte("x"))
	if err != nil || string(v) != "v2" {
		t.Fatalf("group2 value after split: %v %v", v, err)
	}

	// merge shards back: all keys handled by group1
	e3 := distribution.NewEngine()
	e3.UpdateRoute([]byte("a"), nil, 1)
	router.engine = e3

	// write routed to group1 after merge
	req = []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v3")}}},
	}
	if _, err := router.Commit(req); err != nil {
		t.Fatalf("commit after merge: %v", err)
	}
	v, err = router.Get(ctx, []byte("z"))
	if err != nil || string(v) != "v3" {
		t.Fatalf("group1 value after merge: %v %v", v, err)
	}
}

type fakeTM struct {
	commitErr   bool
	commitCalls int
	abortCalls  int
}

func (f *fakeTM) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	f.commitCalls++
	if f.commitErr {
		return nil, fmt.Errorf("commit fail")
	}
	return &TransactionResponse{}, nil
}

func (f *fakeTM) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	f.abortCalls++
	return &TransactionResponse{}, nil
}

func TestShardRouterCommitFailure(t *testing.T) {
	e := distribution.NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), nil, 2)

	router := NewShardRouter(e)

	ok := &fakeTM{}
	fail := &fakeTM{commitErr: true}
	router.Register(1, ok, nil)
	router.Register(2, fail, nil)

	reqs := []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v1")}}},
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("x"), Value: []byte("v2")}}},
	}

	if _, err := router.Commit(reqs); err == nil {
		t.Fatalf("expected error")
	}

	if fail.commitCalls == 0 || ok.commitCalls == 0 {
		t.Fatalf("expected commits on both groups")
	}

	if ok.abortCalls != 0 {
		t.Fatalf("unexpected abort on successful group")
	}
}

func TestShardRouterRoutesListKeys(t *testing.T) {
	e := distribution.NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), nil, 2)

	router := NewShardRouter(e)

	ok := &fakeTM{}
	fail := &fakeTM{}
	router.Register(1, ok, nil)
	router.Register(2, fail, nil)

	listMetaKey := store.ListMetaKey([]byte("b"))
	reqs := []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: listMetaKey, Value: []byte("v")}}},
	}

	if _, err := router.Commit(reqs); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if ok.commitCalls != 1 {
		t.Fatalf("expected commit routed to group1")
	}
	if fail.commitCalls != 0 {
		t.Fatalf("unexpected commit on group2")
	}
}
