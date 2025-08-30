package kv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
)

// helper to create a multi-node raft cluster and return the leader
func newTestRaft(t *testing.T, id string, fsm raft.FSM) (*raft.Raft, func()) {
	t.Helper()

	const n = 3
	addrs := make([]raft.ServerAddress, n)
	trans := make([]*raft.InmemTransport, n)
	for i := 0; i < n; i++ {
		addr, tr := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("%s-%d", id, i)))
		addrs[i] = addr
		trans[i] = tr
	}
	// fully connect transports
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			trans[i].Connect(addrs[j], trans[j])
			trans[j].Connect(addrs[i], trans[i])
		}
	}

	// cluster configuration
	cfg := raft.Configuration{}
	for i := 0; i < n; i++ {
		cfg.Servers = append(cfg.Servers, raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("%s-%d", id, i)),
			Address: addrs[i],
		})
	}

	rafts := make([]*raft.Raft, n)
	for i := 0; i < n; i++ {
		c := raft.DefaultConfig()
		c.LocalID = cfg.Servers[i].ID
		if i == 0 {
			c.HeartbeatTimeout = 200 * time.Millisecond
			c.ElectionTimeout = 400 * time.Millisecond
			c.LeaderLeaseTimeout = 100 * time.Millisecond
		} else {
			c.HeartbeatTimeout = 1 * time.Second
			c.ElectionTimeout = 2 * time.Second
			c.LeaderLeaseTimeout = 500 * time.Millisecond
		}
		ldb := raft.NewInmemStore()
		sdb := raft.NewInmemStore()
		fss := raft.NewInmemSnapshotStore()
		var rfsm raft.FSM
		if i == 0 {
			rfsm = fsm
		} else {
			rfsm = NewKvFSM(store.NewRbMemoryStore(), store.NewRbMemoryStoreWithExpire(time.Minute))
		}
		r, err := raft.NewRaft(c, rfsm, ldb, sdb, fss, trans[i])
		if err != nil {
			t.Fatalf("new raft %d: %v", i, err)
		}
		if err := r.BootstrapCluster(cfg).Error(); err != nil {
			t.Fatalf("bootstrap %d: %v", i, err)
		}
		rafts[i] = r
	}

	// node 0 should become leader
	for i := 0; i < 100; i++ {
		if rafts[0].State() == raft.Leader {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if rafts[0].State() != raft.Leader {
		t.Fatalf("node %s-0 is not leader", id)
	}

	shutdown := func() {
		for _, r := range rafts {
			r.Shutdown()
		}
	}
	return rafts[0], shutdown
}

func TestShardRouterCommit(t *testing.T) {
	e := distribution.NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), nil, 2)

	router := NewShardRouter(e)

	// group 1
	s1 := store.NewRbMemoryStore()
	l1 := store.NewRbMemoryStoreWithExpire(time.Minute)
	r1, stop1 := newTestRaft(t, "1", NewKvFSM(s1, l1))
	defer stop1()
	router.Register(1, NewTransaction(r1))

	// group 2
	s2 := store.NewRbMemoryStore()
	l2 := store.NewRbMemoryStoreWithExpire(time.Minute)
	r2, stop2 := newTestRaft(t, "2", NewKvFSM(s2, l2))
	defer stop2()
	router.Register(2, NewTransaction(r2))

	reqs := []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v1")}}},
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("x"), Value: []byte("v2")}}},
	}

	_, err := router.Commit(reqs)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	v, err := s1.Get(context.Background(), []byte("b"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("group1 value: %v %v", v, err)
	}
	v, err = s2.Get(context.Background(), []byte("x"))
	if err != nil || string(v) != "v2" {
		t.Fatalf("group2 value: %v %v", v, err)
	}
}

func TestShardRouterSplitAndMerge(t *testing.T) {
	ctx := context.Background()

	e := distribution.NewEngine()
	// start with single shard handled by group 1
	e.UpdateRoute([]byte("a"), nil, 1)

	router := NewShardRouter(e)

	// group 1
	s1 := store.NewRbMemoryStore()
	l1 := store.NewRbMemoryStoreWithExpire(time.Minute)
	r1, stop1 := newTestRaft(t, "1", NewKvFSM(s1, l1))
	defer stop1()
	router.Register(1, NewTransaction(r1))

	// group 2 (will be used after split)
	s2 := store.NewRbMemoryStore()
	l2 := store.NewRbMemoryStoreWithExpire(time.Minute)
	r2, stop2 := newTestRaft(t, "2", NewKvFSM(s2, l2))
	defer stop2()
	router.Register(2, NewTransaction(r2))

	// initial write routed to group 1
	req := []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v1")}}},
	}
	if _, err := router.Commit(req); err != nil {
		t.Fatalf("commit group1: %v", err)
	}
	v, err := s1.Get(ctx, []byte("b"))
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
	v, err = s2.Get(ctx, []byte("x"))
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
	v, err = s1.Get(ctx, []byte("z"))
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
	router.Register(1, ok)
	router.Register(2, fail)

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
