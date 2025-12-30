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
	addrs, trans := setupInmemTransports(id, n)
	connectInmemTransports(addrs, trans)
	cfg := buildRaftConfig(id, addrs)
	rafts := initTestRafts(t, cfg, trans, fsm)
	waitForLeader(t, id, rafts[0])

	shutdown := func() {
		for _, r := range rafts {
			r.Shutdown()
		}
	}
	return rafts[0], shutdown
}

func setupInmemTransports(id string, n int) ([]raft.ServerAddress, []*raft.InmemTransport) {
	addrs := make([]raft.ServerAddress, n)
	trans := make([]*raft.InmemTransport, n)
	for i := 0; i < n; i++ {
		addr, tr := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("%s-%d", id, i)))
		addrs[i] = addr
		trans[i] = tr
	}
	return addrs, trans
}

func connectInmemTransports(addrs []raft.ServerAddress, trans []*raft.InmemTransport) {
	// fully connect transports
	for i := 0; i < len(trans); i++ {
		for j := i + 1; j < len(trans); j++ {
			trans[i].Connect(addrs[j], trans[j])
			trans[j].Connect(addrs[i], trans[i])
		}
	}
}

func buildRaftConfig(id string, addrs []raft.ServerAddress) raft.Configuration {
	// cluster configuration
	cfg := raft.Configuration{}
	for i := 0; i < len(addrs); i++ {
		cfg.Servers = append(cfg.Servers, raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("%s-%d", id, i)),
			Address: addrs[i],
		})
	}
	return cfg
}

func initTestRafts(t *testing.T, cfg raft.Configuration, trans []*raft.InmemTransport, fsm raft.FSM) []*raft.Raft {
	t.Helper()

	rafts := make([]*raft.Raft, len(trans))
	for i := 0; i < len(trans); i++ {
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
			rfsm = NewKvFSM(store.NewMVCCStore())
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

	return rafts
}

func waitForLeader(t *testing.T, id string, leader *raft.Raft) {
	t.Helper()

	// node 0 should become leader quickly during tests
	for i := 0; i < 100; i++ {
		if leader.State() == raft.Leader {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader.State() != raft.Leader {
		t.Fatalf("node %s-0 is not leader", id)
	}
}

func TestShardRouterCommit(t *testing.T) {
	ctx := context.Background()

	e := distribution.NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), nil, 2)

	router := NewShardRouter(e)

	// group 1
	s1 := store.NewMVCCStore()
	r1, stop1 := newTestRaft(t, "1", NewKvFSM(s1))
	defer stop1()
	router.Register(1, NewTransaction(r1), s1)

	// group 2
	s2 := store.NewMVCCStore()
	r2, stop2 := newTestRaft(t, "2", NewKvFSM(s2))
	defer stop2()
	router.Register(2, NewTransaction(r2), s2)

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
	r1, stop1 := newTestRaft(t, "1", NewKvFSM(s1))
	defer stop1()
	router.Register(1, NewTransaction(r1), s1)

	// group 2 (will be used after split)
	s2 := store.NewMVCCStore()
	r2, stop2 := newTestRaft(t, "2", NewKvFSM(s2))
	defer stop2()
	router.Register(2, NewTransaction(r2), s2)

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
