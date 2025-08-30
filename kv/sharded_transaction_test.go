package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
)

// helper to create single-node raft
func newTestRaft(t *testing.T, id string, fsm raft.FSM) *raft.Raft {
	t.Helper()
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(id)
	ldb := raft.NewInmemStore()
	sdb := raft.NewInmemStore()
	fss := raft.NewInmemSnapshotStore()
	addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, trans)
	if err != nil {
		t.Fatalf("new raft: %v", err)
	}
	cfg := raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(id), Address: addr}}}
	if err := r.BootstrapCluster(cfg).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// single node should eventually become leader
	for i := 0; i < 100; i++ {
		if r.State() == raft.Leader {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if r.State() != raft.Leader {
		t.Fatalf("node %s is not leader", id)
	}
	return r
}

func TestShardedTransactionManagerCommit(t *testing.T) {
	e := distribution.NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), nil, 2)

	stm := NewShardedTransactionManager(e)

	// group 1
	s1 := store.NewRbMemoryStore()
	l1 := store.NewRbMemoryStoreWithExpire(time.Minute)
	r1 := newTestRaft(t, "1", NewKvFSM(s1, l1))
	defer r1.Shutdown()
	stm.Register(1, NewTransaction(r1))

	// group 2
	s2 := store.NewRbMemoryStore()
	l2 := store.NewRbMemoryStoreWithExpire(time.Minute)
	r2 := newTestRaft(t, "2", NewKvFSM(s2, l2))
	defer r2.Shutdown()
	stm.Register(2, NewTransaction(r2))

	reqs := []*pb.Request{
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v1")}}},
		{IsTxn: false, Phase: pb.Phase_NONE, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("x"), Value: []byte("v2")}}},
	}

	_, err := stm.Commit(reqs)
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
