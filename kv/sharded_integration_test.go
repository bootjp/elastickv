package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

func newSingleRaft(t *testing.T, id string, fsm raft.FSM) (*raft.Raft, func()) {
	t.Helper()

	addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(id)
	c.HeartbeatTimeout = 50 * time.Millisecond
	c.ElectionTimeout = 100 * time.Millisecond
	c.LeaderLeaseTimeout = 50 * time.Millisecond

	ldb := raft.NewInmemStore()
	sdb := raft.NewInmemStore()
	fss := raft.NewInmemSnapshotStore()
	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, trans)
	if err != nil {
		t.Fatalf("new raft: %v", err)
	}
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(id),
				Address:  addr,
			},
		},
	}
	if err := r.BootstrapCluster(cfg).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	for i := 0; i < 100; i++ {
		if r.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if r.State() != raft.Leader {
		t.Fatalf("node %s is not leader", id)
	}

	return r, func() { r.Shutdown() }
}

func TestShardedCoordinatorDispatch(t *testing.T) {
	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSM(s1))
	defer stop1()

	s2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "g2", NewKvFSM(s2))
	defer stop2()

	groups := map[uint64]*ShardGroup{
		1: {Raft: r1, Store: s1, Txn: NewLeaderProxy(r1)},
		2: {Raft: r2, Store: s2, Txn: NewLeaderProxy(r2)},
	}

	coord := NewShardedCoordinator(engine, groups, 1, NewHLC())
	shardStore := NewShardStore(engine, groups)

	ops := &OperationGroup[OP]{
		IsTxn: false,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	}
	if _, err := coord.Dispatch(ops); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	readTS := shardStore.LastCommitTS()
	v, err := shardStore.GetAt(ctx, []byte("b"), readTS)
	if err != nil || string(v) != "v1" {
		t.Fatalf("get b: %v %v", v, err)
	}
	v, err = shardStore.GetAt(ctx, []byte("x"), readTS)
	if err != nil || string(v) != "v2" {
		t.Fatalf("get x: %v %v", v, err)
	}

	if _, err := s1.GetAt(ctx, []byte("x"), readTS); !errors.Is(err, store.ErrKeyNotFound) {
		t.Fatalf("expected key x missing in group1, got %v", err)
	}
	if _, err := s2.GetAt(ctx, []byte("b"), readTS); !errors.Is(err, store.ErrKeyNotFound) {
		t.Fatalf("expected key b missing in group2, got %v", err)
	}
}
