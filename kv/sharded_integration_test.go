package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	testSingleRaftTickInterval  = 10 * time.Millisecond
	testSingleRaftHeartbeatTick = 1
	testSingleRaftElectionTick  = 10
	testSingleRaftMaxSizePerMsg = 1 << 20
	testSingleRaftMaxInflight   = 256
)

func newSingleRaftFactory() raftengine.Factory {
	return etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval:   testSingleRaftTickInterval,
		HeartbeatTick:  testSingleRaftHeartbeatTick,
		ElectionTick:   testSingleRaftElectionTick,
		MaxSizePerMsg:  testSingleRaftMaxSizePerMsg,
		MaxInflightMsg: testSingleRaftMaxInflight,
	})
}

func newSingleRaft(t *testing.T, id string, fsm FSM) (raftengine.Engine, func()) {
	t.Helper()

	factory := newSingleRaftFactory()
	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID:      id,
		LocalAddress: id,
		DataDir:      t.TempDir(),
		Bootstrap:    true,
		StateMachine: fsm,
	})
	if err != nil {
		t.Fatalf("new raft: %v", err)
	}

	for range 200 {
		if result.Engine.State() == raftengine.StateLeader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if result.Engine.State() != raftengine.StateLeader {
		_ = result.Engine.Close()
		if result.Close != nil {
			_ = result.Close()
		}
		t.Fatalf("node %s is not leader", id)
	}

	return result.Engine, func() {
		_ = result.Engine.Close()
		if result.Close != nil {
			_ = result.Close()
		}
	}
}

func TestShardedCoordinatorDispatch(t *testing.T) {
	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSMWithHLC(s1, NewHLC()))
	defer stop1()

	s2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "g2", NewKvFSMWithHLC(s2, NewHLC()))
	defer stop2()

	e1 := r1
	e2 := r2
	groups := map[uint64]*ShardGroup{
		1: {Engine: e1, Store: s1, Txn: NewLeaderProxyWithEngine(e1)},
		2: {Engine: e2, Store: s2, Txn: NewLeaderProxyWithEngine(e2)},
	}

	shardStore := NewShardStore(engine, groups)
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), shardStore)

	ops := &OperationGroup[OP]{
		IsTxn: false,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	}
	if _, err := coord.Dispatch(ctx, ops); err != nil {
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

func TestShardedCoordinatorDispatch_CrossShardTxnSucceeds(t *testing.T) {
	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "g1", NewKvFSMWithHLC(s1, NewHLC()))
	defer stop1()

	s2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "g2", NewKvFSMWithHLC(s2, NewHLC()))
	defer stop2()

	e1 := r1
	e2 := r2
	groups := map[uint64]*ShardGroup{
		1: {Engine: e1, Store: s1, Txn: NewLeaderProxyWithEngine(e1)},
		2: {Engine: e2, Store: s2, Txn: NewLeaderProxyWithEngine(e2)},
	}

	shardStore := NewShardStore(engine, groups)
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), shardStore)

	ops := &OperationGroup[OP]{
		IsTxn: true,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	}
	if _, err := coord.Dispatch(ctx, ops); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	readTS := shardStore.LastCommitTS()
	if v, err := shardStore.GetAt(ctx, []byte("b"), readTS); err != nil || string(v) != "v1" {
		t.Fatalf("get b: %v %v", v, err)
	}
	if v, err := shardStore.GetAt(ctx, []byte("x"), readTS); err != nil || string(v) != "v2" {
		t.Fatalf("get x: %v %v", v, err)
	}
}
