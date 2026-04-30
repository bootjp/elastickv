package kv

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// fakePartitionResolver is a hand-rolled PartitionResolver for the
// router-side tests — keeps the tests free of any adapter-package
// dependency so the routing contract is verified at the kv layer in
// isolation.
type fakePartitionResolver struct {
	routes map[string]uint64
}

func (f *fakePartitionResolver) ResolveGroup(key []byte) (uint64, bool) {
	gid, ok := f.routes[string(key)]
	return gid, ok
}

// TestShardRouter_PartitionResolverWins pins that when the resolver
// claims a key, the router dispatches to the resolver's group even
// if the byte-range engine would have routed elsewhere. This is the
// whole point of the resolver — overlay routing on top of the
// existing non-overlapping cover model.
func TestShardRouter_PartitionResolverWins(t *testing.T) {
	t.Parallel()
	e := distribution.NewEngine()
	e.UpdateRoute([]byte(""), nil, 1) // engine routes everything to group 1

	router := NewShardRouter(e)
	router.WithPartitionResolver(&fakePartitionResolver{
		routes: map[string]uint64{"resolver-key": 42},
	})

	// Per-test sink so a parallel sibling test cannot perturb the
	// invariant we are checking. Each fakeTxn writes its own id
	// into this slot on Commit; the post-condition reads it back.
	var sink atomic.Uint64
	s1 := store.NewMVCCStore()
	s42 := store.NewMVCCStore()
	router.Register(1, &fakeTxn{id: 1, sink: &sink}, s1)
	router.Register(42, &fakeTxn{id: 42, sink: &sink}, s42)

	reqs := []*pb.Request{
		{IsTxn: false, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("resolver-key"), Value: []byte("v")}}},
	}
	resp, err := router.Commit(reqs)
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Verify: the request landed on group 42's fake txn, not 1's.
	require.Equal(t, uint64(42), sink.Load())
}

// TestShardRouter_PartitionResolverFallsThrough pins that when the
// resolver returns (0, false), dispatch falls through to the byte-
// range engine. Without this, the resolver would have to know
// every key in the cluster — which would defeat the overlay
// pattern's purpose (let the resolver answer only for partitioned-
// keyspace keys).
func TestShardRouter_PartitionResolverFallsThrough(t *testing.T) {
	t.Parallel()
	e := distribution.NewEngine()
	e.UpdateRoute([]byte("a"), []byte("m"), 1)
	e.UpdateRoute([]byte("m"), nil, 2)

	router := NewShardRouter(e)
	// Resolver only knows about "resolver-only-key"; everything
	// else falls through to the engine.
	router.WithPartitionResolver(&fakePartitionResolver{
		routes: map[string]uint64{"resolver-only-key": 99},
	})

	var sink atomic.Uint64
	s1 := store.NewMVCCStore()
	s2 := store.NewMVCCStore()
	router.Register(1, &fakeTxn{id: 1, sink: &sink}, s1)
	router.Register(2, &fakeTxn{id: 2, sink: &sink}, s2)

	// "b" is in the engine's [a, m) range → group 1.
	resp1, err1 := router.Commit([]*pb.Request{
		{IsTxn: false, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("v")}}},
	})
	require.NoError(t, err1)
	require.NotNil(t, resp1)
	require.Equal(t, uint64(1), sink.Load(),
		"engine [a,m) range must route to group 1")

	// "x" is in the engine's [m, ∞) range → group 2.
	resp2, err2 := router.Commit([]*pb.Request{
		{IsTxn: false, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("x"), Value: []byte("v")}}},
	})
	require.NoError(t, err2)
	require.NotNil(t, resp2)
	require.Equal(t, uint64(2), sink.Load(),
		"engine [m,∞) range must route to group 2")
}

// TestShardRouter_NilPartitionResolverIsNoOp pins that
// WithPartitionResolver(nil) leaves the router behaving exactly as
// the legacy engine-only dispatcher. This is the documented "no
// partition layer" path that a non-partitioned cluster takes.
func TestShardRouter_NilPartitionResolverIsNoOp(t *testing.T) {
	t.Parallel()
	e := distribution.NewEngine()
	e.UpdateRoute([]byte(""), nil, 7)

	router := NewShardRouter(e)
	router.WithPartitionResolver(nil)

	var sink atomic.Uint64
	s7 := store.NewMVCCStore()
	router.Register(7, &fakeTxn{id: 7, sink: &sink}, s7)

	// With no resolver installed, the engine's default route owns
	// the request — group 7 dispatches.
	resp, err := router.Commit([]*pb.Request{
		{IsTxn: false, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("any"), Value: []byte("v")}}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(7), sink.Load(),
		"nil resolver must leave the engine in charge")
}

// TestShardRouter_GetUsesResolver pins that the resolver-first path
// applies to Get as well as Commit/Abort. A regression that fixed
// only Commit's path would silently route reads through the engine
// even after the resolver claimed the key.
func TestShardRouter_GetUsesResolver(t *testing.T) {
	t.Parallel()
	e := distribution.NewEngine()
	e.UpdateRoute([]byte(""), nil, 1)

	router := NewShardRouter(e)
	router.WithPartitionResolver(&fakePartitionResolver{
		routes: map[string]uint64{"resolver-key": 42},
	})

	var sink atomic.Uint64
	s1 := store.NewMVCCStore()
	s42 := store.NewMVCCStore()
	// Seed group 42 only — if Get falls through to the engine
	// (which would route to group 1), the test fails because
	// group 1's store is empty.
	require.NoError(t, s42.PutAt(context.Background(), []byte("resolver-key"), []byte("v"), 1, 0))
	router.Register(1, &fakeTxn{id: 1, sink: &sink}, s1)
	router.Register(42, &fakeTxn{id: 42, sink: &sink}, s42)

	v, err := router.Get(context.Background(), []byte("resolver-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)
}

// fakeTxn is a Transactional double that records its own id into a
// caller-provided sink whenever Commit lands on it. Using a per-
// test sink (rather than a package-level variable) keeps parallel
// tests from clobbering each other's observations.
type fakeTxn struct {
	id   uint64
	sink *atomic.Uint64
}

func (f *fakeTxn) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	if f.sink != nil {
		f.sink.Store(f.id)
	}
	return &TransactionResponse{CommitIndex: 1}, nil
}

func (f *fakeTxn) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	return &TransactionResponse{}, nil
}
