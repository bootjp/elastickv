package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type readMarkerSampler struct {
	recordingSampler
	marks []uint64
}

func (s *readMarkerSampler) MarkUnattributableClientRead(routeID uint64) {
	s.marks = append(s.marks, routeID)
}

func TestMarkUnattributableClientReadThroughWrappers(t *testing.T) {
	marker := &readMarkerSampler{}
	coordinate := (&Coordinate{engine: newStubLeaderEngine()}).WithSampler(marker, 7)
	wrapped := WithKeyVizLabel(CoordinatorForwarder{Inner: coordinate}, keyviz.LabelRedis)
	MarkUnattributableClientReadThrough(wrapped, 0)
	require.Equal(t, []uint64{0}, marker.marks)
	MarkUnattributableClientReadThrough(&nopCoordinator{}, 0)

	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	sharded := NewShardedCoordinator(routes, map[uint64]*ShardGroup{}, 1, NewHLC(), nil).WithSampler(marker)
	MarkUnattributableClientReadThrough(WithKeyVizLabel(sharded, keyviz.LabelRedis), 0)
	require.Equal(t, []uint64{0, 0}, marker.marks)
}

func TestCoordinateObserveReadForKeyTelemetryOnly(t *testing.T) {
	engine := newStubLeaderEngine()
	rec := &recordingSampler{}
	c := (&Coordinate{engine: engine}).WithSamplerRouteResolver(rec, func(key []byte) (uint64, bool) {
		require.Equal(t, []byte("!sqs|route|global"), key)
		return 7, true
	})
	key := []byte("!sqs|msg|data|p|raw-test-key")
	// A canceled context would fail a Raft read; telemetry does not use it.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ObserveReadForKeyThrough(WithKeyVizLabel(c, keyviz.LabelRedis), ctx, key)
	require.Equal(t, []sampleCall{{routeID: 7, op: keyviz.OpRead, label: keyviz.LabelRedis, key: routeKey(key), keyLen: len(routeKey(key))}}, rec.snapshot())

	engine.StateValue = raftengine.StateFollower
	ObserveReadForKeyThrough(c, context.Background(), key)
	require.Len(t, rec.snapshot(), 1)
	ObserveReadForKeyThrough(&nopCoordinator{}, context.Background(), key)
}

func TestShardedCoordinatorObserveReadForKeyLeaderAndRoute(t *testing.T) {
	routes := distribution.NewEngine()
	routes.UpdateRoute([]byte("a"), []byte("m"), 1)
	routes.UpdateRoute([]byte("m"), nil, 2)
	leader := newStubLeaderEngine()
	follower := newStubLeaderEngine()
	follower.StateValue = raftengine.StateFollower
	groups := map[uint64]*ShardGroup{
		1: {Engine: leader, Store: store.NewMVCCStore()},
		2: {Engine: follower, Store: store.NewMVCCStore()},
	}
	rec := &recordingSampler{}
	c := NewShardedCoordinator(routes, groups, 1, NewHLC(), NewShardStore(routes, groups)).WithSampler(rec).WithKeyVizLabelsEnabled(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ObserveReadForKeyThrough(WithKeyVizLabel(c, keyviz.LabelRedis), ctx, []byte("b"))
	ObserveReadForKeyThrough(c, ctx, []byte("z"))
	require.Len(t, rec.snapshot(), 1)
	route, ok := routes.GetRoute([]byte("b"))
	require.True(t, ok)
	require.Equal(t, sampleCall{routeID: route.RouteID, op: keyviz.OpRead, label: keyviz.LabelRedis, key: []byte("b"), keyLen: 1}, rec.snapshot()[0])
}
