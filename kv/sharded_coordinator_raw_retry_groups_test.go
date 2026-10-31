package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestRawRetryDoesNotResendPartiallyAppliedGroupsAfterRouteChange(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "write conflict", err: store.NewWriteConflictError([]byte("z"))},
		{name: "version GC", err: ErrComposed1VersionGCd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := distribution.NewEngine()
			require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
				Version: 1,
				Routes: []distribution.RouteDescriptor{
					{RouteID: 1, End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
					{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
				},
			}))

			succeeded := &recordingTransactional{}
			conflicted := &recordingTransactional{
				errs: []error{tc.err},
				onCommit: func(_ int, _ *pb.Request) {
					require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
						Version: 2,
						Routes: []distribution.RouteDescriptor{
							{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive},
						},
					}))
				},
			}
			coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
				1: {Txn: succeeded},
				2: {Txn: conflicted},
			}, 1, NewHLC(), nil)
			_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("a"), Value: []byte("first")},
				{Op: Put, Key: []byte("z"), Value: []byte("second")},
			}})
			require.ErrorIs(t, err, tc.err)
			require.Len(t, succeeded.requests, 1, "a successful group must not be sent the same write again")
			require.Len(t, conflicted.requests, 1)
		})
	}
}

func TestRawRetryRequiresSingleAppliedRequestAfterRouteConvergence(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: &recordingTransactional{}},
		2: {Txn: &recordingTransactional{}},
	}, 1, NewHLC(), nil)
	reqs := &OperationGroup[OP]{Elems: []*Elem[OP]{
		{Op: Put, Key: []byte("a"), Value: []byte("first")},
		{Op: Put, Key: []byte("z"), Value: []byte("second")},
	}}
	logs, err := coord.requestLogs(context.Background(), reqs)
	require.NoError(t, err)
	require.Len(t, logs, 2)

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	grouped, err := coord.router.groupRequests(logs)
	require.NoError(t, err)
	require.Len(t, grouped, 1)
	require.Len(t, grouped[1], 2)
	shape := rawDispatchShape{groupCount: len(grouped), requestCount: len(logs)}
	require.False(t, coord.retryableRawFenceConflict(reqs, shape, store.NewWriteConflictError([]byte("z"))),
		"one request can apply while the other conflicts in the same destination group")
	require.False(t, shape.singleRequest(), "version-GC retry must obey the same partial-success guard")
}
