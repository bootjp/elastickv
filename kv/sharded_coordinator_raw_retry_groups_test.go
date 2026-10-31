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
