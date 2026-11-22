package adapter

import (
	"math"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCompareAndSwapCatalogChangeAfterForwardAdmission(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner uint64
		state distribution.RouteState
	}{
		{name: "fenced", owner: casRPCGroup, state: distribution.RouteStateWriteFenced},
		{name: "moved", owner: casRPCGroup + 1, state: distribution.RouteStateActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCASRPCFixture(t)
			key := []byte("row-on-right")
			version := f.put(t, key, []byte("before"))
			req := &pb.CompareAndSwapRequest{Key: key, ExpectedCommitTs: version, Value: []byte("after")}
			f.proposals.park.Store(true)
			done := make(chan error, 1)
			go func() {
				_, err := f.follower.CompareAndSwap(f.ctx, req)
				done <- err
			}()
			select {
			case <-f.proposals.parked:
			case err := <-done:
				t.Fatalf("forwarded CAS returned before proposing: %v", err)
			case <-f.ctx.Done():
				t.Fatal(f.ctx.Err())
			}

			muts := []*store.KVPairMutation{
				{Op: store.OpTypePut, Key: distribution.CatalogVersionKey(), Value: distribution.EncodeCatalogVersion(2)},
			}
			for _, route := range []distribution.RouteDescriptor{
				{RouteID: 1, End: []byte("m"), GroupID: casRPCGroup, State: distribution.RouteStateActive},
				{RouteID: 2, Start: []byte("m"), GroupID: tc.owner, State: tc.state},
			} {
				raw, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
				require.NoError(t, err)
				muts = append(muts, &store.KVPairMutation{
					Op: store.OpTypePut, Key: distribution.CatalogRouteKey(route.RouteID), Value: raw,
				})
			}
			replicateCASCatalogMutations(t, f.ctx, f.proposals, f.local.LastCommitTS(), muts)
			f.proposals.park.Store(false)
			close(f.proposals.release)

			err := <-done
			st := status.Convert(err)
			require.Equal(t, codes.Aborted, st.Code(), "the indexed fence was definitive: %v", err)
			require.NotEmpty(t, st.Details())
			detail, ok := st.Details()[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			require.Equal(t, "CAS_ROUTE_FENCED", detail.GetReason())

			// The front end still resolves the old owner. Its next forward
			// must be rejected by the receiver before any Raft proposal.
			for _, client := range []pb.TransactionalKVClient{f.leader, f.follower} {
				_, err = client.CompareAndSwap(f.ctx, req)
				st := status.Convert(err)
				require.Equal(t, codes.FailedPrecondition, st.Code(), "%v", err)
				require.Len(t, st.Details(), 1)
				info, ok := st.Details()[0].(*errdetails.ErrorInfo)
				require.True(t, ok)
				require.Equal(t, publicGRPCNotAdmittedReason, info.GetReason())
			}
			require.EqualValues(t, 2, f.forwards.Load())
			require.EqualValues(t, 1, f.proposals.attempts.Load())
			row, err := store.GetVersionAt(f.ctx, f.local, key, math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, store.VersionedValue{TS: version, Value: []byte("before")}, row)
		})
	}
}
