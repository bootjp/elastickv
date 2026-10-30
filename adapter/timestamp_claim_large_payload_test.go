package adapter

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Deliberately sequential, one value per case. The default 64 MiB transport
// and Raft limits are unchanged; both TSO and data groups use native Raft.
func TestTimestampClaimLargeNativeForward(t *testing.T) {
	for _, size := range []struct {
		name  string
		bytes int
	}{{"22MiB", 22 << 20}, {"33MiB", 33 << 20}, {"63.5MiB", 127 << 19}} {
		t.Run(size.name, func(t *testing.T) {
			defer runtime.GC()
			allocator := newNativeAdapterClaimAllocator(t)
			st, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			engine, stop := newSingleRaftForDistributionE2E(t, "large-claim-data", kv.NewKvFSMWithHLC(st, kv.NewHLC()))
			manager := kv.NewTransactionWithProposer(engine)
			t.Cleanup(manager.Close)
			t.Cleanup(stop)
			server := grpc.NewServer(internalutil.GRPCServerOptions()...)
			pb.RegisterInternalServer(server, NewInternalWithEngine(manager, engine, nil, nil,
				WithInternalTimestampAllocator(allocator), WithInternalStore(st), WithInternalTimestampClaimGroup(1)))
			address, unused := serveClaimRPC(t, server)
			require.NoError(t, unused.Close())
			connection, err := grpc.NewClient(address, internalutil.GRPCDialOptions()...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })
			value := bytes.Repeat([]byte{'v'}, size.bytes)
			request := &pb.Request{TimestampClaimGroupId: 1, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("large-raw"), Value: value}}}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			require.NoError(t, kv.IssueRequestTimestampClaim(ctx, allocator, request))
			forward := &pb.ForwardRequest{Requests: []*pb.Request{request}}
			t.Logf("value=%d intent=%d request=%d forward=%d cap=%d", len(value), proto.Size(request.TimestampIntent), proto.Size(request), proto.Size(forward), internalutil.GRPCMaxMessageBytes)
			require.Less(t, proto.Size(request.TimestampIntent), 1024, "manifest size does not grow with value bytes")
			response, err := pb.NewInternalClient(connection).Forward(ctx, forward)
			require.NoError(t, err, "actual adapter admission, native Raft marshal/commit/apply must accept the original legal value")
			require.True(t, response.Success)
			require.Equal(t, kv.TSOClaimProtocolVersion, response.TimestampClaimProtocolVersion)
			require.Positive(t, response.CommitIndex)
			got, err := st.GetAt(ctx, []byte("large-raw"), request.Ts)
			require.NoError(t, err)
			require.True(t, bytes.Equal(value, got), "readback must retain every byte, not just its digest")
		})
	}
}
