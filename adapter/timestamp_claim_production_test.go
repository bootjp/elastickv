package adapter

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This fixture tests cryptographic ownership and real wire propagation, not
// production TLS/registry provisioning or a distributed activation campaign.
type adapterPinnedClaimAuthority struct{ key [ed25519.PublicKeySize]byte }

func (p adapterPinnedClaimAuthority) AuthorizeTimestampClaimLookup(ctx context.Context) error {
	return ctx.Err()
}

func (p adapterPinnedClaimAuthority) AuthorizeTimestampClaim(ctx context.Context, key [ed25519.PublicKeySize]byte) error {
	if key != p.key {
		return kv.ErrTSOClaimAuthorityRequired
	}
	return ctx.Err()
}

type freshClaimDataGate struct{}

func (freshClaimDataGate) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return 0, nil
}
func (freshClaimDataGate) CheckPhaseDProtocol(context.Context) error   { return nil }
func (freshClaimDataGate) SealPhaseDCreation(context.Context) error    { return nil }
func (freshClaimDataGate) PreparePhaseD(context.Context, uint64) error { return nil }
func (freshClaimDataGate) FinishPhaseD(context.Context, uint64) error  { return nil }

func newNativeAdapterClaimAllocator(t *testing.T) *kv.RaftTSOAllocator {
	t.Helper()
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(2 * time.Minute).UnixMilli())
	state := kv.NewTSOStateMachine(clock)
	engine, _ := newSingleRaftForDistributionE2E(t, "native-claim-tso", state)
	identity, err := kv.NewTSOClaimIdentity(bytes.Repeat([]byte{41}, ed25519.SeedSize))
	require.NoError(t, err)
	allocator, err := kv.NewRaftTSOAllocator(&kv.ShardGroup{Engine: engine, TSOState: state}, clock,
		kv.WithTSOCutoverFloorProvider(freshClaimDataGate{}), kv.WithTSOPhaseDActivationGate(freshClaimDataGate{}),
		kv.WithRaftTSOClaimIdentity(identity), kv.WithTSOClaimAuthority(adapterPinnedClaimAuthority{key: identity.PublicKey()}))
	require.NoError(t, err)
	_, err = allocator.ReserveBatchAfter(t.Context(), 16, 0, true, true)
	require.NoError(t, err)
	return allocator
}

type forceClaimRPCAllocator struct{ *kv.RaftTSOAllocator }

func (forceClaimRPCAllocator) IsLeader() bool { return false }

type claimRPCLeader struct {
	forwardObserverLeader
	address string
}

type claimForwardFollower struct {
	raftengine.Engine
	address string
}

func (claimForwardFollower) State() raftengine.State { return raftengine.StateFollower }
func (f claimForwardFollower) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{Address: f.address}
}

func (l claimRPCLeader) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{Address: l.address}
}

func serveClaimRPC(t *testing.T, server *grpc.Server) (string, *grpc.ClientConn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	return listener.Addr().String(), connection
}

func TestTimestampClaimsProductionDistributionAndForward(t *testing.T) {
	allocator := newNativeAdapterClaimAllocator(t)
	var replyLost atomic.Bool
	var claimCalls atomic.Uint64
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		response, err := next(ctx, request)
		if info.FullMethod == pb.Distribution_GetTimestamp_FullMethodName {
			claimCalls.Add(1)
			if err == nil && replyLost.CompareAndSwap(false, true) {
				return nil, status.Error(codes.Unavailable, "lose applied claim reply")
			}
		}
		return response, err
	}))
	pb.RegisterDistributionServer(server, NewDistributionServer(distribution.NewEngine(), nil, WithDistributionTimestampAllocator(allocator)))
	address, connection := serveClaimRPC(t, server)
	routed, err := kv.NewLeaderRoutedTSOAllocator(forceClaimRPCAllocator{allocator}, claimRPCLeader{address: address})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, routed.Close()) })
	request := &pb.Request{TimestampClaimGroupId: 1, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("wire-key"), Value: []byte("wire-value")}}}
	require.NoError(t, kv.IssueRequestTimestampClaim(t.Context(), routed, request))
	require.GreaterOrEqual(t, claimCalls.Load(), uint64(2), "same signed attempt must reconcile an applied-but-lost reply")
	validation, err := pb.NewDistributionClient(connection).ValidateTimestamp(t.Context(), &pb.ValidateTimestampRequest{ClaimedRequest: request})
	require.NoError(t, err)
	require.Equal(t, kv.TSOClaimProtocolVersion, validation.ClaimProtocolVersion)
	st := store.NewMVCCStore()
	dataEngine, _ := newSingleRaftForDistributionE2E(t, "native-claim-data", kv.NewKvFSMWithHLC(st, kv.NewHLC()))
	manager := kv.NewTransactionWithProposer(dataEngine)
	t.Cleanup(manager.Close)
	dataServer := grpc.NewServer()
	receiverIdentity, err := kv.NewTSOClaimIdentity(bytes.Repeat([]byte{42}, ed25519.SeedSize))
	require.NoError(t, err)
	receiver, err := kv.NewBatchAllocator(routed, 16, kv.WithBatchTSOClaimIdentity(receiverIdentity))
	require.NoError(t, err)
	pb.RegisterInternalServer(dataServer, NewInternalWithEngine(manager, dataEngine, nil, nil,
		WithInternalTimestampAllocator(receiver), WithInternalStore(st), WithInternalTimestampClaimGroup(1)))
	dataAddress, dataConnection := serveClaimRPC(t, dataServer)
	client := pb.NewInternalClient(dataConnection)
	response, err := client.Forward(t.Context(), &pb.ForwardRequest{Requests: []*pb.Request{request}})
	require.NoError(t, err)
	require.Equal(t, kv.TSOClaimProtocolVersion, response.TimestampClaimProtocolVersion)
	stored, err := st.GetAt(t.Context(), []byte("wire-key"), request.Ts)
	require.NoError(t, err)
	require.Equal(t, []byte("wire-value"), stored)
	forged := proto.CloneOf(request)
	forged.Mutations[0].Key = []byte("foreign-key")
	_, err = client.Forward(t.Context(), &pb.ForwardRequest{Requests: []*pb.Request{forged}})
	require.Error(t, err)
	bare := proto.CloneOf(request)
	bare.TimestampClaim = nil
	_, err = client.Forward(t.Context(), &pb.ForwardRequest{Requests: []*pb.Request{bare}})
	require.Error(t, err, "receiver cannot rebind a forwarded attempt to its own signing identity")
	_, err = st.GetAt(t.Context(), []byte("foreign-key"), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	sourceEngine := claimForwardFollower{Engine: dataEngine, address: dataAddress}
	sourceManager := kv.NewTransactionWithProposer(sourceEngine)
	t.Cleanup(sourceManager.Close)
	source := kv.NewCoordinatorWithEngine(sourceManager, sourceEngine, kv.WithTSOAllocator(routed), kv.WithCoordinateTimestampClaimGroup(1))
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	operation := &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("origin-owned"), Value: []byte("retained")}}}
	_, err = source.Dispatch(t.Context(), operation)
	require.NoError(t, err)
	claim, err := kv.DecodeRequestTimestampClaim(operation.TimestampClaim)
	require.NoError(t, err)
	require.Equal(t, allocator.TimestampClaimIdentity().PublicKey(), claim.Signed.PublicKey,
		"Forward must preserve the origin's signer, not reissue under the receiving leader's different seed")
	value, err := st.GetAt(t.Context(), []byte("origin-owned"), operation.CommitTS)
	require.NoError(t, err)
	require.Equal(t, []byte("retained"), value)
}

type oldTimestampDecoder struct {
	pb.UnimplementedDistributionServer
	calls atomic.Uint64
}

func (s *oldTimestampDecoder) GetTimestamp(context.Context, *pb.GetTimestampRequest) (*pb.GetTimestampResponse, error) {
	s.calls.Add(1)
	return &pb.GetTimestampResponse{Timestamp: 123, Count: 1, CommittedByDedicatedTso: true, PhaseDActive: true}, nil
}

func TestTimestampClaimProtocolRejectsOldDecoder(t *testing.T) {
	allocator := newNativeAdapterClaimAllocator(t)
	server := grpc.NewServer()
	decoder := &oldTimestampDecoder{}
	pb.RegisterDistributionServer(server, decoder)
	address, _ := serveClaimRPC(t, server)
	routed, err := kv.NewLeaderRoutedTSOAllocator(forceClaimRPCAllocator{allocator}, claimRPCLeader{address: address})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, routed.Close()) })
	request := &pb.Request{TimestampClaimGroupId: 1, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value")}}}
	require.ErrorIs(t, kv.IssueRequestTimestampClaim(t.Context(), routed, request), kv.ErrTSOProtocolUnsupported)
	require.Zero(t, request.Ts, "an old decoder's scalar reservation is not a claim")
	require.Empty(t, request.TimestampClaim)
	require.Equal(t, uint64(1), decoder.calls.Load(), "decoder mismatch is definitive, not a reservation retry loop")
}
