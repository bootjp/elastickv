package adapter

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBoundTimestampPeerIssuerRejectsOldAndMalformedResponses(t *testing.T) {
	request := &pb.IssueBoundTimestampRequest{MinTimestamp: 10, PhysicalBandProtocolVersion: kv.TSOPhysicalBandProtocolVersion}
	response := &pb.IssueBoundTimestampResponse{Timestamp: 11, CommittedBandEnd: 0xffff}
	require.ErrorIs(t, validateBoundTimestampResponse(request, response), kv.ErrTSOProtocolUnsupported)
	response.PhysicalBandProtocolVersion = kv.TSOPhysicalBandProtocolVersion
	require.NoError(t, validateBoundTimestampResponse(request, response))
	response.PreviousIssuedCursor = 11
	require.ErrorIs(t, validateBoundTimestampResponse(request, response), kv.ErrTSOProtocolUnsupported)
	response.PreviousIssuedCursor = 0
	response.CommittedBandEnd = 12
	require.ErrorIs(t, validateBoundTimestampResponse(request, response), kv.ErrTSOProtocolUnsupported)
}

func TestAuthenticatedIssuerToHomeRawBindingAndRetry(t *testing.T) {
	issuer := &boundTimestampIssuer{}
	groupZero := NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil,
		WithKVForwardRejected(), WithInternalTimestampAdmission("timestamp-admission-token", issuer))
	_, clientOwner, conn, serverNode := newTimestampNativePeer(t, 0, boundTimestampPeerMethod(0), groupZero)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handle, err := clientOwner.Handshake(ctx, conn, 0, serverNode)
	require.NoError(t, err)
	peerIssuer := BoundTimestampPeerIssuer{Handle: handle, Conn: conn, Token: "timestamp-admission-token"}
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	leader := &prepareSafetyRPCLeader{}
	request := &pb.Request{TimestampBindingOperationId: append([]byte{1}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("home-key"), Value: []byte("home-value")}}}
	issue := func(ctx context.Context) (uint64, error) { return peerIssuer.NextAfter(ctx, 0) }
	prepared, err := kv.PrepareHomeRawTimestampBinding(ctx, leader, metadata, issue, request)
	require.NoError(t, err)
	require.NotZero(t, prepared.GetTs())
	require.Zero(t, request.GetTs())
	require.Equal(t, int32(1), issuer.calls.Load())
	unresolved, err := kv.PrepareHomeRawTimestampBinding(ctx, leader, metadata, issue, request)
	require.Nil(t, unresolved)
	require.ErrorIs(t, err, kv.ErrTimestampBindingPending)
	require.Equal(t, int32(1), issuer.calls.Load())
	manifest := proto.CloneOf(prepared)
	manifest.TimestampBindingOperationId = nil
	manifest.TimestampAdmissionFloor = 0
	manifest.TimestampBindingAdmissionTerm = 0
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(manifest)
	require.NoError(t, err)
	binding := kv.TimestampBinding{Timestamp: prepared.GetTs(), OperationID: [16]byte{1}, ManifestDigest: sha256.Sum256(encoded)}
	_, err = kv.ApplyTimestampBoundCreation(ctx, st, 1, binding, func(entryCtx context.Context) error {
		return st.ApplyMutationsRaftAt(entryCtx, []*store.KVPairMutation{{Op: store.OpTypePut, Key: []byte("home-key"), Value: []byte("home-value")}},
			nil, prepared.GetTs(), prepared.GetTs(), 1)
	})
	require.NoError(t, err)
	value, err := st.GetAt(ctx, []byte("home-key"), prepared.GetTs())
	require.NoError(t, err)
	require.Equal(t, []byte("home-value"), value)
	retry, err := kv.PrepareHomeRawTimestampBinding(ctx, leader, metadata, issue, request)
	require.NoError(t, err)
	require.Equal(t, prepared, retry)
	require.Equal(t, int32(1), issuer.calls.Load(), "durable home binding resolves retry without new issuance")
	changed := proto.CloneOf(request)
	changed.Mutations[0].Value = []byte("different")
	_, err = kv.PrepareHomeRawTimestampBinding(ctx, leader, metadata, issue, changed)
	require.ErrorIs(t, err, kv.ErrTimestampBindingConflict)
}
