package adapter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type twoGroupRPCProposer struct{ calls atomic.Int32 }

func (p *twoGroupRPCProposer) ProposeIfTerm(_ context.Context, term uint64, _ []byte) (*raftengine.ProposalResult, error) {
	p.calls.Add(1)
	if term != 1 {
		return nil, raftengine.ErrStaleAdmissionTerm
	}
	return &raftengine.ProposalResult{CommitIndex: 7}, nil
}

func twoGroupRPCManifest() *pb.TwoGroupPrepareManifest {
	prepare := func(key string) *pb.Request {
		return &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 10, Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{
				PrimaryKey: []byte("home-key"), CommitTS: 42, PrepareCommitTS: 42, LockTTLms: 1000,
			})},
			{Op: pb.Op_PUT, Key: []byte(key), Value: []byte("value")},
		}}
	}
	return &pb.TwoGroupPrepareManifest{Version: 1, OperationId: []byte{
		0x08, 0xc0, 0xcb, 0xe1, 0xb7, 0x61, 0x8d, 0x49,
		0x2d, 0x8f, 0x93, 0xff, 0x9c, 0x0b, 0x59, 0x12,
	},
		PersistenceTimestamp: 42, PrimaryGroupId: 1, Participants: []*pb.TwoGroupPrepareParticipant{
			{GroupId: 1, Prepare: prepare("home-key")}, {GroupId: 2, Prepare: prepare("secondary-key")},
		}}
}

func twoGroupRPCMethod(group uint64) peersession.Method {
	return peersession.Method{Name: pb.Internal_ProposeTwoGroupPrepare_FullMethodName,
		Request: func(v any) (peersession.Message, error) {
			if _, ok := v.(*pb.ProposeTwoGroupPrepareRequest); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			return peersession.Message{Group: group, From: 2, To: 1, Traffic: peersession.Control}, nil
		},
		Reply: func(req peersession.Message, v any) (peersession.Message, error) {
			if _, ok := v.(*pb.ProposeTwoGroupPrepareResponse); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			req.From, req.To = req.To, req.From
			return req, nil
		}}
}

func TestTwoGroupPrepareRPCRequiresPrivateSessionAndCanonicalManifest(t *testing.T) {
	proposer := &twoGroupRPCProposer{}
	service := NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil,
		WithInternalTwoGroupPrepareAdmission(1, "two-group-token", proposer, nil))
	serverOwner, clientOwner, conn, serverNode := newTimestampNativePeer(t, 1, twoGroupRPCMethod(1), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	manifest := twoGroupRPCManifest()
	request := &pb.ProposeTwoGroupPrepareRequest{Manifest: manifest}
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer two-group-token")
	_, err := pb.NewInternalClient(conn).ProposeTwoGroupPrepare(ctx, request)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Zero(t, proposer.calls.Load())
	handle, err := clientOwner.Handshake(ctx, conn, 1, serverNode)
	require.NoError(t, err)
	require.Equal(t, uint64(1), handle.GroupID())
	wrongRoute := TwoGroupPreparePeer{Handle: handle, Conn: conn, Token: "two-group-token"}
	require.ErrorIs(t, wrongRoute.PrepareTwoGroupOnLeader(ctx, 2, manifest), kv.ErrTwoGroupAdmission)
	require.Zero(t, proposer.calls.Load())
	response := new(pb.ProposeTwoGroupPrepareResponse)
	invoke := func(req *pb.ProposeTwoGroupPrepareRequest) error {
		return handle.Invoke(ctx, conn, pb.Internal_ProposeTwoGroupPrepare_FullMethodName, req, response,
			grpc.PerRPCCredentials(migrationNativeBearer{token: "two-group-token"}))
	}
	bad := twoGroupRPCManifest()
	bad.Participants[1].Prepare.Mutations[0].Value = []byte("wrong")
	require.Error(t, invoke(&pb.ProposeTwoGroupPrepareRequest{Manifest: bad}))
	require.Zero(t, proposer.calls.Load())
	require.NoError(t, invoke(request))
	require.Equal(t, uint64(7), response.GetCommitIndex())
	require.Equal(t, int32(1), proposer.calls.Load())
	serverOwner.RevokeGroup(1)
	require.Error(t, invoke(request))
	require.Equal(t, int32(1), proposer.calls.Load())
}

func TestTwoGroupPrepareRPCDisabledWithoutOptIn(t *testing.T) {
	client := boundTimestampClient(t, NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil))
	_, err := client.ProposeTwoGroupPrepare(boundTimestampContext(t), &pb.ProposeTwoGroupPrepareRequest{Manifest: twoGroupRPCManifest()})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}
