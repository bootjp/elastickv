package adapter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func twoGroupEvidenceRPCMethod(group uint64) peersession.Method {
	return peersession.Method{Name: pb.Internal_ProbeTwoGroupPrepareEvidence_FullMethodName,
		Request: func(v any) (peersession.Message, error) {
			if _, ok := v.(*pb.ProbeTwoGroupPrepareEvidenceRequest); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			return peersession.Message{Group: group, From: 2, To: 1, Traffic: peersession.Control}, nil
		},
		Reply: func(req peersession.Message, v any) (peersession.Message, error) {
			if _, ok := v.(*pb.ProbeTwoGroupPrepareEvidenceResponse); !ok {
				return peersession.Message{}, peersession.ErrFormat
			}
			req.From, req.To = req.To, req.From
			return req, nil
		}}
}

func TestTwoGroupEvidenceProbePrivateGRPC(t *testing.T) {
	st := store.NewMVCCStore()
	leader := &prepareSafetyRPCLeader{}
	service := NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalStore(st), WithInternalTwoGroupPrepareAdmission(2, "two-group-token", &twoGroupRPCProposer{}, nil),
		WithInternalTwoGroupEvidenceProbe())
	serverOwner, clientOwner, conn, serverNode := newTimestampNativePeer(t, 2, twoGroupEvidenceRPCMethod(2), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	direct := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer two-group-token")
	_, err := pb.NewInternalClient(conn).ProbeTwoGroupPrepareEvidence(direct, &pb.ProbeTwoGroupPrepareEvidenceRequest{Version: 1})
	require.Error(t, err, "mTLS and bearer without a peer session cannot attest the marker")
	require.Zero(t, leader.reads.Load())
	handle, err := clientOwner.Handshake(ctx, conn, 2, serverNode)
	require.NoError(t, err)
	peer := TwoGroupEvidencePeer{Handle: handle, Conn: conn, Token: "two-group-token"}
	require.Error(t, peer.RequireTwoGroupPrepareEvidence(ctx, 2), "disabled marker must fail closed")
	require.Error(t, peer.RequireTwoGroupPrepareEvidence(ctx, 1), "wrong group must not route")
	forged := metadata.AppendToOutgoingContext(direct, "x-elastickv-peer-session", strings.Repeat("0", 64),
		"x-elastickv-peer-proof", strings.Repeat("0", 64))
	_, err = pb.NewInternalClient(conn).ProbeTwoGroupPrepareEvidence(forged, &pb.ProbeTwoGroupPrepareEvidenceRequest{Version: 1})
	require.Error(t, err, "forged session must not attest the marker")
	require.NoError(t, st.PutAt(t.Context(), []byte(kv.TxnMetaPrefix+"prepare_evidence/enabled"), []byte{1}, 1, 0))
	require.NoError(t, peer.RequireTwoGroupPrepareEvidence(ctx, 2))
	response := new(pb.ProbeTwoGroupPrepareEvidenceResponse)
	bad := &pb.ProbeTwoGroupPrepareEvidenceRequest{Version: 1}
	bad.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
	require.Error(t, handle.Invoke(ctx, conn, pb.Internal_ProbeTwoGroupPrepareEvidence_FullMethodName,
		bad, response, grpc.PerRPCCredentials(migrationNativeBearer{token: "two-group-token"})))
	serverOwner.RevokeGroup(2)
	require.Error(t, peer.RequireTwoGroupPrepareEvidence(ctx, 2))
}

func TestTwoGroupEvidenceProbeRejectsLeaderTransfer(t *testing.T) {
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(t.Context(), []byte(kv.TxnMetaPrefix+"prepare_evidence/enabled"), []byte{1}, 1, 0))
	leader := &prepareSafetyRPCLeader{advanceTermOnRead: true}
	service := NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalStore(st), WithInternalTwoGroupPrepareAdmission(2, "two-group-token", &twoGroupRPCProposer{}, nil),
		WithInternalTwoGroupEvidenceProbe())
	_, clientOwner, conn, serverNode := newTimestampNativePeer(t, 2, twoGroupEvidenceRPCMethod(2), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handle, err := clientOwner.Handshake(ctx, conn, 2, serverNode)
	require.NoError(t, err)
	peer := TwoGroupEvidencePeer{Handle: handle, Conn: conn, Token: "two-group-token"}
	require.Error(t, peer.RequireTwoGroupPrepareEvidence(ctx, 2))
}

func TestTwoGroupEvidenceProbeDisabledWithoutOptIn(t *testing.T) {
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(t.Context(), []byte(kv.TxnMetaPrefix+"prepare_evidence/enabled"), []byte{1}, 1, 0))
	service := NewInternalWithEngine(nil, &prepareSafetyRPCLeader{}, nil, nil,
		WithInternalStore(st), WithInternalTwoGroupPrepareAdmission(2, "two-group-token", &twoGroupRPCProposer{}, nil))
	_, clientOwner, conn, serverNode := newTimestampNativePeer(t, 2, twoGroupEvidenceRPCMethod(2), service)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handle, err := clientOwner.Handshake(ctx, conn, 2, serverNode)
	require.NoError(t, err)
	peer := TwoGroupEvidencePeer{Handle: handle, Conn: conn, Token: "two-group-token"}
	require.Error(t, peer.RequireTwoGroupPrepareEvidence(ctx, 2))
}
