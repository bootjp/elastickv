package adapter

import (
	"context"
	"io"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fenceAdmissionTransaction struct{ calls int }

func (x *fenceAdmissionTransaction) Commit(context.Context, []*pb.Request) (*kv.TransactionResponse, error) {
	x.calls++
	return &kv.TransactionResponse{CommitIndex: 1}, nil
}
func (x *fenceAdmissionTransaction) Abort(context.Context, []*pb.Request) (*kv.TransactionResponse, error) {
	panic("unexpected abort")
}

func TestRawPutKnownPreDispatchFenceIsNotIndeterminate(t *testing.T) {
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateWriteFenced}}}))
	txn := &fenceAdmissionTransaction{}
	coord := kv.NewShardedCoordinator(engine, map[uint64]*kv.ShardGroup{1: {Txn: txn}}, 1, kv.NewHLC(), nil)
	server := NewGRPCServer(store.NewMVCCStore(), coord)
	response, err := server.RawPut(t.Context(), &pb.RawPutRequest{Key: []byte("z/client"), Value: []byte("value")})
	require.False(t, response.GetSuccess())
	require.Zero(t, txn.calls, "no write reached any proposal path")
	require.Equal(t, codes.Unavailable, status.Code(err), "known pre-dispatch rejection must not be reported as indeterminate")
	detail, ok := status.Convert(err).Details()[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, publicGRPCNotAdmittedReason, detail.Reason)
	require.Empty(t, detail.Metadata)
	require.NotContains(t, err.Error(), "z/client")
	deleted, err := server.RawDelete(t.Context(), &pb.RawDeleteRequest{Key: []byte("z/client")})
	require.False(t, deleted.GetSuccess())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, txn.calls)
	_, proof := coord.Dispatch(t.Context(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("z/client"), Value: []byte("v")}}})
	for _, mixed := range []error{errors.Join(proof, io.EOF), errors.Join(proof, kv.ErrTransactionOutcomeUnknown), errors.CombineErrors(proof, io.EOF)} {
		service := NewGRPCServer(store.NewMVCCStore(), &availabilityErrorCoordinator{err: mixed})
		_, err = service.RawPut(t.Context(), &pb.RawPutRequest{Key: []byte("z/client"), Value: []byte("v")})
		require.Equal(t, codes.Unknown, status.Code(err), "ambiguous evidence cannot be hidden")
	}
}

func TestForwardRawPreSubmissionDecisionRequiresExactScope(t *testing.T) {
	raw := &pb.Request{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("z/private"), Value: []byte("v")}}}
	for _, tc := range []struct {
		name   string
		reqs   []*pb.Request
		cause  error
		commit bool
		want   pb.ForwardDecisionClass
	}{
		{"pre-submit cutover", []*pb.Request{raw}, kv.ErrRouteCutoverPending, false, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_REJECTED_BEFORE_SUBMISSION},
		{"pre-submit fence", []*pb.Request{raw}, kv.ErrRouteWriteFenced, false, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_REJECTED_BEFORE_SUBMISSION},
		{"unproven post-dispatch fence", []*pb.Request{raw}, kv.ErrRouteWriteFenced, true, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN},
		{"mixed pre-submit", []*pb.Request{raw}, errors.Join(kv.ErrRouteCutoverPending, io.EOF), false, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN},
		{"lost reply", []*pb.Request{raw}, io.EOF, true, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN},
		{"transaction", []*pb.Request{{IsTxn: true}}, kv.ErrRouteCutoverPending, false, pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := forwardStatusForReceiver(&pb.ForwardRequest{Requests: tc.reqs, DecisionDetailVersion: kv.ForwardDecisionSchemaVersion}, tc.cause, tc.commit)
			_, decision, valid := kv.DecodeForwardDecisionStatus(wire, pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY)
			require.True(t, valid)
			require.Equal(t, tc.want, decision)
			require.NotContains(t, wire.Error(), "z/private")
		})
	}
}
