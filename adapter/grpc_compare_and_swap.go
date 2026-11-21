package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (r *GRPCServer) CompareAndSwap(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.CompareAndSwapResponse, error) {
	if err := validateCompareAndSwapRequest(req); err != nil {
		return nil, err
	}
	resp, err := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn: true,
		Elems: []*kv.Elem[kv.OP]{{
			Op: kv.CompareAndSwap, Key: req.GetKey(), Value: req.GetValue(), ExpectedCommitTS: req.GetExpectedCommitTs(),
		}},
	})
	if err != nil {
		return compareAndSwapFailure(err)
	}
	if resp == nil || resp.CommitIndex == 0 || resp.CommitTS <= req.GetExpectedCommitTs() {
		return nil, projectPublicGRPCMutationError(kv.ErrTransactionOutcomeUnknown)
	}
	return &pb.CompareAndSwapResponse{Result: &pb.CompareAndSwapResponse_NewCommitTs{NewCommitTs: resp.CommitTS}}, nil
}

func compareAndSwapFailure(err error) (*pb.CompareAndSwapResponse, error) {
	if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && kv.IsCertifiedOnePhaseConflict(err) {
		switch {
		case errors.Is(err, kv.ErrCASMismatch):
			return &pb.CompareAndSwapResponse{Result: &pb.CompareAndSwapResponse_Mismatch{Mismatch: true}}, nil
		case errors.Is(err, kv.ErrRouteWriteFenced):
			return nil, publicMutationStatus(codes.Aborted, "Compare-and-swap route is write-fenced", "CAS_ROUTE_FENCED")
		case errors.Is(err, kv.ErrTxnLocked):
			return nil, publicMutationStatus(codes.Aborted, "Compare-and-swap key is locked", "CAS_LOCKED")
		}
	}
	return nil, projectPublicGRPCMutationError(err)
}

func validateCompareAndSwapRequest(req *pb.CompareAndSwapRequest) error {
	if len(req.GetKey()) == 0 || req.GetExpectedCommitTs() == 0 {
		return errors.WithStack(status.Error(codes.InvalidArgument, "key and positive expected commit timestamp are required"))
	}
	if !kv.IsCASUserKey(req.GetKey()) {
		return errors.WithStack(status.Error(codes.PermissionDenied, "compare-and-swap requires a public key"))
	}
	return nil
}
