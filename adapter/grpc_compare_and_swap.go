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
	if errors.Is(err, kv.ErrTxnLocked) && !errors.Is(err, kv.ErrTransactionOutcomeUnknown) && kv.IsCertifiedOnePhaseConflict(err) {
		return nil, publicMutationStatus(codes.Aborted, "Compare-and-swap key is locked", "CAS_LOCKED")
	}
	if kv.IsCertifiedCASMismatch(err) {
		return &pb.CompareAndSwapResponse{Result: &pb.CompareAndSwapResponse_Mismatch{Mismatch: true}}, nil
	}
	if err != nil {
		return nil, projectPublicGRPCMutationError(err)
	}
	if resp == nil || resp.CommitIndex == 0 || resp.CommitTS <= req.GetExpectedCommitTs() {
		return nil, projectPublicGRPCMutationError(kv.ErrTransactionOutcomeUnknown)
	}
	return &pb.CompareAndSwapResponse{Result: &pb.CompareAndSwapResponse_NewCommitTs{NewCommitTs: resp.CommitTS}}, nil
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
