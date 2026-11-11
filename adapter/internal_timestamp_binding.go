package adapter

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const timestampBindingPeerBudget = 5 * time.Second

func rejectUnactivatedBoundForward(requests []*pb.Request) error {
	for _, request := range requests {
		if len(request.GetTimestampBindingOperationId()) != 0 {
			return errors.WithStack(kv.ErrTimestampBindingInvalid)
		}
	}
	return nil
}

// WithInternalTimestampAdmission supplies TLS-and-bearer RPC credentials for
// tests and future rollout wiring. It does not authenticate a peer identity.
// Production leaves this option unset until identity and replica gates exist.
func WithInternalTimestampAdmission(token string, issuer kv.TimestampAfterAllocator) InternalOption {
	return func(i *Internal) {
		i.timestampAdmissionToken = token
		i.timestampAdmissionIssuer = issuer
	}
}

func (i *Internal) authorizeTimestampAdmission(ctx context.Context) error {
	if err := checkBearerToken(ctx, i.timestampAdmissionToken, "timestamp admission"); err != nil {
		return err
	}
	p, ok := peer.FromContext(ctx)
	if !ok || !splitMigrationTLSProtectedTransport(p.AuthInfo) {
		return errors.WithStack(status.Error(codes.Unauthenticated, "timestamp admission requires TLS"))
	}
	return nil
}

func (i *Internal) IssueBoundTimestamp(ctx context.Context, req *pb.IssueBoundTimestampRequest) (*pb.IssueBoundTimestampResponse, error) {
	if err := i.checkBoundTimestampIssuer(ctx, req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timestampBindingPeerBudget)
	defer cancel()
	if err := i.leader.VerifyLeader(ctx); err != nil {
		return nil, errors.Wrap(err, "verify group-0 issuer")
	}
	ts, err := i.timestampAdmissionIssuer.NextAfter(ctx, req.GetMinTimestamp())
	if err != nil {
		return nil, errors.Wrap(err, "issue bound timestamp")
	}
	if ts == 0 || ts <= req.GetMinTimestamp() || i.leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "TSO issuance lost its leader fence"))
	}
	return &pb.IssueBoundTimestampResponse{Timestamp: ts}, nil
}

func (i *Internal) checkBoundTimestampIssuer(ctx context.Context, req *pb.IssueBoundTimestampRequest) error {
	if err := i.authorizeTimestampAdmission(ctx); err != nil {
		return err
	}
	if req == nil || !i.kvForwardRejected || i.timestampAdmissionIssuer == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "dedicated TSO issuance is unavailable"))
	}
	if i.leader == nil || i.leader.State() != raftengine.StateLeader {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "TSO is not leader"))
	}
	return nil
}

func (i *Internal) LookupTimestampBinding(ctx context.Context, req *pb.LookupTimestampBindingRequest) (*pb.LookupTimestampBindingResponse, error) {
	metadata, operationID, err := i.homeBindingLookupInputs(ctx, req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timestampBindingPeerBudget)
	defer cancel()
	index, err := i.leader.LinearizableRead(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "read home-group binding barrier")
	}
	binding, found, err := kv.LookupTimestampBinding(ctx, metadata, operationID)
	if err != nil {
		return nil, errors.Wrap(err, "lookup home-group binding")
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	if i.leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "home-group leadership changed"))
	}
	response := &pb.LookupTimestampBindingResponse{Found: found, AppliedIndex: index}
	if found {
		response.Timestamp = binding.Timestamp
		response.ManifestDigest = binding.ManifestDigest[:]
		response.IsTxn = binding.IsTxn
		response.Phase = binding.Phase
	}
	return response, nil
}

func (i *Internal) homeBindingLookupInputs(ctx context.Context, req *pb.LookupTimestampBindingRequest) (store.RaftControlMetadataStore, [16]byte, error) {
	var operationID [16]byte
	if err := i.authorizeTimestampAdmission(ctx); err != nil {
		return nil, operationID, err
	}
	if req == nil || len(req.GetOperationId()) != len(operationID) || i.kvForwardRejected {
		return nil, operationID, errors.WithStack(status.Error(codes.InvalidArgument, "data-group operation ID is required"))
	}
	metadata, ok := i.store.(store.RaftControlMetadataStore)
	if !ok || i.leader == nil || i.leader.State() != raftengine.StateLeader {
		return nil, operationID, errors.WithStack(status.Error(codes.FailedPrecondition, "data-group binding lookup is unavailable"))
	}
	copy(operationID[:], req.GetOperationId())
	return metadata, operationID, nil
}
