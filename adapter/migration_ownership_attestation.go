package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AttestMigrationOwnership is a privileged root-leader peer RPC. Unlike route
// read RPCs, its producer waits for the committed catalog's applied boundary.
func (s *DistributionServer) AttestMigrationOwnership(ctx context.Context, req *pb.AttestMigrationOwnershipRequest) (*pb.AttestMigrationOwnershipResponse, error) {
	if len(req.GetPriorPartition()) != 0 || len(req.GetPriorOwnershipState()) != 0 {
		return s.attestMigrationPartition(ctx, req)
	}
	if req.GetJobId() == 0 {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "ownership attestation requires job_id"))
	}
	if _, err := kv.MigrationOwnershipStateFromProto(req.GetJobId(), req.GetPrior()); err != nil {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "invalid prior ownership proof"))
	}
	if err := s.requireReadReady(); err != nil {
		return nil, err
	}
	proof, err := s.attestUnchangedOwnershipAtCommittedRoot(ctx, req.GetPrior())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.AttestMigrationOwnershipResponse{OwnershipProtocol: migrationOwnershipProtocol, Ownership: proof}, nil
}
