package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validatePartitionCleanupRequest(req *pb.CleanupMigrationRequest) error {
	if err := kv.ValidateMigrationPartitionRequest(req); err != nil {
		return errors.WithStack(status.Error(codes.InvalidArgument, "invalid partition control"))
	}
	return nil
}

func (i *Internal) validateMigrationCleanupAdmission(req *pb.CleanupMigrationRequest) error {
	if err := validateCleanupMigrationRequest(req); err != nil {
		return errors.WithStack(err)
	}
	if ownershipCleanupMode(req.GetMode()) && !i.ownershipAdmissionReady() {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "migration ownership data admission is not wired"))
	}
	if req.GetMode() == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_PARTITION && i.implementedPartitionProtocol() == 0 {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "partition control admission is not installed"))
	}
	if i.migrationProposer == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "migration cleanup proposer is not configured"))
	}
	return nil
}

func (i *Internal) probeMigrationPartition(ctx context.Context, req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	resp := &pb.ProbeMigrationStateResponse{OwnershipPartitionProtocol: i.implementedPartitionProtocol()}
	if !i.partitionProbeAdmissionReady(req) {
		return resp, nil
	}
	var transition store.MigrationOwnershipPartitionTransition
	if err := kv.DecodeMigrationPartitionJSON(req.GetPartitionTransition(), &transition); err != nil {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "invalid partition probe"))
	}
	index, ready, err := i.partitionProbeEpoch(ctx, req, transition)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !ready {
		return resp, nil
	}
	reader, ok := i.store.(store.MigrationOwnershipPartitionStore)
	if !ok {
		return resp, nil
	}
	state, err := reader.MigrationOwnershipPartitions(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	resp.OwnershipGeneration, resp.OwnershipProtocol, resp.AppliedConfigurationIndex = state.Generation, migrationOwnershipProtocol, index
	if state.Generation != transition.Generation {
		return resp, nil
	}
	id, err := store.MigrationOwnershipPartitionReceiptIDFor(transition)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if migrationPartitionReceiptPresent(state, id) {
		resp.PartitionReceiptId, resp.Ready = bytes.Clone(id), true
	}
	return resp, nil
}

func (i *Internal) partitionProbeAdmissionReady(req *pb.ProbeMigrationStateRequest) bool {
	return i.implementedPartitionProtocol() != 0 && i.ownershipAdmissionReady() && req.GetRequireConfigurationIndex() && req.GetExpectedConfigurationIndex() != 0
}

func (i *Internal) partitionProbeEpoch(ctx context.Context, req *pb.ProbeMigrationStateRequest, t store.MigrationOwnershipPartitionTransition) (uint64, bool, error) {
	engine, ok := i.leader.(raftengine.AppliedConfigurationReader)
	if !ok {
		return 0, false, nil
	}
	index, supported := engine.AppliedConfigurationIndex()
	if !supported || index == 0 || index != req.GetExpectedConfigurationIndex() || index != t.ConfigurationIndex {
		return 0, false, nil
	}
	durable, err := kv.MigrationConfigurationIndex(ctx, i.store)
	return index, durable == index, errors.WithStack(err)
}

func migrationPartitionReceiptPresent(state store.MigrationOwnershipPartitionState, id []byte) bool {
	for _, receipt := range state.Receipts {
		if !bytes.Equal(receipt.ReceiptID, id) || receipt.AppliedIndex == 0 {
			continue
		}
		actual, hashErr := store.MigrationOwnershipPartitionReceiptIDFor(receipt.Transition)
		if hashErr != nil || !bytes.Equal(actual, id) {
			continue
		}
		return true
	}
	return false
}
