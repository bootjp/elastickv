package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errMigrationNamespaceAdmissionNotConfigured = status.Error(codes.FailedPrecondition, "migration namespace admission is not configured")

func (i *Internal) verifyMigrationImportProposal(ctx context.Context) error {
	if i.migrationProposer == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "migration import proposer is not configured"))
	}
	if i.migrationImportGate == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "migration import opcode gate is not configured"))
	}
	if err := i.verifyInternalLeader(ctx); err != nil {
		return err
	}
	return i.verifyMigrationImportEnabled(ctx)
}

func (i *Internal) verifyMigrationPromoteProposal(ctx context.Context) error {
	if i.migrationProposer == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "migration promote proposer is not configured"))
	}
	if i.migrationPromoteGate == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "migration promote opcode gate is not configured"))
	}
	if err := i.verifyInternalLeader(ctx); err != nil {
		return err
	}
	return i.verifyMigrationPromoteEnabled(ctx)
}

func (i *Internal) requireMigrationNamespaceAdmission(ctx context.Context) error {
	if err := kv.RequireMigrationNamespaceAdmission(ctx, i.store); err != nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, err.Error()))
	}
	if !i.MigrationNamespaceAdmissionSupported() {
		return errors.WithStack(errMigrationNamespaceAdmissionNotConfigured)
	}
	return nil
}

func WithMigrationNamespaceAdmissionReady(ready func() bool) InternalOption {
	return func(i *Internal) { i.migrationNamespaceReady = ready }
}

// Installed support is safe to query before a candidate has any ConfState or
// marker. The callback must describe actual public/FSM module installation.
func (i *Internal) MigrationNamespaceAdmissionSupported() bool {
	return i != nil && i.store != nil && i.migrationProposer != nil && i.migrationNamespaceReady != nil && i.migrationNamespaceReady()
}

func (i *Internal) probeMigrationNamespace(ctx context.Context, _ *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	response := &pb.ProbeMigrationStateResponse{}
	if !i.MigrationNamespaceAdmissionSupported() {
		return response, nil
	}
	response.NamespaceAdmissionVersion = kv.MigrationNamespaceVersion
	reader, ok := i.leader.(raftengine.AppliedConfigurationReader)
	if !ok {
		return response, nil
	}
	index, supported := reader.AppliedConfigurationIndex()
	response.AppliedConfigurationIndex = index
	if !supported || index == 0 {
		return response, nil
	}
	version, err := kv.MigrationNamespaceAdmissionVersion(ctx, i.store)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	configuration, err := kv.MigrationConfigurationIndex(ctx, i.store)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	response.Ready = version == kv.MigrationNamespaceVersion && configuration == index
	return response, nil
}

// MODE8 is the sole exception to the marker-required migration mutation gate.
// Do not route it through cleanup/partition/Draft operational readiness gates:
// those controls require the very marker this control is installing.
func (i *Internal) applyMigrationNamespaceAdmission(ctx context.Context, request *pb.CleanupMigrationRequest) (*pb.CleanupMigrationResponse, error) {
	if !i.MigrationNamespaceAdmissionSupported() {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "namespace admission modules are not installed"))
	}
	if err := kv.ValidateNamespaceAdmissionRequest(request); err != nil {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, err.Error()))
	}
	if err := i.verifyInternalLeaderApplied(ctx); err != nil {
		return nil, err
	}
	configuration, err := kv.MigrationConfigurationIndex(ctx, i.store)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if configuration != request.GetExpectedConfigurationIndex() {
		return nil, errors.WithStack(status.Error(codes.Aborted, "namespace configuration changed"))
	}
	if err := i.proposeNamespaceAdmission(ctx, request); err != nil {
		return nil, err
	}
	if err := kv.RequireMigrationNamespaceAdmission(ctx, i.store); err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.CleanupMigrationResponse{Done: true}, nil
}

func (i *Internal) proposeNamespaceAdmission(ctx context.Context, request *pb.CleanupMigrationRequest) error {
	encoded, err := kv.MarshalMigrationCleanupCommand(request)
	if err != nil {
		return errors.WithStack(err)
	}
	response, err := i.proposeMigrationCommand(ctx, encoded, "namespace admission")
	if err != nil {
		return err
	}
	if verdict, ok := response.(error); ok {
		return errors.WithStack(verdict)
	}
	if response != nil {
		return errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	return nil
}
