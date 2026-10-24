package main

import (
	"context"
	"strings"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const migrationMemberProbeTimeout = 2 * time.Second

func newMigrationMemberProtocolProbe(token string) func(context.Context, string) error {
	return func(ctx context.Context, address string) error {
		if strings.TrimSpace(address) == "" {
			return errors.WithStack(status.Error(codes.FailedPrecondition, "migration member address is empty"))
		}
		conn, err := grpc.NewClient(address, internalutil.GRPCDialOptions()...)
		if err != nil {
			return errors.Wrap(err, "migration member capability connection")
		}
		defer func() { _ = conn.Close() }()
		return checkMigrationMemberProtocols(ctx, newSplitMigrationInternalClient(conn, token))
	}
}

func checkMigrationMemberProtocols(ctx context.Context, client pb.InternalClient) error {
	attempt, cancel := context.WithTimeout(ctx, migrationMemberProbeTimeout)
	defer cancel()
	// Capability queries require a nonzero syntactic job ID but do not read or
	// mutate that job. A joining learner need not have an applied epoch yet.
	response, err := client.ProbeMigrationState(attempt, &pb.ProbeMigrationStateRequest{
		JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY,
	})
	if err != nil {
		return errors.Wrap(err, "migration member capability probe")
	}
	return validateMigrationMemberProtocols(response.GetTransferSizeGuardVersion(), response.GetOwnershipPartitionProtocol())
}

func validateMigrationMemberProtocols(transferVersion, partitionVersion uint32) error {
	const requiredTransferVersion, requiredPartitionVersion = 1, 1
	if transferVersion != requiredTransferVersion || partitionVersion != requiredPartitionVersion {
		return errors.WithStack(status.Errorf(codes.FailedPrecondition,
			"migration member lacks required protocols: transfer-size=%d ownership-partition=%d", transferVersion, partitionVersion))
	}
	return nil
}
