package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

func splitCleanupAckConfiguration(acks []splitVoterAck) uint64 {
	if len(acks) == 0 {
		return 0
	}
	index := acks[0].configurationIndex
	for _, ack := range acks {
		if !ack.acked || ack.configurationIndex != index {
			return 0
		}
	}
	return index
}

func splitCleanupCursorConfiguration(cursor []byte) (uint64, error) {
	acks, err := decodeSplitVoterAckCursor(reusableSplitCleanupAckCursor(cursor))
	if err != nil {
		return 0, err
	}
	index := splitCleanupAckConfiguration(acks)
	if index == 0 {
		return 0, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return index, nil
}

func (s *DistributionServer) bindSplitSourceCleanup(ctx context.Context, job distribution.SplitJob, source SplitMigrationClient, routeEnd []byte, req *pb.CleanupMigrationRequest) error {
	var index uint64
	var err error
	if job.Phase == distribution.SplitJobPhaseCleanup {
		index, err = s.splitCompletedSourceConfiguration(ctx, job)
	} else {
		index, err = s.splitAbandonedSourceConfiguration(ctx, job, source, routeEnd)
	}
	if err != nil {
		return err
	}
	req.RequireConfigurationIndex = true
	req.ExpectedConfigurationIndex = index
	return nil
}

func (s *DistributionServer) splitCompletedSourceConfiguration(ctx context.Context, job distribution.SplitJob) (uint64, error) {
	complete, err := s.refreshSplitJobSourceCleanupBarriers(ctx, job)
	if err != nil {
		return 0, err
	}
	if !complete {
		return 0, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	current, found, err := s.catalog.SplitJob(ctx, job.JobID)
	if err != nil || !found {
		return 0, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	return splitCleanupCursorConfiguration(current.SourceReadDrainCursor)
}

func (s *DistributionServer) splitAbandonedSourceConfiguration(ctx context.Context, job distribution.SplitJob, source SplitMigrationClient, routeEnd []byte) (uint64, error) {
	groupID, err := s.splitJobSourceGroupID(ctx, job)
	if err != nil {
		return 0, err
	}
	// A repeated clear may already have retired this job. Both proofs require
	// an upgraded all-voter configuration attestation; zero never authorizes it.
	for _, kind := range []pb.MigrationStateProbeKind{
		pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_GUARD_PRESENT,
		pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED,
	} {
		cursor, complete, err := s.syncSplitMigrationVoterBarrier(ctx, groupID, nil, source, &pb.ProbeMigrationStateRequest{
			JobId: job.JobID, Kind: kind, RouteStart: job.SplitKey, RouteEnd: routeEnd, RequireConfigurationIndex: true,
		})
		if err != nil {
			return 0, err
		}
		if complete {
			return splitCleanupCursorConfiguration(cursor)
		}
	}
	return 0, errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
}
