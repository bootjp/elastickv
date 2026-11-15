package adapter

import (
	"context"
	"slices"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LegacyMetadataPassSource returns only a complete and currently valid
// four-plan diagnostic set. It is not a Phase-3 gate authority.
type LegacyMetadataPassSource interface {
	CompleteLegacyMetadataGroupPasses(context.Context, uint64) ([]LegacyMetadataPassSnapshot, error)
}

func (s *AdminServer) RegisterLegacyMetadataPassSource(source LegacyMetadataPassSource) {
	s.groupsMu.Lock()
	s.legacyMetadataSource = source
	s.groupsMu.Unlock()
}

func (s *AdminServer) GetRedisLegacyMetadataPasses(ctx context.Context,
	req *pb.GetRedisLegacyMetadataPassesRequest) (*pb.GetRedisLegacyMetadataPassesResponse, error) {
	if req == nil || req.GetGroupId() == 0 || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "a data-group ID is required"))
	}
	s.groupsMu.RLock()
	source := s.legacyMetadataSource
	s.groupsMu.RUnlock()
	if source == nil {
		return nil, errors.WithStack(status.Error(codes.Unavailable, "legacy metadata pass source is unavailable"))
	}
	passes, err := source.CompleteLegacyMetadataGroupPasses(ctx, req.GetGroupId())
	if err != nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "complete current legacy metadata pass set unavailable"))
	}
	if err := validateLegacyMetadataRPCPasses(req.GetGroupId(), passes); err != nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "complete current legacy metadata pass set unavailable"))
	}
	return legacyMetadataRPCResponse(req.GetGroupId(), passes), nil
}

func validateLegacyMetadataRPCPasses(groupID uint64, passes []LegacyMetadataPassSnapshot) error {
	if len(passes) != len(legacyMetadataPlanNames) {
		return errors.WithStack(ErrLegacyMetadataPassIncomplete)
	}
	seen := make(map[string]bool, len(passes))
	for _, pass := range passes {
		if !validLegacyMetadataPassHeader(pass, groupID) || seen[pass.Plan] ||
			pass.CatalogVersion != passes[0].CatalogVersion || pass.LeaderTerm != passes[0].LeaderTerm {
			return errors.WithStack(ErrLegacyMetadataPassIncomplete)
		}
		seen[pass.Plan] = true
	}
	return nil
}

func legacyMetadataRPCResponse(groupID uint64, passes []LegacyMetadataPassSnapshot) *pb.GetRedisLegacyMetadataPassesResponse {
	response := &pb.GetRedisLegacyMetadataPassesResponse{GroupId: groupID,
		CatalogVersion: passes[0].CatalogVersion, LeaderTerm: passes[0].LeaderTerm}
	for _, pass := range passes {
		entry := &pb.RedisLegacyMetadataPass{GroupId: groupID, Plan: pass.Plan,
			CatalogVersion: pass.CatalogVersion, LeaderTerm: pass.LeaderTerm,
			StartedAtUnixNanos: pass.StartedAt.UnixNano(), CompletionEpochUnixNanos: pass.CompletionEpoch.UnixNano()}
		owners := make([]uint64, 0, len(pass.Counts))
		for owner := range pass.Counts {
			owners = append(owners, owner)
		}
		slices.Sort(owners)
		for _, owner := range owners {
			counts := pass.Counts[owner]
			entry.Counts = append(entry.Counts, &pb.RedisLegacyMetadataReasonCounts{OwnerGroupId: owner,
				Served: counts.Served, OwnerHasVersion: counts.OwnerHasVersion,
				OwnerLedElsewhere: counts.OwnerLedElsewhere, StagedVisibility: counts.StagedVisibility})
		}
		response.Passes = append(response.Passes, entry)
	}
	return response
}
