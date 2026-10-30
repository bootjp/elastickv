package kv

import (
	"context"
	"sort"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// An observation is NOT activation authority. The control-manifest owner must
// bind these observations to authenticated peer identities, persist the epoch,
// seal creation, and enforce the new-member decoder admission fence.
type TSOClaimDecoderObservation struct{ Groups []TSOClaimDecoderGroup }
type TSOClaimDecoderGroup struct {
	GroupID            uint64
	Term               uint64
	ConfigurationIndex uint64
	Members            []raftengine.Server
}

func (s *ShardStore) ObserveTimestampClaimDecoders(ctx context.Context) (TSOClaimDecoderObservation, error) {
	var observation TSOClaimDecoderObservation
	if err := s.requireLocalPrepareEvidenceCutover(); err != nil {
		return observation, err
	}
	ids := make([]uint64, 0, len(s.groups))
	for id := range s.groups {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		group, err := s.observeTimestampClaimGroup(ctx, id)
		if err != nil {
			return TSOClaimDecoderObservation{}, err
		}
		observation.Groups = append(observation.Groups, group)
	}
	for _, group := range observation.Groups {
		status := engineForGroup(s.groups[group.GroupID]).Status()
		if status.Term != group.Term || status.ConfigurationIndex != group.ConfigurationIndex || status.PendingConfChange {
			return TSOClaimDecoderObservation{}, errors.WithStack(ErrTSOPhaseDActivationPending)
		}
	}
	return observation, nil
}

func (s *ShardStore) observeTimestampClaimGroup(ctx context.Context, id uint64) (TSOClaimDecoderGroup, error) {
	var observation TSOClaimDecoderGroup
	engine := engineForGroup(s.groups[id])
	if engine == nil {
		return observation, errors.WithStack(ErrTSOProtocolUnsupported)
	}
	before := engine.Status()
	if err := s.checkLocalTimestampClaimDecoder(id, before); err != nil {
		return observation, err
	}
	configuration, err := engine.Configuration(ctx)
	if err != nil {
		return observation, errors.WithStack(err)
	}
	if len(configuration.Servers) == 0 {
		return observation, errors.WithStack(ErrTSOProtocolUnsupported)
	}
	eg, probeCtx := errgroup.WithContext(ctx)
	eg.SetLimit(maxTSOCommitFloorConcurrency)
	for _, member := range configuration.Servers {
		if before.State == raftengine.StateLeader && member.ID == before.Leader.ID {
			continue
		}
		eg.Go(func() error { return s.CheckTimestampClaimMember(probeCtx, id, member.Address) })
	}
	if err := eg.Wait(); err != nil {
		return observation, errors.WithStack(err)
	}
	if err := prepareEvidenceMembershipStable(before, engine.Status()); err != nil {
		return observation, err
	}
	return TSOClaimDecoderGroup{GroupID: id, Term: before.Term, ConfigurationIndex: before.ConfigurationIndex, Members: configuration.Servers}, nil
}

func (s *ShardStore) checkLocalTimestampClaimDecoder(id uint64, status raftengine.Status) error {
	if status.PendingConfChange {
		return errors.WithStack(ErrTSOPhaseDActivationPending)
	}
	if id != 0 && status.State == raftengine.StateLeader {
		if _, ok := s.groups[id].Store.(PreparedClaimMutationApplier); !ok {
			return errors.WithStack(ErrTSOProtocolUnsupported)
		}
	}
	return nil
}

func (s *ShardStore) CheckTimestampClaimMember(ctx context.Context, groupID uint64, address string) error {
	group := s.groups[groupID]
	if group == nil || address == "" {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	connection, err := s.connCache.ConnFor(address)
	if err != nil {
		return errors.Wrap(err, "connect claim decoder member")
	}
	ctx, cancel := context.WithTimeout(ctx, verifyLeaderTimeout)
	defer cancel()
	response, err := pb.NewInternalClient(connection).GetPrepareEvidenceState(prepareEvidencePeerContext(ctx, group), &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
	if err != nil {
		return errors.Wrap(err, "probe claim decoder member")
	}
	if err := s.validatePrepareEvidenceMemberResponse(groupID, address, response, true); err != nil {
		return err
	}
	if response.TimestampClaimProtocolVersion != TSOClaimProtocolVersion {
		return errors.Wrapf(ErrTSOProtocolUnsupported, "group %d member %s does not decode concrete timestamp claims", groupID, address)
	}
	if groupID != 0 && response.TimestampClaimPrepareApplyVersion != TSOClaimProtocolVersion {
		return errors.Wrap(ErrTSOProtocolUnsupported, "member lacks atomic prepared-claim apply")
	}
	return nil
}
