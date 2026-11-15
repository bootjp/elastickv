package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// TimestampBindingProtocolVersion attests binary support, not activation.
const TimestampBindingProtocolVersion uint32 = 1

var ErrTimestampBindingCapability = errors.New("timestamp binding protocol is not proven for every apply member")

// CheckTimestampBindingGroupCapability is a rolling-upgrade preflight for one
// locally led group. It does not persist a certificate or enable admission.
func (s *ShardStore) CheckTimestampBindingGroupCapability(ctx context.Context, groupID uint64) error {
	group, engine, err := timestampBindingGroupEngine(s, groupID)
	if err != nil {
		return err
	}
	before := engine.Status()
	if !timestampBindingStableLeader(before) {
		return errors.WithStack(ErrTimestampBindingCapability)
	}
	configuration, err := engine.Configuration(ctx)
	if err != nil || len(configuration.Servers) == 0 {
		return errors.Wrap(ErrTimestampBindingCapability, "membership unavailable")
	}
	if err := s.probeTimestampBindingMembers(ctx, groupID, group, before.Leader.ID, configuration.Servers); err != nil {
		return err
	}
	after := engine.Status()
	if !timestampBindingStableLeader(after) || after.Term != before.Term || after.Leader != before.Leader ||
		after.ConfigurationIndex != before.ConfigurationIndex {
		return errors.WithStack(ErrTimestampBindingCapability)
	}
	return nil
}

func timestampBindingGroupEngine(s *ShardStore, groupID uint64) (*ShardGroup, raftengine.Engine, error) {
	if s == nil {
		return nil, nil, errors.WithStack(ErrTimestampBindingCapability)
	}
	group := s.groups[groupID]
	engine := engineForGroup(group)
	if engine == nil {
		return nil, nil, errors.WithStack(ErrTimestampBindingCapability)
	}
	return group, engine, nil
}

func timestampBindingStableLeader(status raftengine.Status) bool {
	return status.State == raftengine.StateLeader && status.Term != 0 && status.Leader.ID != "" &&
		status.ConfigurationIndex != 0 && !status.PendingConfChange && status.LeadTransferee == 0
}

func (s *ShardStore) probeTimestampBindingMembers(ctx context.Context, groupID uint64, group *ShardGroup,
	leaderID string, members []raftengine.Server) error {
	seen := make(map[string]struct{}, len(members))
	local := false
	for _, member := range members {
		if member.ID == "" || member.Address == "" || (member.Suffrage != "voter" && member.Suffrage != "learner") {
			return errors.WithStack(ErrTimestampBindingCapability)
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return errors.WithStack(ErrTimestampBindingCapability)
		}
		seen[member.ID] = struct{}{}
		if member.ID == leaderID {
			local = true
			continue
		}
		if err := s.probeTimestampBindingMember(ctx, groupID, group, member); err != nil {
			return err
		}
	}
	if !local {
		return errors.WithStack(ErrTimestampBindingCapability)
	}
	return nil
}

func (s *ShardStore) probeTimestampBindingMember(ctx context.Context, groupID uint64, group *ShardGroup,
	member raftengine.Server) error {
	probeCtx, cancel := context.WithTimeout(prepareEvidencePeerContext(ctx, group), verifyLeaderTimeout)
	defer cancel()
	reply, err := s.remoteStartupPrepareState(probeCtx, groupID, member.Address,
		&pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
	if err != nil || !reply.GetSupported() || reply.GetTimestampBindingProtocolVersion() < TimestampBindingProtocolVersion {
		return errors.Wrapf(ErrTimestampBindingCapability, "group %d member %s lacks protocol: %v", groupID, member.ID, err)
	}
	return nil
}
