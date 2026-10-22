package kv

import (
	"context"
	"sort"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
)

// CheckPhaseDProtocol negotiates with every voter and learner, including group
// zero, before any entry that an older FSM cannot decode is proposed.
func (s *ShardStore) CheckPhaseDProtocol(ctx context.Context) error {
	if err := s.requireLocalPrepareEvidenceCutover(); err != nil {
		return err
	}
	ids := make([]uint64, 0, len(s.groups))
	for id := range s.groups {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := s.checkPrepareEvidenceGroupProtocol(ctx, id); err != nil {
			return err
		}
	}
	if err := s.fencePrepareEvidenceGroups(ctx, 0); err != nil {
		return err
	}
	return s.inspectPrepareEvidenceGroups(ctx, 0)
}

func (s *ShardStore) requireLocalPrepareEvidenceCutover() error {
	if s == nil || s.groups[0] == nil {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	if state := s.groups[0].TSOState; state == nil || !state.CutoverActive() {
		return errors.Wrap(ErrTSOPhaseDActivationPending, "local dedicated timestamp cutover has not applied")
	}
	return nil
}

func (s *ShardStore) checkPrepareEvidenceGroupProtocol(ctx context.Context, id uint64) error {
	group := s.groups[id]
	engine := engineForGroup(group)
	if engine == nil {
		return errors.Wrapf(ErrTSOProtocolUnsupported, "group %d has no engine", id)
	}
	before := engine.Status()
	if before.PendingConfChange {
		return errors.WithStack(ErrTSOPhaseDActivationPending)
	}
	configuration, err := engine.Configuration(ctx)
	if err != nil || len(configuration.Servers) == 0 {
		return errors.Wrapf(ErrTSOProtocolUnsupported, "group %d membership unavailable: %v", id, err)
	}
	eg, probeCtx := errgroup.WithContext(ctx)
	eg.SetLimit(maxTSOCommitFloorConcurrency)
	for _, member := range configuration.Servers {
		if isLeaderEngine(engine) && member.ID == before.Leader.ID {
			continue
		}
		eg.Go(func() error {
			return s.checkPrepareEvidenceMember(probeCtx, id, member.Address, true)
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.WithStack(err)
	}
	return prepareEvidenceMembershipStable(before, engine.Status())
}

func prepareEvidenceMembershipStable(before, after raftengine.Status) error {
	if after.ConfigurationIndex != before.ConfigurationIndex || after.PendingConfChange {
		return errors.WithStack(ErrTSOPhaseDActivationPending)
	}
	return nil
}

// CheckPrepareEvidenceMember is also the membership admission hook. Requiring
// support on every newly admitted member closes the negotiation/add race.
func (s *ShardStore) CheckPrepareEvidenceMember(ctx context.Context, groupID uint64, address string) error {
	if s == nil || s.groups[0] == nil || s.groups[0].TSOState == nil || !s.groups[0].TSOState.CutoverActive() {
		return nil
	}
	return s.checkPrepareEvidenceMember(ctx, groupID, address, false)
}

func (s *ShardStore) checkPrepareEvidenceMember(ctx context.Context, groupID uint64, address string, requireCutover bool) error {
	group := s.groups[groupID]
	if group == nil || address == "" {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	conn, err := s.connCache.ConnFor(address)
	if err != nil {
		return errors.Wrap(err, "prepare evidence capability connection")
	}
	probeCtx, cancel := context.WithTimeout(ctx, verifyLeaderTimeout)
	defer cancel()
	probeCtx = prepareEvidencePeerContext(probeCtx, group)
	response, err := pb.NewInternalClient(conn).GetPrepareEvidenceState(probeCtx, &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
	if err != nil {
		return errors.Wrapf(ErrTSOProtocolUnsupported, "group %d member %s lacks prepare evidence protocol: %v", groupID, address, err)
	}
	return s.validatePrepareEvidenceMemberResponse(groupID, address, response, requireCutover)
}

func (s *ShardStore) validatePrepareEvidenceMemberResponse(groupID uint64, address string, response *pb.PrepareEvidenceStateResponse, requireCutover bool) error {
	if !response.GetSupported() {
		return errors.Wrapf(ErrTSOProtocolUnsupported, "group %d member %s lacks prepare evidence protocol", groupID, address)
	}
	if timestampGroup := s.groups[0]; timestampGroup != nil && timestampGroup.TSOState != nil && timestampGroup.TSOState.CutoverActive() && !response.GetDedicatedTsoConfigured() {
		return errors.Wrapf(ErrTSOProtocolUnsupported, "group %d member %s must configure the dedicated timestamp runtime before joining a cutover cluster", groupID, address)
	}
	if requireCutover && !response.GetCutoverApplied() {
		return errors.Wrapf(ErrTSOPhaseDActivationPending, "group %d member %s has not applied dedicated timestamp cutover", groupID, address)
	}
	return nil
}

// PreparePhaseD keeps COMMIT/ABORT available while refusing activation until
// all old unproven locks, including secondaries and staged locks, have drained.
func (s *ShardStore) PreparePhaseD(ctx context.Context, floor uint64) error {
	if floor == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}
	if err := s.CheckPhaseDProtocol(ctx); err != nil {
		return err
	}
	if err := s.fencePrepareEvidenceGroups(ctx, floor); err != nil {
		return err
	}
	return s.inspectPrepareEvidenceGroups(ctx, floor)
}

func (s *ShardStore) inspectPrepareEvidenceGroups(ctx context.Context, floor uint64) error {
	ids, err := s.tsoCommitFloorGroupIDs()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.inspectPrepareEvidenceGroup(ctx, id, floor); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardStore) fencePrepareEvidenceGroups(ctx context.Context, floor uint64) error {
	return s.proposePrepareEvidenceGroups(ctx, EncodePrepareEvidenceFence(floor))
}

// SealPhaseDCreation closes the late-apply race before sampling the final
// committed timestamp floor. Existing COMMIT and ABORT requests remain usable.
func (s *ShardStore) SealPhaseDCreation(ctx context.Context) error {
	return s.proposePrepareEvidenceGroups(ctx, EncodePrepareEvidenceSeal())
}

// FinishPhaseD must run after the durable group-zero marker, including after
// leadership changes and retries of a partially completed release fanout.
func (s *ShardStore) FinishPhaseD(ctx context.Context, floor uint64) error {
	return s.proposePrepareEvidenceGroups(ctx, EncodePrepareEvidenceRelease(floor))
}

func (s *ShardStore) proposePrepareEvidenceGroups(ctx context.Context, command []byte) error {
	ids, err := s.tsoCommitFloorGroupIDs()
	if err != nil {
		return err
	}
	for _, id := range ids {
		group := s.groups[id]
		proposer := NewLeaderAdminProposer(group.Engine, group.Proposer(), &s.connCache, WithLeaderAdminToken(group.forwardedLeaderReadToken()))
		result, err := proposer.ProposeAdmin(ctx, command)
		if err != nil {
			return errors.Wrapf(err, "prepare evidence control group %d", id)
		}
		if result == nil {
			return errors.Wrapf(ErrTSOProtocolUnsupported, "group %d returned no prepare control receipt", id)
		}
		if applyErr, ok := result.Response.(error); ok && applyErr != nil {
			return errors.Wrapf(applyErr, "prepare evidence control group %d apply", id)
		}
	}
	return nil
}

func (s *ShardStore) inspectPrepareEvidenceGroup(ctx context.Context, id, floor uint64) error {
	group := s.groups[id]
	engine := engineForGroup(group)
	if isLeaderEngine(engine) {
		if _, err := linearizableReadEngineCtx(ctx, engine); err != nil {
			return err
		}
		enabled, unproven, actualFloor, err := InspectPrepareEvidence(ctx, group.Store, floor)
		return prepareEvidenceDrainResult(id, enabled, unproven, actualFloor, floor, err)
	}
	address := leaderAddrFromEngine(engine)
	if address == "" {
		return errors.WithStack(ErrLeaderNotFound)
	}
	conn, err := s.connCache.ConnFor(address)
	if err != nil {
		return err
	}
	response, err := pb.NewInternalClient(conn).GetPrepareEvidenceState(prepareEvidencePeerContext(ctx, group), &pb.PrepareEvidenceStateRequest{Floor: floor})
	return prepareEvidenceDrainResult(id, response.GetProofEnabled(), response.GetUnprovenLocks(), response.GetFenceFloor(), floor, err)
}

func prepareEvidenceDrainResult(id uint64, enabled, unproven bool, actual, expected uint64, err error) error {
	if err != nil {
		return errors.Wrapf(err, "inspect prepare evidence group %d", id)
	}
	if !enabled || unproven || expected != 0 && actual != expected {
		return errors.Wrapf(ErrTSOPhaseDActivationPending, "group %d is waiting for its prepare evidence fence and legacy lock drain", id)
	}
	return nil
}

func prepareEvidencePeerContext(ctx context.Context, group *ShardGroup) context.Context {
	if token := group.forwardedLeaderReadToken(); token != "" {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	return ctx
}

var _ TSOPhaseDActivationGate = (*ShardStore)(nil)
