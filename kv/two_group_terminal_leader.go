package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// VerifyTwoGroupPrepareOnLeader returns only after a term-fenced ReadIndex and
// an exact local PREPARE lookup. It is used as the primary COMMIT precondition.
func VerifyTwoGroupPrepareOnLeader(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest,
	leader raftengine.LeaderView, metadata store.RaftControlMetadataStore,
	checkSession func(context.Context) error) (uint64, uint64, error) {
	if !validTwoGroupVerifyInputs(group, manifest, leader, metadata, checkSession) {
		return 0, 0, errors.WithStack(ErrTwoGroupAdmission)
	}
	reader, term, err := twoGroupVerifyLeader(ctx, leader)
	if err != nil {
		return 0, 0, err
	}
	index, err := leader.LinearizableRead(ctx)
	if err != nil || index == 0 {
		return 0, 0, errors.Join(ErrTwoGroupAdmission, err)
	}
	if err := lookupTwoGroupLocalPrepare(ctx, metadata, manifest, group); err != nil {
		return 0, 0, err
	}
	if err := checkSession(ctx); err != nil {
		return 0, 0, err
	}
	if current := reader.Status(); current.State != raftengine.StateLeader || current.Term != term {
		return 0, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	return index, term, nil
}

func validTwoGroupVerifyInputs(group uint64, manifest *pb.TwoGroupPrepareManifest,
	leader raftengine.LeaderView, metadata store.RaftControlMetadataStore, checkSession func(context.Context) error) bool {
	if _, _, _, err := canonicalTwoGroupManifest(manifest); err != nil {
		return false
	}
	return group != 0 && leader != nil && metadata != nil && checkSession != nil && twoGroupManifestHasGroupID(manifest, group)
}

func twoGroupVerifyLeader(ctx context.Context, leader raftengine.LeaderView) (raftengine.StatusReader, uint64, error) {
	reader, ok := leader.(raftengine.StatusReader)
	if !ok || leader.State() != raftengine.StateLeader {
		return nil, 0, errors.WithStack(raftengine.ErrNotLeader)
	}
	term := reader.Status().Term
	if term == 0 {
		return nil, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	if err := leader.VerifyLeader(ctx); err != nil {
		return nil, 0, errors.Wrap(err, "verify two-group leader")
	}
	return reader, term, nil
}

func twoGroupManifestHasGroupID(manifest *pb.TwoGroupPrepareManifest, group uint64) bool {
	for _, participant := range manifest.GetParticipants() {
		if participant.GetGroupId() == group {
			return true
		}
	}
	return false
}

// ProposeTwoGroupTerminalOnLeader never accepts a caller-supplied observation.
// The primary independently verifies secondary PREPARE before COMMIT, while the
// secondary independently observes the applied primary decision.
func ProposeTwoGroupTerminalOnLeader(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest,
	decision pb.Phase, leader raftengine.LeaderView, proposer raftengine.TermFencedProposer,
	metadata store.RaftControlMetadataStore, home TwoGroupDecisionLookup,
	verifySecondary func(context.Context, uint64, *pb.TwoGroupPrepareManifest) error,
	checkSession func(context.Context) error) (uint64, uint64, error) {
	if !validTwoGroupTerminalLeaderInputs(group, manifest, decision, metadata) {
		return 0, 0, errors.WithStack(ErrTwoGroupAdmission)
	}
	reader, term, err := twoGroupLeaderTerm(ctx, group, manifest, leader, proposer, checkSession)
	if err != nil {
		return 0, 0, err
	}
	if _, _, err := VerifyTwoGroupPrepareOnLeader(ctx, group, manifest, leader, metadata, checkSession); err != nil {
		return 0, 0, err
	}
	observed, err := twoGroupLeaderDecisionObservation(ctx, group, manifest, decision, home, verifySecondary)
	if err != nil {
		return 0, 0, err
	}
	if err := checkSession(ctx); err != nil {
		return 0, 0, err
	}
	if current := reader.Status(); current.State != raftengine.StateLeader || current.Term != term {
		return 0, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	data, err := marshalTwoGroupTerminalEntry(group, manifest, decision, observed)
	if err != nil {
		return 0, 0, err
	}
	return proposeTwoGroupTerminalData(ctx, proposer, term, data)
}

func validTwoGroupTerminalLeaderInputs(group uint64, manifest *pb.TwoGroupPrepareManifest,
	decision pb.Phase, metadata store.RaftControlMetadataStore) bool {
	return validTwoGroupDecision(decision) && metadata != nil && twoGroupManifestHasGroupID(manifest, group)
}

func twoGroupLeaderDecisionObservation(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest,
	decision pb.Phase, home TwoGroupDecisionLookup,
	verifySecondary func(context.Context, uint64, *pb.TwoGroupPrepareManifest) error) (TwoGroupDecisionObservation, error) {
	if group != manifest.GetPrimaryGroupId() {
		observed, err := ObserveTwoGroupDecision(ctx, home, manifest, decision)
		if err != nil {
			return TwoGroupDecisionObservation{}, errors.Join(ErrTransactionOutcomeUnknown, err)
		}
		return observed, nil
	}
	if decision != pb.Phase_COMMIT {
		return TwoGroupDecisionObservation{}, nil
	}
	secondary := manifest.GetParticipants()[0].GetGroupId()
	if secondary == group {
		secondary = manifest.GetParticipants()[1].GetGroupId()
	}
	if verifySecondary == nil {
		return TwoGroupDecisionObservation{}, errors.WithStack(ErrTransactionOutcomeUnknown)
	}
	if err := verifySecondary(ctx, secondary, manifest); err != nil {
		return TwoGroupDecisionObservation{}, errors.Join(ErrTransactionOutcomeUnknown, err)
	}
	return TwoGroupDecisionObservation{}, nil
}

func proposeTwoGroupTerminalData(ctx context.Context, proposer raftengine.TermFencedProposer, term uint64, data []byte) (uint64, uint64, error) {
	result, err := proposer.ProposeIfTerm(ctx, term, data)
	if err != nil || result == nil || result.CommitIndex == 0 {
		return 0, 0, errors.Join(ErrTransactionOutcomeUnknown, err)
	}
	if result.Response != nil {
		if applyErr, ok := result.Response.(error); ok {
			return 0, 0, applyErr
		}
		return 0, 0, errors.WithStack(ErrRaftEntryApply)
	}
	return result.CommitIndex, term, nil
}
